//! Integration tests for the Twirp cache service v2 routes.
//!
//! Drives the full app via `tower::ServiceExt::oneshot` and goes
//! through the auth middleware (with `skip_token_validation=true`) so
//! the route + auth wiring is exercised end-to-end. Tokens are
//! RS256-signed with a process-wide RSA key — signatures are
//! meaningless under skip-validation but the JWT still has to parse,
//! and the middleware still pulls `ac` / `repository_id` out.
//!
//! Shared harness is in `tests/twirp_common/mod.rs`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod twirp_common;

use axum::http::StatusCode;
use gha_cache_oxide::db::entities::{CacheEntryCoord, NewUpload};
use gha_cache_oxide::db::id::new_upload_id;
use gha_cache_oxide::db::queries::{insert_storage_location_tx, upsert_cache_entry_tx};
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tower::ServiceExt;

use twirp_common::{
    BASE_PATH, Harness, body_json, count, fetch_string, harness, mint_token, post, read_only_token,
    write_token,
};

// --- 401 / route-not-found ----------------------------------------------

#[tokio::test]
async fn unauthenticated_request_is_401() {
    let h = harness().await;
    let req = post(
        &format!("{BASE_PATH}/CreateCacheEntry"),
        None,
        &json!({"key":"k","version":"v"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["statusCode"], 401);
    assert!(body.get("message").is_some());
}

#[tokio::test]
async fn unknown_method_under_cache_service_is_404() {
    let h = harness().await;
    let token = write_token();
    let req = post(
        &format!("{BASE_PATH}/CreateCache"), // typo'd method
        Some(&token),
        &json!({"key":"k","version":"v"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// --- CreateCacheEntry ---------------------------------------------------

#[tokio::test]
async fn create_cache_entry_happy_path_returns_signed_upload_url() {
    let h = harness().await;
    let token = write_token();
    let req = post(
        &format!("{BASE_PATH}/CreateCacheEntry"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    let url = body["signed_upload_url"].as_str().expect("string");
    let id_part = url
        .strip_prefix("http://localhost:3000/devstoreaccount1/upload/")
        .expect("expected upstream-shaped URL");
    let id: i64 = id_part.parse().unwrap();
    assert!(id >= 0);
}

#[tokio::test]
async fn create_cache_entry_with_inflight_upload_returns_ok_false() {
    let h = harness().await;
    let token = write_token();

    // Pre-seed an upload at the same coord.
    let coord = CacheEntryCoord {
        key: "build-cache",
        version: "v1",
        scope: "refs/heads/main",
        repo_id: "42",
    };
    let id = new_upload_id();
    h.db.create_upload(NewUpload {
        id,
        coord,
        folder_name: &id.to_string(),
        created_at_ms: 0,
    })
    .await
    .unwrap();

    let req = post(
        &format!("{BASE_PATH}/CreateCacheEntry"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"ok": false}));

    // Second upload row must NOT have been inserted.
    assert_eq!(count(&h.db, "SELECT COUNT(*) FROM uploads").await, 1);
}

#[tokio::test]
async fn create_cache_entry_with_read_only_scopes_is_403() {
    let h = harness().await;
    let token = read_only_token();
    let req = post(
        &format!("{BASE_PATH}/CreateCacheEntry"),
        Some(&token),
        &json!({"key":"k","version":"v"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["statusCode"], 403);
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("write permission")
    );
}

#[tokio::test]
async fn create_cache_entry_with_bad_body_is_400() {
    let h = harness().await;
    let token = write_token();
    let req = post(
        &format!("{BASE_PATH}/CreateCacheEntry"),
        Some(&token),
        &json!({}), // missing key + version
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["statusCode"], 400);
}

// --- FinalizeCacheEntryUpload ------------------------------------------

/// Seeds an upload + drops `parts_on_disk` empty files at
/// `<folder>/parts/{i}` on the harness's tmp dir. Returns the id.
async fn seed_finalize_upload(
    h: &Harness,
    started: i64,
    finished: i64,
    parts_on_disk: usize,
) -> i64 {
    let id = new_upload_id();
    let folder = id.to_string();
    let coord = CacheEntryCoord {
        key: "build-cache",
        version: "v1",
        scope: "refs/heads/main",
        repo_id: "42",
    };
    h.db.create_upload(NewUpload {
        id,
        coord,
        folder_name: &folder,
        created_at_ms: 0,
    })
    .await
    .unwrap();
    for _ in 0..started {
        h.db.increment_upload_started(id).await.unwrap();
    }
    for _ in 0..finished {
        h.db.increment_upload_finished(id, 0).await.unwrap();
    }
    let parts_dir = h.tmp.path().join(&folder).join("parts");
    tokio::fs::create_dir_all(&parts_dir).await.unwrap();
    for i in 0..parts_on_disk {
        let mut f = tokio::fs::File::create(parts_dir.join(i.to_string()))
            .await
            .unwrap();
        f.write_all(b"x").await.unwrap();
    }
    id
}

#[tokio::test]
async fn finalize_happy_path_returns_entry_id_and_persists_cache_entry() {
    let h = harness().await;
    let token = write_token();
    let id = seed_finalize_upload(&h, 1, 1, 1).await;

    let req = post(
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["entry_id"], json!(id.to_string()));

    assert_eq!(count(&h.db, "SELECT COUNT(*) FROM uploads").await, 0);
    assert_eq!(count(&h.db, "SELECT COUNT(*) FROM cache_entries").await, 1);
    assert_eq!(
        count(&h.db, "SELECT COUNT(*) FROM storage_locations").await,
        1
    );
}

#[tokio::test]
async fn finalize_started_finished_mismatch_is_400_and_deletes_upload() {
    let h = harness().await;
    let token = write_token();
    let _ = seed_finalize_upload(&h, 2, 1, 1).await;

    let req = post(
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["statusCode"], 400);

    assert_eq!(
        count(&h.db, "SELECT COUNT(*) FROM uploads").await,
        0,
        "uploads row must be deleted on mismatch"
    );
}

#[tokio::test]
async fn finalize_disk_count_mismatch_is_400_and_deletes_upload() {
    let h = harness().await;
    let token = write_token();
    // DB says 2 parts done, only 1 file on disk.
    let _ = seed_finalize_upload(&h, 2, 2, 1).await;

    let req = post(
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["statusCode"], 400);

    assert_eq!(count(&h.db, "SELECT COUNT(*) FROM uploads").await, 0);
}

#[tokio::test]
async fn finalize_without_seeded_upload_is_404() {
    let h = harness().await;
    let token = write_token();
    let req = post(
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        Some(&token),
        &json!({"key":"k","version":"v"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["statusCode"], 404);
}

#[tokio::test]
async fn finalize_read_only_scopes_is_403() {
    let h = harness().await;
    let token = read_only_token();
    let req = post(
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        Some(&token),
        &json!({"key":"k","version":"v"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// --- GetCacheEntryDownloadURL ------------------------------------------

async fn seed_cache_entry(h: &Harness, key: &str, scope: &str, updated_at: i64) -> String {
    use gha_cache_oxide::db::id::new_uuid;
    let location_id = new_uuid();
    let mut tx = h.db.begin().await.unwrap();
    insert_storage_location_tx(&mut tx, &location_id, &format!("folder-{location_id}"), 1)
        .await
        .unwrap();
    let coord = CacheEntryCoord {
        key,
        version: "v1",
        scope,
        repo_id: "42",
    };
    let _ = upsert_cache_entry_tx(&mut tx, coord, &location_id, updated_at)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    fetch_string(
        &h.db,
        &format!(
            "SELECT id FROM cache_entries WHERE key = '{key}' AND scope = '{scope}' \
             AND version = 'v1' AND repoId = '42'"
        ),
    )
    .await
}

#[tokio::test]
async fn get_download_url_hit_returns_url_and_matched_key() {
    let h = harness().await;
    let token = write_token();
    let entry_id = seed_cache_entry(&h, "build-cache", "refs/heads/main", 1_000).await;

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["matched_key"], json!("build-cache"));
    assert_eq!(
        body["signed_download_url"],
        json!(format!("http://localhost:3000/download/{entry_id}"))
    );
}

#[tokio::test]
async fn get_download_url_miss_returns_ok_false() {
    let h = harness().await;
    let token = write_token();
    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"absent","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"ok": false}));
}

#[tokio::test]
async fn get_download_url_prefers_higher_permission_scope() {
    let h = harness().await;
    // Two scopes seeded with the SAME primary key so the higher-permission
    // scope wins after the per-permission-DESC sort that this handler
    // applies (mirrors upstream sortBy([prop('Permission'),'desc'])).
    let high_id = seed_cache_entry(&h, "shared", "scope-write", 1_000).await;
    let _low_id = seed_cache_entry(&h, "shared", "scope-read", 9_999).await;

    // Note ordering in the token is read-then-write so we PROVE the
    // sort runs (raw order would pick scope-read first).
    let token = mint_token(
        &json!([
            {"Scope": "scope-read",  "Permission": 0},
            {"Scope": "scope-write", "Permission": 3},
        ]),
        "42",
    );
    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"shared","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["signed_download_url"]
            .as_str()
            .unwrap()
            .ends_with(&high_id),
        "expected URL pointing at high-permission scope's entry id"
    );
}

#[tokio::test]
async fn get_download_url_bad_body_is_400() {
    let h = harness().await;
    let token = write_token();
    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"k"}), // missing version
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["statusCode"], 400);
}

#[tokio::test]
async fn get_download_url_with_restore_keys_walks_them() {
    let h = harness().await;
    let token = write_token();
    // Primary won't match; the restore key prefix should hit.
    let _ = seed_cache_entry(&h, "deps-lockfile-abc", "refs/heads/main", 1_000).await;

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({
            "key":"deps-primary-miss",
            "version":"v1",
            "restore_keys": ["deps-lockfile-"],
        }),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["matched_key"], json!("deps-lockfile-abc"));
}
