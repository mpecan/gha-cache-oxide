//! Integration tests for `GetCacheEntryDownloadURL`. Split out of
//! `tests/twirp.rs` to keep both files comfortably under the 500-line
//! soft limit. Shares the harness in `tests/twirp_common/mod.rs`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod twirp_common;

use std::sync::Arc;

use axum::http::StatusCode;
use gha_cache_oxide::db::entities::CacheEntryCoord;
use gha_cache_oxide::storage::StorageAdapter;
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;

use twirp_common::{
    BASE_PATH, Harness, HarnessOpts, SIGNED_URL, SigningFilesystem, body_json, fetch_string,
    harness, harness_with, mint_token, post, write_token,
};

async fn seed_cache_entry(h: &Harness, key: &str, scope: &str, updated_at: i64) -> String {
    seed_cache_entry_with_location(h, key, scope, updated_at)
        .await
        .0
}

/// Seeds a cache entry + storage location and returns `(entry_id, location_id)`
/// so tests can mark the location as merged after the fact.
async fn seed_cache_entry_with_location(
    h: &Harness,
    key: &str,
    scope: &str,
    updated_at: i64,
) -> (String, String) {
    use gha_cache_oxide::db::id::new_uuid;
    let location_id = new_uuid();
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location(&location_id, &format!("folder-{location_id}"), 1)
        .await
        .unwrap();
    let coord = CacheEntryCoord {
        key,
        version: "v1",
        scope,
        repo_id: "42",
    };
    let _ = tx
        .upsert_cache_entry(coord, &location_id, updated_at)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let entry_id = fetch_string(
        &*h.db,
        &format!(
            "SELECT id FROM cache_entries WHERE key = '{key}' AND scope = '{scope}' \
             AND version = 'v1' AND repoId = '42'"
        ),
    )
    .await;
    (entry_id, location_id)
}

#[tokio::test]
async fn hit_returns_url_and_matched_key() {
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
async fn miss_returns_ok_false() {
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
async fn prefers_higher_permission_scope() {
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
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["matched_key"], json!("shared"));
    assert!(
        body["signed_download_url"]
            .as_str()
            .unwrap()
            .ends_with(&high_id),
        "expected URL pointing at high-permission scope's entry id"
    );
}

#[tokio::test]
async fn unauthenticated_is_401() {
    // Symmetry gap identified in review: the other two routes have
    // direct 401 coverage; this one relied on it transitively via the
    // shared middleware. Pin it so a future change to the sub-router
    // layer wiring would surface here.
    let h = harness().await;
    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        None,
        &json!({"key":"k","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["statusCode"], 401);
}

#[tokio::test]
async fn version_mismatch_returns_ok_false() {
    // DB-level version isolation is pinned by match_cache_entry_tests;
    // this pins that the handler faithfully forwards the body's
    // `version` field and treats a mismatch as a cache miss rather
    // than a 4xx.
    let h = harness().await;
    let token = write_token();
    let _ = seed_cache_entry(&h, "k", "refs/heads/main", 1_000).await;

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"k","version":"wrong-version"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"ok": false}));
}

#[tokio::test]
async fn bad_body_is_400_with_status_message() {
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
    assert!(
        body["statusMessage"].as_str().is_some(),
        "body-parse 400s should carry statusMessage per upstream parity"
    );
}

#[tokio::test]
async fn with_restore_keys_walks_them() {
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

// --- ENABLE_DIRECT_DOWNLOADS coverage (issue #16) -----------------------
//
// Four branches mirror upstream `lib/storage.ts:511-519` and the issue's
// acceptance criteria:
//   1. flag off                           → server URL
//   2. flag on  + non-signing driver      → server URL
//   3. flag on  + signer + merged         → presigned URL
//   4. flag on  + signer + NOT merged     → server URL (merge-and-serve)

async fn signing_harness(enable_flag: bool) -> Harness {
    // Store backing the SigningFilesystem in its own tmpdir so the shim
    // and the harness root stay distinct — the harness's `tmp` is only
    // used as an owned `TempDir` guard in the default path.
    let root = TempDir::new().unwrap();
    let storage: Arc<dyn StorageAdapter> = Arc::new(SigningFilesystem::new(root.path()));
    // Leak the root so it outlives the Harness — otherwise the tmpdir
    // races against any late download attempt. Tests don't actually
    // download here (they assert on the URL), but keep the fd alive
    // just in case a handler touches storage.
    let _ = Box::leak(Box::new(root));
    harness_with(HarnessOpts {
        enable_direct_downloads: enable_flag,
        storage: Some(storage),
    })
    .await
}

#[tokio::test]
async fn direct_downloads_flag_off_always_returns_server_url() {
    // Even when the adapter can sign URLs, the flag being off means we
    // stay on the server-proxied path. Mirrors upstream `env.ENABLE_DIRECT_DOWNLOADS`
    // short-circuit in `lib/storage.ts:511`.
    let h = signing_harness(false).await;
    let token = write_token();
    let (entry_id, location_id) =
        seed_cache_entry_with_location(&h, "k", "refs/heads/main", 1_000).await;
    // Mark the location merged so we also cover the "would-have-signed"
    // condition: flag being off is the only reason we stay on the
    // server URL.
    h.db.mark_merged(&location_id, 2_000).await.unwrap();

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"k","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(
        body["signed_download_url"],
        json!(format!("http://localhost:3000/download/{entry_id}"))
    );
}

#[tokio::test]
async fn direct_downloads_non_signing_driver_returns_server_url() {
    // Plain FilesystemAdapter returns Ok(None) from signed_url, so even
    // with the flag on the merged-path URL must stay server-relative.
    let h = harness_with(HarnessOpts {
        enable_direct_downloads: true,
        storage: None, // default FilesystemAdapter
    })
    .await;
    let token = write_token();
    let (entry_id, location_id) =
        seed_cache_entry_with_location(&h, "k", "refs/heads/main", 1_000).await;
    h.db.mark_merged(&location_id, 2_000).await.unwrap();

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"k","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["signed_download_url"],
        json!(format!("http://localhost:3000/download/{entry_id}"))
    );
}

#[tokio::test]
async fn direct_downloads_signer_and_merged_returns_presigned_url() {
    let h = signing_harness(true).await;
    let token = write_token();
    let (_entry_id, location_id) =
        seed_cache_entry_with_location(&h, "k", "refs/heads/main", 1_000).await;
    h.db.mark_merged(&location_id, 2_000).await.unwrap();

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"k","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["signed_download_url"], json!(SIGNED_URL));
}

#[tokio::test]
async fn direct_downloads_signer_but_not_merged_returns_server_url() {
    // First-download path: merged_at is still NULL, so even with
    // the flag and a signer the server must serve via /download/{id}
    // to trigger lazy-merge. Matches upstream `lib/storage.ts:517-519`
    // where the unmerged branch returns `defaultUrl`.
    let h = signing_harness(true).await;
    let token = write_token();
    let (entry_id, _location_id) =
        seed_cache_entry_with_location(&h, "k", "refs/heads/main", 1_000).await;

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({"key":"k","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["signed_download_url"],
        json!(format!("http://localhost:3000/download/{entry_id}"))
    );
}
