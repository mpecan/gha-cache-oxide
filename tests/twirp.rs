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
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tower::ServiceExt;

use twirp_common::{
    BASE_PATH, Harness, body_json, count, harness, post, read_only_token, write_token,
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
async fn unknown_method_under_cache_service_falls_through_to_proxy() {
    // After issue #24, unhandled paths fall through to the
    // `DEFAULT_ACTIONS_RESULTS_URL` catch-all proxy (matching upstream
    // `routes/[...path].ts`). The harness configures
    // `default_actions_results_url = https://results-receiver.test/`,
    // a non-existent host, so the proxy reports 502 rather than the
    // pre-#24 404. The assertion still proves axum routing did NOT
    // accidentally match `/CreateCache` against `/CreateCacheEntry`.
    let h = harness().await;
    let token = write_token();
    let req = post(
        &format!("{BASE_PATH}/CreateCache"), // typo'd method
        Some(&token),
        &json!({"key":"k","version":"v"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
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
    assert_eq!(count(&*h.db, "SELECT COUNT(*) FROM uploads").await, 1);
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
async fn create_cache_entry_with_bad_body_is_400_with_status_message() {
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
    // Body-parse errors use `statusMessage` per upstream (see route
    // docstring). Confirm the key is populated and non-empty.
    let msg = body["statusMessage"]
        .as_str()
        .expect("statusMessage should be set for body-parse errors");
    assert!(msg.starts_with("Invalid body:"), "got: {msg}");
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

    assert_eq!(count(&*h.db, "SELECT COUNT(*) FROM uploads").await, 0);
    assert_eq!(count(&*h.db, "SELECT COUNT(*) FROM cache_entries").await, 1);
    assert_eq!(
        count(&*h.db, "SELECT COUNT(*) FROM storage_locations").await,
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
    // Validation-class finalize errors use `message` (not
    // `statusMessage`) per the route docstring's split.
    let msg = body["message"]
        .as_str()
        .expect("message should be set for validation-class errors");
    assert!(
        msg.contains("only") && msg.contains("of") && msg.contains("parts"),
        "expected PartsCountMismatch wording, got: {msg}"
    );

    assert_eq!(
        count(&*h.db, "SELECT COUNT(*) FROM uploads").await,
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
    let msg = body["message"].as_str().expect("message should be set");
    assert!(
        msg.contains("disk count") || msg.contains("does not match"),
        "expected DiskCountMismatch wording, got: {msg}"
    );

    assert_eq!(count(&*h.db, "SELECT COUNT(*) FROM uploads").await, 0);
}

#[tokio::test]
async fn finalize_no_parts_uploaded_is_400_and_deletes_upload() {
    // Closes the HTTP-level gap identified in review: cache_tests.rs
    // already covers the CompleteUploadError::NoPartsUploaded branch at
    // the service layer; this pins its HTTP mapping.
    let h = harness().await;
    let token = write_token();
    let _ = seed_finalize_upload(&h, 0, 0, 0).await;

    let req = post(
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["statusCode"], 400);
    let msg = body["message"].as_str().expect("message should be set");
    assert!(
        msg.contains("no parts"),
        "expected NoPartsUploaded wording, got: {msg}"
    );
    assert_eq!(count(&*h.db, "SELECT COUNT(*) FROM uploads").await, 0);
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
    assert_eq!(body["message"], json!("Upload not found"));
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

// GetCacheEntryDownloadURL integration tests live in tests/twirp_download.rs
// to keep both files under the 500-line soft limit.
