//! `POST /management/cleanup/trigger` — one-shot cleanup pass.

use axum::http::{Method, StatusCode};
use gha_cache_oxide::db::entities::{CacheEntryCoord, NewUpload};
use gha_cache_oxide::db::id::new_upload_id;
use tower::ServiceExt;

use super::common::{KEY, body_json, harness, req};

#[tokio::test]
async fn cleanup_trigger_runs_pass_and_returns_report() {
    let h = harness(Some(KEY)).await;
    // Seed a stale upload — the cleanup pass should reap it.
    let upload_id = new_upload_id();
    h.db.create_upload(NewUpload {
        id: upload_id,
        coord: CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-cleanup",
            repo_id: "r",
        },
        folder_name: "fldr-cleanup-stale",
        // createdAt = 0 → unconditionally older than the 90-day cutoff.
        created_at_ms: 0,
    })
    .await
    .unwrap();

    let resp = h
        .router
        .oneshot(req(Method::POST, "/management/cleanup/trigger", Some(KEY)))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    // Body shape — the keys the JS client / shell scripts will read.
    for k in [
        "merges_reset",
        "uploads_deleted",
        "parts_deleted",
        "entries_deleted",
        "locations_deleted",
    ] {
        assert!(body.get(k).is_some(), "expected {k} in response: {body}");
    }
    assert_eq!(body["uploads_deleted"], 1, "stale upload must be reaped");
    assert!(
        h.db.find_upload_by_id(upload_id).await.unwrap().is_none(),
        "cleanup must have deleted the stale upload row",
    );
}
