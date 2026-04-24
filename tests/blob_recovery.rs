//! Download-path recovery tests for issue #17.
//!
//! Kept separate from `tests/blob.rs` because the hard 700-line file
//! limit leaves no room for additions there. These tests seed the
//! `(DB, storage)` state directly via the `Db` trait + filesystem
//! primitives — no Twirp roundtrip — so they stay small and focused.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod twirp_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use gha_cache_oxide::db::entities::CacheEntryCoord;
use serde_json::json;
use tower::ServiceExt;

use twirp_common::{Harness, body_json, harness};

fn get_download(entry_id: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/download/{entry_id}"))
        .body(Body::empty())
        .unwrap()
}

async fn collect_body(resp: axum::response::Response) -> Vec<u8> {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    bytes.to_vec()
}

/// Seeds a `storage_locations` + `cache_entries` row for the test and
/// returns `(entry_id, folder_name)`. The caller writes the part files
/// (or not) and twiddles `mergedAt` / `partsDeletedAt` as the scenario
/// demands.
async fn seed_entry(h: &Harness, scope: &str, part_count: i64) -> (String, String) {
    let location_id = format!("loc-recov-{scope}");
    let folder = format!("folder-recov-{scope}");
    let entry_id = format!("entry-recov-{scope}");
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location(&location_id, &folder, part_count)
        .await
        .unwrap();
    tx.seed_cache_entry(
        &entry_id,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope,
            repo_id: "r",
        },
        0,
        &location_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    (entry_id, folder)
}

#[tokio::test]
async fn download_falls_through_to_parts_when_merged_blob_missing_but_parts_intact() {
    // Backup-restore edge case: DB claims `mergedAt` but the merged
    // blob never landed on storage, while parts (unusually) remain.
    // The download path must log + fall through to parts rather than
    // surfacing 404 on the stale DB state.
    let h = harness().await;
    let scope = "blob-missing-parts-here";
    let (entry_id, folder) = seed_entry(&h, scope, 1).await;

    // Write one part file on disk.
    let parts_dir = h.tmp.path().join(&folder).join("parts");
    tokio::fs::create_dir_all(&parts_dir).await.unwrap();
    tokio::fs::write(parts_dir.join("0"), b"payload")
        .await
        .unwrap();

    // Flip the DB into "merged" without producing a merged blob.
    let location_id = format!("loc-recov-{scope}");
    h.db.try_mark_merge_started(&location_id, 100)
        .await
        .unwrap();
    h.db.mark_merged(&location_id, 200).await.unwrap();

    assert!(
        !h.tmp.path().join(&folder).join("merged").exists(),
        "merged blob must be absent to exercise the fall-through"
    );

    let resp = h.router.oneshot(get_download(&entry_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(collect_body(resp).await, b"payload");
}

#[tokio::test]
async fn download_falls_through_to_404_when_merged_blob_and_parts_both_gone() {
    // Operator deletion / post-merge storage loss: `mergedAt` set,
    // merged blob gone, `partsDeletedAt` also set. The fall-through
    // must reach the `partsDeletedAt` branch and return 404 — not a
    // 500, and not a truncated body.
    let h = harness().await;
    let scope = "everything-gone";
    let (entry_id, folder) = seed_entry(&h, scope, 1).await;

    let location_id = format!("loc-recov-{scope}");
    h.db.try_mark_merge_started(&location_id, 100)
        .await
        .unwrap();
    h.db.mark_merged(&location_id, 200).await.unwrap();

    let mut tx = h.db.begin().await.unwrap();
    tx.mark_parts_deleted(&location_id, 300).await.unwrap();
    tx.commit().await.unwrap();

    // No parts on disk, no merged blob — the state matches the DB.
    assert!(
        !h.tmp.path().join(&folder).exists(),
        "nothing should exist on disk for this entry"
    );

    let resp = h.router.oneshot(get_download(&entry_id)).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], json!("Cache file not found"));
}
