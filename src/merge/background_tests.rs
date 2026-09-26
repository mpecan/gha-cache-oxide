//! Tests for [`start_background_merge`] (the post-commit merge) and the
//! "location vanished mid-merge" guard in `run_merger`. Sibling of
//! `tests.rs` (kept separate for the file-length limit); reuses its
//! harness.

use std::sync::Arc;

use tokio::sync::Notify;

use super::tests::{GatedMergedUpload, harness, location, seed_location_with_parts};
use super::*;

#[tokio::test]
async fn background_merge_completes_without_a_reader() {
    let (db, adapter, tmp) = harness().await;
    let tracker = MergeTracker::new();
    let parts: &[&[u8]] = &[b"alpha-", b"beta-", b"gamma"];
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr-bg", parts).await;
    let loc = location(&*db, &location_id).await;

    let storage: Arc<dyn StorageAdapter> = adapter.clone();
    assert!(
        start_background_merge(db.clone(), storage, &tracker, loc)
            .await
            .unwrap()
    );
    tracker.shutdown().await;

    let merged = tokio::fs::read(tmp.path().join("fldr-bg").join("merged"))
        .await
        .unwrap();
    assert_eq!(merged, b"alpha-beta-gamma");
    let after = location(&*db, &location_id).await;
    assert!(after.merged_at.is_some(), "merge must be finalised");
    assert!(after.parts_deleted_at.is_some(), "parts must be cleaned up");
    assert_eq!(tracker.metrics.merges.completed.get(), 1);
    assert_eq!(tracker.metrics.merges.duration.count(), 1);
    assert_eq!(tracker.metrics.merges.failed.get(), 0);
}

#[tokio::test]
async fn second_background_merge_loses_the_claim() {
    let (db, adapter, _tmp) = harness().await;
    let tracker = MergeTracker::new();
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr-2x", &[b"x"]).await;
    let release = Arc::new(Notify::new());
    let gated: Arc<dyn StorageAdapter> = Arc::new(GatedMergedUpload {
        inner: adapter.clone(),
        release: release.clone(),
    });

    let loc = location(&*db, &location_id).await;
    assert!(
        start_background_merge(db.clone(), gated.clone(), &tracker, loc.clone())
            .await
            .unwrap()
    );
    assert!(
        !start_background_merge(db.clone(), gated, &tracker, loc)
            .await
            .unwrap(),
        "a merge already holds the claim",
    );
    release.notify_one();
    tracker.shutdown().await;
    assert_eq!(tracker.metrics.merges.completed.get(), 1);
}

/// A key re-committed while its previous location was still merging:
/// the supersede deletes the location row and folder, then the merger's
/// `merged` upload lands. It must be dropped, not leaked into a folder
/// no row points at.
#[tokio::test]
async fn merge_finishing_after_its_location_was_deleted_leaves_no_blob() {
    let (db, adapter, _tmp) = harness().await;
    let tracker = MergeTracker::new();
    let location_id =
        seed_location_with_parts(&*db, adapter.as_ref(), "fldr-gone", &[b"late"]).await;
    let release = Arc::new(Notify::new());
    let gated: Arc<dyn StorageAdapter> = Arc::new(GatedMergedUpload {
        inner: adapter.clone(),
        release: release.clone(),
    });

    let loc = location(&*db, &location_id).await;
    assert!(
        start_background_merge(db.clone(), gated, &tracker, loc)
            .await
            .unwrap()
    );
    // Supersede while the merged upload is parked on the gate.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let mut tx = db.begin().await.unwrap();
    tx.delete_storage_location(&location_id).await.unwrap();
    tx.commit().await.unwrap();
    adapter.delete_folder("fldr-gone").await.unwrap();

    release.notify_one();
    tracker.shutdown().await;

    let left = adapter.list_folder("fldr-gone").await.unwrap();
    assert!(left.is_empty(), "orphaned merge leaked: {left:?}");
    assert_eq!(tracker.metrics.merges.completed.get(), 0);
}
