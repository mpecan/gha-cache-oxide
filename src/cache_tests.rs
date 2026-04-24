//! Tests for [`complete_upload`]. Attached via `#[path]` from cache.rs
//! so both production code and tests stay well under the 500-line soft
//! limit. Each test drives the real schema + a temp-dir filesystem
//! adapter to exercise the DB/storage interaction end-to-end.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::sync::Arc;

use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

use super::*;
use crate::db::entities::{CacheEntryCoord, NewUpload};
use crate::db::id::new_upload_id;
use crate::storage::FilesystemAdapter;

struct TestFixture {
    db: Db,
    adapter: Arc<FilesystemAdapter>,
    tmp: TempDir,
}

async fn fixture() -> TestFixture {
    let tmp = TempDir::new().unwrap();
    let db = Db::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let adapter = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    TestFixture { db, adapter, tmp }
}

fn coord<'a>() -> CacheEntryCoord<'a> {
    CacheEntryCoord {
        key: "build-cache",
        version: "v1",
        scope: "refs/heads/main",
        repo_id: "42",
    }
}

/// Seeds an upload row + drops `parts_on_disk` empty files at
/// `<folder>/parts/{i}` so the disk-count check sees the expected
/// number. Returns the upload id.
async fn seed_upload(fx: &TestFixture, started: i64, finished: i64, parts_on_disk: usize) -> i64 {
    let id = new_upload_id();
    let folder = id.to_string();
    fx.db
        .create_upload(NewUpload {
            id,
            coord: coord(),
            folder_name: &folder,
            created_at_ms: 0,
        })
        .await
        .unwrap();
    // Bring the counters up to the requested values via the existing
    // increment helpers so the fixture matches real post-upload state.
    for _ in 0..started {
        fx.db.increment_upload_started(id).await.unwrap();
    }
    for _ in 0..finished {
        fx.db.increment_upload_finished(id, 0).await.unwrap();
    }
    // Drop empty files at <folder>/parts/{i} on disk. Bypass the
    // adapter here; we want a raw directory structure without feeding
    // streams through the public API.
    let parts_dir = fx.tmp.path().join(&folder).join("parts");
    tokio::fs::create_dir_all(&parts_dir).await.unwrap();
    for i in 0..parts_on_disk {
        let path = parts_dir.join(i.to_string());
        let mut f = tokio::fs::File::create(&path).await.unwrap();
        f.write_all(b"x").await.unwrap();
    }
    id
}

async fn count_uploads(db: &Db) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM uploads")
        .fetch_one(db.sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap()
}

async fn count_storage_locations(db: &Db) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM storage_locations")
        .fetch_one(db.sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap()
}

async fn count_cache_entries(db: &Db) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cache_entries")
        .fetch_one(db.sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap()
}

#[tokio::test]
async fn happy_path_commits_entry_and_removes_upload() {
    let fx = fixture().await;
    let id = seed_upload(&fx, 2, 2, 2).await;

    let upload = complete_upload(
        &fx.db,
        fx.adapter.as_ref(),
        CompleteUploadParams {
            coord: coord(),
            now_ms: 1_000,
        },
    )
    .await
    .unwrap();

    assert_eq!(upload.id, id);
    assert_eq!(count_uploads(&fx.db).await, 0, "uploads row should be gone");
    assert_eq!(count_cache_entries(&fx.db).await, 1);
    assert_eq!(count_storage_locations(&fx.db).await, 1);
}

#[tokio::test]
async fn missing_upload_returns_not_found_without_mutating_state() {
    let fx = fixture().await;

    let err = complete_upload(
        &fx.db,
        fx.adapter.as_ref(),
        CompleteUploadParams {
            coord: coord(),
            now_ms: 0,
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(err, CompleteUploadError::UploadNotFound));
    assert_eq!(count_storage_locations(&fx.db).await, 0);
    assert_eq!(count_cache_entries(&fx.db).await, 0);
}

#[tokio::test]
async fn no_parts_uploaded_deletes_row_and_returns_error() {
    let fx = fixture().await;
    let _id = seed_upload(&fx, 0, 0, 0).await;

    let err = complete_upload(
        &fx.db,
        fx.adapter.as_ref(),
        CompleteUploadParams {
            coord: coord(),
            now_ms: 0,
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(err, CompleteUploadError::NoPartsUploaded));
    assert_eq!(count_uploads(&fx.db).await, 0);
    assert_eq!(count_cache_entries(&fx.db).await, 0);
}

#[tokio::test]
async fn started_finished_mismatch_deletes_row_and_returns_error() {
    let fx = fixture().await;
    let _id = seed_upload(&fx, 2, 1, 1).await;

    let err = complete_upload(
        &fx.db,
        fx.adapter.as_ref(),
        CompleteUploadParams {
            coord: coord(),
            now_ms: 0,
        },
    )
    .await
    .unwrap_err();

    match err {
        CompleteUploadError::PartsCountMismatch { started, finished } => {
            assert_eq!(started, 2);
            assert_eq!(finished, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(count_uploads(&fx.db).await, 0);
    assert_eq!(count_cache_entries(&fx.db).await, 0);
}

#[tokio::test]
async fn disk_count_mismatch_deletes_row_and_returns_error() {
    let fx = fixture().await;
    // DB thinks 2 parts uploaded; only 1 on disk.
    let _id = seed_upload(&fx, 2, 2, 1).await;

    let err = complete_upload(
        &fx.db,
        fx.adapter.as_ref(),
        CompleteUploadParams {
            coord: coord(),
            now_ms: 0,
        },
    )
    .await
    .unwrap_err();

    match err {
        CompleteUploadError::DiskCountMismatch { db, disk } => {
            assert_eq!(db, 2);
            assert_eq!(disk, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(count_uploads(&fx.db).await, 0);
}

#[tokio::test]
async fn overwriting_existing_entry_deletes_previous_location_row() {
    let fx = fixture().await;

    // Seed an existing cache_entry + storage_location at the same coord
    // so the upsert takes the update path.
    let mut tx = fx.db.begin().await.unwrap();
    insert_storage_location_tx(&mut tx, "old-loc", "old-folder", 1)
        .await
        .unwrap();
    let _ = upsert_cache_entry_tx(&mut tx, coord(), "old-loc", 500)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    tokio::fs::create_dir_all(fx.tmp.path().join("old-folder").join("parts"))
        .await
        .unwrap();

    assert_eq!(count_storage_locations(&fx.db).await, 1);
    assert_eq!(count_cache_entries(&fx.db).await, 1);

    // Now the fresh upload.
    let _ = seed_upload(&fx, 1, 1, 1).await;

    let _ = complete_upload(
        &fx.db,
        fx.adapter.as_ref(),
        CompleteUploadParams {
            coord: coord(),
            now_ms: 1_000,
        },
    )
    .await
    .unwrap();

    // Only one cache_entry (updated), only one storage_location (new),
    // uploads gone.
    assert_eq!(count_cache_entries(&fx.db).await, 1);
    assert_eq!(count_storage_locations(&fx.db).await, 1);
    assert_eq!(count_uploads(&fx.db).await, 0);

    // The cache_entry row points at the NEW location, not old-loc.
    let loc_id: String = sqlx::query_scalar("SELECT locationId FROM cache_entries WHERE key = ?")
        .bind("build-cache")
        .fetch_one(fx.db.sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_ne!(loc_id, "old-loc");
}

#[tokio::test]
async fn overwriting_existing_entry_deletes_previous_folder_from_storage() {
    let fx = fixture().await;

    // Same setup as the previous test, but inspect the filesystem
    // afterwards. The old folder must be gone.
    let mut tx = fx.db.begin().await.unwrap();
    insert_storage_location_tx(&mut tx, "old-loc", "old-folder", 1)
        .await
        .unwrap();
    let _ = upsert_cache_entry_tx(&mut tx, coord(), "old-loc", 500)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Put a sentinel file inside old-folder/parts to prove delete_folder ran.
    let old_parts = fx.tmp.path().join("old-folder").join("parts");
    tokio::fs::create_dir_all(&old_parts).await.unwrap();
    tokio::fs::File::create(old_parts.join("0"))
        .await
        .unwrap()
        .write_all(b"sentinel")
        .await
        .unwrap();
    assert!(old_parts.join("0").exists());

    let _ = seed_upload(&fx, 1, 1, 1).await;
    let _ = complete_upload(
        &fx.db,
        fx.adapter.as_ref(),
        CompleteUploadParams {
            coord: coord(),
            now_ms: 1_000,
        },
    )
    .await
    .unwrap();

    assert!(
        !old_parts.join("0").exists(),
        "the old upload's folder should be deleted from storage"
    );
}
