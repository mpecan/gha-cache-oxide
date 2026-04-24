//! Unit tests for the `merge` module. Attached via `#[cfg(test)] mod
//! tests;` from `merge.rs`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;

use super::*;
use crate::db::SqliteDb;
use crate::db::entities::CacheEntryCoord;
use crate::db::id::new_uuid;
use crate::storage::FilesystemAdapter;

async fn harness() -> (Arc<dyn Db>, Arc<FilesystemAdapter>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let adapter = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    (db, adapter, tmp)
}

async fn seed_location_with_parts(
    db: &dyn Db,
    adapter: &dyn StorageAdapter,
    folder: &str,
    parts: &[&[u8]],
) -> String {
    let location_id = new_uuid();
    let entry_id = new_uuid();
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(
        &location_id,
        folder,
        i64::try_from(parts.len()).expect("test parts fit in i64"),
    )
    .await
    .unwrap();
    tx.seed_cache_entry(
        &entry_id,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "s",
            repo_id: "r",
        },
        0,
        &location_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    for (i, p) in parts.iter().enumerate() {
        let name = format!("{folder}/parts/{i}");
        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![Ok(Bytes::copy_from_slice(p))];
        let stream = futures::stream::iter(chunks).boxed();
        adapter.upload_stream(&name, stream).await.unwrap();
    }
    location_id
}

/// Fetches the `storage_location` row by id, using the `SQLite`-only
/// escape hatch on the test harness. Tests thread this around instead
/// of re-implementing a trait extension.
async fn location(db: &dyn Db, location_id: &str) -> StorageLocation {
    let pool = db
        .as_sqlite_pool()
        .expect("tests use SQLite harness exclusively");
    sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind(location_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn drain(mut stream: ByteStream) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

#[tokio::test]
async fn claimed_merge_streams_parts_and_populates_merged_blob() {
    let (db, adapter, tmp) = harness().await;
    let tracker = MergeTracker::new();
    let parts: &[&[u8]] = &[b"hello ", b"lazy ", b"merge"];
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr", parts).await;
    let loc = location(&*db, &location_id).await;

    let outcome = start_lazy_merge(db.clone(), adapter.clone(), &tracker, loc)
        .await
        .unwrap();
    let stream = match outcome {
        LazyMergeOutcome::Claimed(s) => s,
        LazyMergeOutcome::LostRace => panic!("fresh location should claim"),
    };

    let bytes = drain(stream).await;
    assert_eq!(bytes, b"hello lazy merge");

    // Wait for the background merger + finalise.
    tracker.shutdown().await;

    // merged blob present on disk.
    let merged = tmp.path().join("fldr").join("merged");
    assert_eq!(tokio::fs::read(&merged).await.unwrap(), b"hello lazy merge");

    // parts folder is empty. (LocalFileSystem-backed object_store
    // deletes objects but doesn't remove now-empty parent dirs; upstream
    // `fs.rm({recursive})` does. Difference is benign — the StorageAdapter
    // contract is "no objects under this prefix", which we verify through
    // the adapter's own counting method.)
    assert_eq!(
        adapter.count_files_in_folder("fldr/parts").await.unwrap(),
        0,
        "all part files should be deleted after the merge finalises"
    );

    // DB flags reflect the completed merge.
    let loc_after = location(&*db, &location_id).await;
    assert!(loc_after.merged_at.is_some());
    assert!(loc_after.parts_deleted_at.is_some());
}

#[tokio::test]
async fn second_start_lazy_merge_returns_lost_race() {
    let (db, adapter, _tmp) = harness().await;
    let tracker = MergeTracker::new();
    let parts: &[&[u8]] = &[b"x"];
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr2", parts).await;
    let loc_first = location(&*db, &location_id).await;
    let loc_second = loc_first.clone();

    let first = start_lazy_merge(db.clone(), adapter.clone(), &tracker, loc_first)
        .await
        .unwrap();
    assert!(matches!(first, LazyMergeOutcome::Claimed(_)));

    let second = start_lazy_merge(db.clone(), adapter.clone(), &tracker, loc_second)
        .await
        .unwrap();
    assert!(matches!(second, LazyMergeOutcome::LostRace));

    // Drain the first stream so the merger completes (otherwise
    // shutdown would hang waiting for the pump/merger).
    if let LazyMergeOutcome::Claimed(s) = first {
        let _ = drain(s).await;
    }
    tracker.shutdown().await;
}

#[tokio::test]
async fn merger_resets_flags_when_upload_fails() {
    // Build a failing storage adapter: the `upload_stream` call for the
    // merged blob errors. Pump still feeds it; the merger's reset path
    // runs.
    let (db, adapter, tmp) = harness().await;
    let tracker = MergeTracker::new();
    let parts: &[&[u8]] = &[b"abc"];
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr-fail", parts).await;
    let loc = location(&*db, &location_id).await;

    // Wrap the adapter in a "fail on merged-upload" proxy.
    let proxy: Arc<dyn StorageAdapter> = Arc::new(FailOnMergedUpload {
        inner: adapter.clone(),
    });

    let outcome = start_lazy_merge(db.clone(), proxy, &tracker, loc)
        .await
        .unwrap();
    let LazyMergeOutcome::Claimed(stream) = outcome else {
        panic!("expected claim");
    };
    // Drain — client still sees the bytes even though the merger fails.
    let got = drain(stream).await;
    assert_eq!(got, b"abc");

    tracker.shutdown().await;

    // Parts still there (no deletion), flags reset, merged absent.
    // We check via the inner FilesystemAdapter so we bypass the failure
    // injector and see the real filesystem state.
    assert_eq!(
        adapter
            .count_files_in_folder("fldr-fail/parts")
            .await
            .unwrap(),
        1,
        "failed merge must not delete the parts"
    );
    assert!(!tmp.path().join("fldr-fail").join("merged").exists());
    let loc_after = location(&*db, &location_id).await;
    assert!(loc_after.merge_started_at.is_none());
    assert!(loc_after.merged_at.is_none());
    assert!(loc_after.parts_deleted_at.is_none());
}

/// Adapter that forwards every call to `inner` except `upload_stream`
/// for `/merged` paths, which always errors. Lets us simulate an
/// upload failure during the lazy merge.
struct FailOnMergedUpload {
    inner: Arc<FilesystemAdapter>,
}

#[async_trait::async_trait]
impl StorageAdapter for FailOnMergedUpload {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        if object_name.ends_with("/merged") {
            // Drain the body so the pump isn't blocked on backpressure.
            let mut body = body;
            while body.next().await.is_some() {}
            return Err(StorageError::Io(std::io::Error::other("injected failure")));
        }
        self.inner.upload_stream(object_name, body).await
    }

    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        self.inner.download_stream(object_name).await
    }

    async fn delete_folder(&self, folder_name: &str) -> Result<(), StorageError> {
        self.inner.delete_folder(folder_name).await
    }

    async fn count_files_in_folder(&self, folder_name: &str) -> Result<u64, StorageError> {
        self.inner.count_files_in_folder(folder_name).await
    }

    async fn signed_url(&self, object_name: &str) -> Result<Option<url::Url>, StorageError> {
        self.inner.signed_url(object_name).await
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.inner.clear().await
    }
}
