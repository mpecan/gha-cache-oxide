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
async fn dropped_response_consumer_does_not_abort_merger() {
    // Upstream-parity invariant (`pumpPartsToStreams` lines 313-346):
    // if the HTTP client disconnects mid-download, the pump stops
    // feeding the response channel but **keeps feeding** the merger
    // channel so the merged blob still lands. Pins the
    // `response_alive = false` branch in `pump`.
    let (db, adapter, tmp) = harness().await;
    let tracker = MergeTracker::new();
    let parts: &[&[u8]] = &[b"alpha-", b"beta-", b"gamma"];
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr-drop", parts).await;
    let loc = location(&*db, &location_id).await;

    let outcome = start_lazy_merge(db.clone(), adapter.clone(), &tracker, loc)
        .await
        .unwrap();
    let LazyMergeOutcome::Claimed(stream) = outcome else {
        panic!("expected claim");
    };

    // Drop the stream immediately — simulates a client that closed the
    // TCP connection before reading any bytes.
    drop(stream);

    // Shutdown waits for the merger to finish its upload despite the
    // dropped response. If pump aborted when `resp_tx.send` first
    // errored, the merger channel would close early and
    // `upload_stream` would see a short body.
    tracker.shutdown().await;

    let merged = tmp.path().join("fldr-drop").join("merged");
    let written = tokio::fs::read(&merged).await.unwrap();
    assert_eq!(
        written, b"alpha-beta-gamma",
        "merged blob must be complete even after the response consumer goes away"
    );
    let loc_after = location(&*db, &location_id).await;
    assert!(loc_after.merged_at.is_some());
    assert!(loc_after.parts_deleted_at.is_some());
}

#[tokio::test]
async fn shutdown_awaits_merge_that_is_still_uploading_when_close_fires() {
    // AC4: graceful shutdown completes in-flight merges. We inject an
    // `upload_stream` that parks on a `Notify` until released, start a
    // merge, drain the response, then call `tracker.shutdown()`. The
    // shutdown future must NOT resolve until we release the notify;
    // after it does resolve, the merged blob must be on disk. This
    // pins the `waitForOngoingMerges` upstream-parity invariant.
    use tokio::sync::Notify;

    let (db, adapter, tmp) = harness().await;
    let tracker = MergeTracker::new();
    let parts: &[&[u8]] = &[b"blocked-merge"];
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr-sh", parts).await;
    let loc = location(&*db, &location_id).await;

    let release = Arc::new(Notify::new());
    let proxy: Arc<dyn StorageAdapter> = Arc::new(GatedMergedUpload {
        inner: adapter.clone(),
        release: release.clone(),
    });

    let outcome = start_lazy_merge(db.clone(), proxy, &tracker, loc)
        .await
        .unwrap();
    let LazyMergeOutcome::Claimed(stream) = outcome else {
        panic!("expected claim");
    };
    // Drain the response — the pump finishes, but the merger is
    // parked in upload_stream waiting on `release`.
    let _ = drain(stream).await;

    // Launch shutdown in a side task. The gate is still closed, so
    // shutdown MUST be blocked on the merger.
    let tracker_for_shutdown = tracker.clone();
    let shutdown_task = tokio::spawn(async move { tracker_for_shutdown.shutdown().await });

    // Give the tokio scheduler a few ticks; the shutdown task must
    // still be running (merger is parked on the gate).
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(
        !shutdown_task.is_finished(),
        "shutdown must block until the in-flight merger completes",
    );

    // Release the gate — merger now finishes its upload, finalises
    // the DB, and the tracker's wait() returns.
    release.notify_one();
    shutdown_task.await.unwrap();

    assert_eq!(
        tokio::fs::read(tmp.path().join("fldr-sh").join("merged"))
            .await
            .unwrap(),
        b"blocked-merge",
        "merge must complete during shutdown, not be aborted",
    );
    let loc_after = location(&*db, &location_id).await;
    assert!(loc_after.merged_at.is_some());
    assert!(loc_after.parts_deleted_at.is_some());
}

#[tokio::test]
async fn finalize_rolls_back_parts_deleted_when_delete_folder_fails() {
    // Covers the rollback branch in `finalize_merge`: upload succeeds,
    // `mark_merged` runs, but `delete_folder` fails mid-tx. The tx
    // rolls back so `parts_deleted_at` stays NULL — maintaining the
    // "`parts_deleted_at` set ⇒ parts gone" invariant.
    let (db, adapter, tmp) = harness().await;
    let tracker = MergeTracker::new();
    let parts: &[&[u8]] = &[b"xy"];
    let location_id = seed_location_with_parts(&*db, adapter.as_ref(), "fldr-delfail", parts).await;
    let loc = location(&*db, &location_id).await;

    let proxy: Arc<dyn StorageAdapter> = Arc::new(FailOnDeleteFolder {
        inner: adapter.clone(),
    });

    let outcome = start_lazy_merge(db.clone(), proxy, &tracker, loc)
        .await
        .unwrap();
    let LazyMergeOutcome::Claimed(stream) = outcome else {
        panic!("expected claim");
    };
    let _ = drain(stream).await;
    tracker.shutdown().await;

    // merged_at IS set (upload succeeded); parts_deleted_at is NOT
    // set (tx rolled back on delete_folder failure). Part files are
    // still on disk — the next request will serve from merged.
    let loc_after = location(&*db, &location_id).await;
    assert!(loc_after.merged_at.is_some(), "merge upload was successful");
    assert!(
        loc_after.parts_deleted_at.is_none(),
        "delete_folder failure must roll back parts_deleted_at"
    );
    assert!(tmp.path().join("fldr-delfail").join("merged").exists());
    assert!(
        tmp.path()
            .join("fldr-delfail")
            .join("parts")
            .join("0")
            .exists(),
        "parts must NOT be deleted when delete_folder fails",
    );
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

/// Adapter whose `upload_stream` for the `/merged` key parks on a
/// `Notify` until the test releases it. Every other call forwards to
/// the inner filesystem adapter unchanged. Used to make the merger's
/// upload genuinely slow so the shutdown-awaits-merge test isn't
/// timing-dependent.
struct GatedMergedUpload {
    inner: Arc<FilesystemAdapter>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl StorageAdapter for GatedMergedUpload {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        if object_name.ends_with("/merged") {
            // Drain the body first so the pump can progress (merger
            // signals the caller via channel-close); then wait for
            // the test to release the gate before reporting success.
            let mut body = body;
            let mut buf = Vec::new();
            while let Some(chunk) = body.next().await {
                buf.extend_from_slice(&chunk?);
            }
            self.release.notified().await;
            let one_shot: ByteStream =
                futures::stream::iter(vec![Ok(bytes::Bytes::from(buf))]).boxed();
            return self.inner.upload_stream(object_name, one_shot).await;
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

/// Adapter that forwards every call to `inner` except `delete_folder`,
/// which always errors. Lets us exercise the `finalize_merge` rollback
/// branch where `mark_parts_deleted` is undone by a failing storage
/// delete.
struct FailOnDeleteFolder {
    inner: Arc<FilesystemAdapter>,
}

#[async_trait::async_trait]
impl StorageAdapter for FailOnDeleteFolder {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        self.inner.upload_stream(object_name, body).await
    }

    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        self.inner.download_stream(object_name).await
    }

    async fn delete_folder(&self, _folder_name: &str) -> Result<(), StorageError> {
        Err(StorageError::Io(std::io::Error::other(
            "injected delete failure",
        )))
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
