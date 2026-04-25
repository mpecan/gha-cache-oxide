//! Integration coverage for the background cleanup scheduler
//! (issue #18).
//!
//! `spawn_scheduler` is exercised at a fast 50 ms cadence so a few
//! ticks fire deterministically; the test cancels the token and joins
//! the handle. `maybe_spawn` is exercised in both modes — the
//! `disable_cleanup_jobs = true` path returns `None` (no scheduler
//! running) and the `false` path returns a live handle.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use gha_cache_oxide::db::entities::{CacheEntryCoord, NewUpload};
use gha_cache_oxide::db::id::new_upload_id;
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::storage::{ByteStream, FilesystemAdapter, StorageAdapter, StorageError};
use gha_cache_oxide::tasks::cleanup;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

async fn fresh_db_arc() -> Arc<dyn Db> {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    Arc::new(db)
}

fn fresh_storage_arc() -> (Arc<dyn StorageAdapter>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let adapter = FilesystemAdapter::new(dir.path()).unwrap();
    (Arc::new(adapter), dir)
}

/// `spawn_scheduler` wakes on its `interval`, runs `run_all`, then
/// shuts down cleanly when the cancellation token fires. We seed a
/// stale upload, spawn with a 50 ms interval, give it ~250 ms to fire
/// at least one tick, then cancel and `.await` the handle. The stale
/// upload row must be gone.
#[tokio::test]
async fn scheduler_runs_then_cancels_cleanly() {
    let db = fresh_db_arc().await;
    let (storage, _tmp) = fresh_storage_arc();

    let upload_id = new_upload_id();
    db.create_upload(NewUpload {
        id: upload_id,
        coord: CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scn-sched",
            repo_id: "r",
        },
        folder_name: "fldr-sched",
        // createdAt = 0 → unconditionally older than the 1-min cutoff.
        created_at_ms: 0,
    })
    .await
    .unwrap();

    let token = CancellationToken::new();
    let handle = cleanup::spawn_scheduler(
        db.clone(),
        storage.clone(),
        90,
        Duration::from_millis(50),
        token.clone(),
    );

    // Two intervals + scheduling slack: at least one cleanup pass fires.
    tokio::time::sleep(Duration::from_millis(250)).await;

    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();

    assert!(
        db.find_upload_by_id(upload_id).await.unwrap().is_none(),
        "scheduler must have deleted the stale upload"
    );
}

/// Smoke test for the binary-side wiring path: when
/// `disable_cleanup_jobs` is true, `maybe_spawn` returns `None` and no
/// task is started. When false, it returns `Some(handle)` and the
/// handle stays alive until cancellation.
#[tokio::test]
async fn maybe_spawn_respects_disable_flag() {
    let db = fresh_db_arc().await;
    let (storage, _tmp) = fresh_storage_arc();
    let token = CancellationToken::new();

    let none = cleanup::maybe_spawn(db.clone(), storage.clone(), 90, true, token.clone());
    assert!(none.is_none(), "disable_cleanup_jobs=true must skip spawn");

    let handle = cleanup::maybe_spawn(db.clone(), storage.clone(), 90, false, token.clone());
    assert!(
        handle.is_some(),
        "disable_cleanup_jobs=false must spawn the scheduler"
    );

    // Clean shutdown of the spawned task.
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle.unwrap())
        .await
        .unwrap()
        .unwrap();
}

/// Confirms that an explicit cancellation BEFORE the first tick still
/// joins cleanly — the scheduler shouldn't busy-wait or leak.
#[tokio::test]
async fn scheduler_cancels_before_any_tick() {
    let db = fresh_db_arc().await;
    let (storage, _tmp) = fresh_storage_arc();

    let token = CancellationToken::new();
    let handle = cleanup::spawn_scheduler(
        db.clone(),
        storage.clone(),
        90,
        // Long enough that we'd never see a tick within the test.
        Duration::from_secs(60),
        token.clone(),
    );

    // Cancel immediately — handle should resolve fast.
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

struct AlwaysFailDelete {
    delete_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl StorageAdapter for AlwaysFailDelete {
    async fn upload_stream(&self, _: &str, _: ByteStream) -> Result<(), StorageError> {
        Ok(())
    }
    async fn download_stream(&self, n: &str) -> Result<ByteStream, StorageError> {
        Err(StorageError::ObjectNotFound(n.to_string()))
    }
    async fn delete_folder(&self, _: &str) -> Result<(), StorageError> {
        self.delete_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(StorageError::Io(std::io::Error::other("always fails")))
    }
    async fn count_files_in_folder(&self, _: &str) -> Result<u64, StorageError> {
        Ok(0)
    }
    async fn signed_url(&self, _: &str) -> Result<Option<url::Url>, StorageError> {
        Ok(None)
    }
    async fn clear(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Asserts that a custom `StorageAdapter` whose `delete_folder` always
/// errors does **not** wedge the scheduler. The stale upload stays in
/// the DB (rolled back by upload-cleanup's storage-failure handling)
/// but the scheduler keeps ticking and shuts down cleanly.
///
/// `delete_calls` proves at least one tick actually fired — without
/// it, a "preserved upload" assertion would also pass for a scheduler
/// that never woke up.
#[tokio::test]
async fn scheduler_keeps_running_when_storage_fails() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = fresh_db_arc().await;
    let delete_calls = Arc::new(AtomicUsize::new(0));
    let storage: Arc<dyn StorageAdapter> = Arc::new(AlwaysFailDelete {
        delete_calls: delete_calls.clone(),
    });

    let upload_id = new_upload_id();
    db.create_upload(NewUpload {
        id: upload_id,
        coord: CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scn-sched-fail",
            repo_id: "r",
        },
        folder_name: "fldr-sched-fail",
        created_at_ms: 0,
    })
    .await
    .unwrap();

    let token = CancellationToken::new();
    let handle = cleanup::spawn_scheduler(
        db.clone(),
        storage,
        90,
        Duration::from_millis(50),
        token.clone(),
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();

    // At least one tick actually fired — otherwise the upload-row
    // assertion below would silently pass for a stuck scheduler.
    assert!(
        delete_calls.load(Ordering::SeqCst) >= 1,
        "scheduler must have ticked at least once",
    );
    // Storage delete always failed → upload row preserved.
    assert!(
        db.find_upload_by_id(upload_id).await.unwrap().is_some(),
        "storage failure must roll back the DB delete (upload preserved)",
    );
}
