//! Integration coverage for the background cleanup schedulers
//! (issues #18, #73).
//!
//! Three cadences run as independent `tokio::spawn`s, all sharing the
//! same `CancellationToken`. Tests use compressed intervals (50–200 ms)
//! to drive each cadence at least once within the timeout. The
//! `disable_cleanup_jobs = true` path returns `None`; the `false` path
//! returns a live `Schedulers` whose three handles join cleanly on
//! cancellation.

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

/// Compressed schedules for tests: only the named cadence is active at
/// 50 ms; the other two are pinned to a long duration so they don't
/// fire within the test window. Lets each test isolate one cadence.
/// All variants use [`cleanup::Schedule::Every`] — cron's 1 s minimum
/// would slow the integration suite by ~20×.
/// Three long-`Every` schedules — used when a test only cares about
/// the spawn / shutdown wiring and shouldn't actually fire any cadence
/// during the test window.
fn default_test_schedules() -> cleanup::CleanupSchedules {
    let long = cleanup::Schedule::Every(Duration::from_secs(60));
    cleanup::CleanupSchedules {
        uploads: long.clone(),
        hourly: long.clone(),
        daily: long,
    }
}

fn schedules_only(active: &str) -> cleanup::CleanupSchedules {
    let long = cleanup::Schedule::Every(Duration::from_secs(60));
    let fast = cleanup::Schedule::Every(Duration::from_millis(50));
    match active {
        "uploads" => cleanup::CleanupSchedules {
            uploads: fast,
            hourly: long.clone(),
            daily: long,
        },
        "hourly" => cleanup::CleanupSchedules {
            uploads: long.clone(),
            hourly: fast,
            daily: long,
        },
        "daily" => cleanup::CleanupSchedules {
            uploads: long.clone(),
            hourly: long,
            daily: fast,
        },
        _ => unreachable!("unknown cadence: {active}"),
    }
}

fn spawn_test(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    schedules: cleanup::CleanupSchedules,
    token: CancellationToken,
) -> cleanup::Schedulers {
    cleanup::spawn_schedulers(
        cleanup::SchedulerSpawn {
            db,
            storage,
            cache_cleanup_older_than_days: 90,
            schedules,
        },
        token,
    )
}

/// Seeds a stale upload with `created_at_ms = 0` and the canonical
/// `k-up` / `scn-up` coords. Returns the new upload's id.
async fn seed_stale_upload(db: &Arc<dyn Db>) -> i64 {
    let id = new_upload_id();
    db.create_upload(NewUpload {
        id,
        coord: CacheEntryCoord {
            key: "k-up",
            version: "v",
            scope: "scn-up",
            repo_id: "r",
        },
        folder_name: "fldr-up",
        created_at_ms: 0,
    })
    .await
    .unwrap();
    id
}

/// Seeds a `storage_locations` row + matching `cache_entries` row.
/// Used to set up either a stale merge claim (caller follows up with
/// `try_mark_merge_started`) or an expired location (caller follows up
/// with `touch_location_downloaded`).
async fn seed_loc_and_entry(db: &Arc<dyn Db>, loc: &str, folder: &str, entry: &str, scope: &str) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(loc, folder, 1).await.unwrap();
    tx.seed_cache_entry(
        entry,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope,
            repo_id: "r",
        },
        0,
        loc,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// Seeds a stale merge claim on `loc-merge` (for the hourly cadence).
async fn seed_stale_merge_claim(db: &Arc<dyn Db>) {
    seed_loc_and_entry(db, "loc-merge", "fldr-merge", "entry-merge", "scn-m").await;
    assert!(db.try_mark_merge_started("loc-merge", 0).await.unwrap());
}

/// Seeds an expired location on `loc-entry` (for the daily cadence's
/// `entries` task; pair with `cache_cleanup_older_than_days = 1`).
async fn seed_expired_location(db: &Arc<dyn Db>) {
    seed_loc_and_entry(db, "loc-entry", "fldr-entry", "entry-entry", "scn-e").await;
    db.touch_location_downloaded("loc-entry", 0).await.unwrap();
}

/// Seeds an orphan `storage_locations` row (for the daily cadence's
/// `locations` task — no matching `cache_entries` row).
async fn seed_orphan_location(db: &Arc<dyn Db>) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-orphan", "fldr-orphan", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// `spawn_schedulers` wakes the uploads cadence on its `interval`,
/// runs `cleanup:uploads`, then shuts down cleanly when the
/// cancellation token fires. We seed a stale upload, spawn with
/// uploads = 50 ms (hourly/daily long), give it ~250 ms to fire at
/// least one tick, then cancel and `.await` shutdown. The stale upload
/// row must be gone.
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
    let schedulers = spawn_test(
        db.clone(),
        storage.clone(),
        schedules_only("uploads"),
        token.clone(),
    );

    // Two intervals + scheduling slack: at least one cleanup pass fires.
    tokio::time::sleep(Duration::from_millis(250)).await;

    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), schedulers.shutdown())
        .await
        .unwrap();

    assert!(
        db.find_upload_by_id(upload_id).await.unwrap().is_none(),
        "scheduler must have deleted the stale upload"
    );
}

/// Smoke test for the binary-side wiring path: when
/// `disable_cleanup_jobs` is true, `maybe_spawn` returns `None` and no
/// task is started. When false, it returns `Some(Schedulers)` and the
/// three handles stay alive until cancellation.
#[tokio::test]
async fn maybe_spawn_respects_disable_flag() {
    let db = fresh_db_arc().await;
    let (storage, _tmp) = fresh_storage_arc();
    let token = CancellationToken::new();

    let spawn_disabled = cleanup::SchedulerSpawn {
        db: db.clone(),
        storage: storage.clone(),
        cache_cleanup_older_than_days: 90,
        schedules: default_test_schedules(),
    };
    let none = cleanup::maybe_spawn(spawn_disabled, true, token.clone());
    assert!(none.is_none(), "disable_cleanup_jobs=true must skip spawn");

    let spawn_enabled = cleanup::SchedulerSpawn {
        db: db.clone(),
        storage: storage.clone(),
        cache_cleanup_older_than_days: 90,
        schedules: default_test_schedules(),
    };
    let schedulers = cleanup::maybe_spawn(spawn_enabled, false, token.clone());
    assert!(
        schedulers.is_some(),
        "disable_cleanup_jobs=false must spawn the schedulers"
    );

    // Clean shutdown of all three spawned tasks.
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), schedulers.unwrap().shutdown())
        .await
        .unwrap();
}

/// Confirms that an explicit cancellation BEFORE any tick still joins
/// every handle cleanly — none of the three should busy-wait or leak.
#[tokio::test]
async fn scheduler_cancels_before_any_tick() {
    let db = fresh_db_arc().await;
    let (storage, _tmp) = fresh_storage_arc();

    let token = CancellationToken::new();
    // Long intervals — no tick would fire within the test window.
    let schedulers = spawn_test(
        db.clone(),
        storage.clone(),
        default_test_schedules(),
        token.clone(),
    );

    // Cancel immediately — every handle should resolve fast.
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), schedulers.shutdown())
        .await
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
    async fn list_folder(
        &self,
        _: &str,
    ) -> Result<Vec<gha_cache_oxide::storage::ObjectInfo>, StorageError> {
        Ok(Vec::new())
    }
    async fn copy(&self, from: &str, _: &str) -> Result<(), StorageError> {
        Err(StorageError::ObjectNotFound(from.to_string()))
    }
    async fn signed_url(&self, _: &str) -> Result<Option<url::Url>, StorageError> {
        Ok(None)
    }
    async fn clear(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Asserts that a custom `StorageAdapter` whose `delete_folder` always
/// errors does **not** wedge the schedulers. The stale upload stays in
/// the DB (rolled back by upload-cleanup's storage-failure handling)
/// but the uploads cadence keeps ticking and shuts down cleanly.
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
    let schedulers = spawn_test(
        db.clone(),
        storage,
        schedules_only("uploads"),
        token.clone(),
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), schedulers.shutdown())
        .await
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

/// Seeds one row per cadence (stale upload for uploads, stale merge
/// claim for hourly, expired location for daily, orphan location also
/// for daily) and runs all three cadences at compressed intervals so
/// each fires at least twice within the test window. Every seeded row
/// must be processed.
///
/// `cache_cleanup_older_than_days = 1` is set inside this test (not
/// the default 90) so the seeded `last_downloaded_at = 0` location is
/// past retention against the system clock.
#[tokio::test]
async fn three_cadences_each_drive_their_own_tasks() {
    let db = fresh_db_arc().await;
    let (storage, _tmp) = fresh_storage_arc();

    let upload_id = seed_stale_upload(&db).await;
    seed_stale_merge_claim(&db).await;
    seed_expired_location(&db).await;
    seed_orphan_location(&db).await;

    let token = CancellationToken::new();
    let schedulers = cleanup::spawn_schedulers(
        cleanup::SchedulerSpawn {
            db: db.clone(),
            storage: storage.clone(),
            cache_cleanup_older_than_days: 1,
            schedules: cleanup::CleanupSchedules {
                uploads: cleanup::Schedule::Every(Duration::from_millis(50)),
                hourly: cleanup::Schedule::Every(Duration::from_millis(80)),
                daily: cleanup::Schedule::Every(Duration::from_millis(120)),
            },
        },
        token.clone(),
    );

    // Long enough that each cadence fires at least twice (daily ticks
    // every 120 ms → ~4 ticks in 500 ms).
    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), schedulers.shutdown())
        .await
        .unwrap();

    // uploads cadence
    assert!(
        db.find_upload_by_id(upload_id).await.unwrap().is_none(),
        "uploads cadence must have deleted the stale upload",
    );
    // hourly cadence: merges::run resets the claim, doesn't delete the row.
    let merge_row = db
        .find_location_for_entry("entry-merge")
        .await
        .unwrap()
        .unwrap();
    assert!(
        merge_row.merge_started_at.is_none(),
        "hourly cadence must have reset the stale merge claim",
    );
    // daily cadence: expired location is deleted (entries::run).
    assert!(
        db.find_location_for_entry("entry-entry")
            .await
            .unwrap()
            .is_none(),
        "daily cadence (entries) must have deleted the expired location",
    );
    // daily cadence: orphan location is deleted (locations::run).
    assert!(
        db.find_storage_location_by_id("loc-orphan")
            .await
            .unwrap()
            .is_none(),
        "daily cadence (locations) must have deleted the orphan location",
    );
}

/// Cadence isolation: with the uploads cadence active at 50 ms and the
/// hourly/daily cadences pinned at 60 s (won't fire in the test
/// window), only the upload should be processed. The merge claim stays
/// claimed. Proves the uploads scheduler doesn't depend on the hourly
/// tick.
#[tokio::test]
async fn cadences_are_independent() {
    let db = fresh_db_arc().await;
    let (storage, _tmp) = fresh_storage_arc();

    let upload_id = seed_stale_upload(&db).await;
    seed_stale_merge_claim(&db).await; // hourly cadence work — must NOT fire.

    let token = CancellationToken::new();
    let schedulers = spawn_test(
        db.clone(),
        storage.clone(),
        schedules_only("uploads"),
        token.clone(),
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), schedulers.shutdown())
        .await
        .unwrap();

    assert!(
        db.find_upload_by_id(upload_id).await.unwrap().is_none(),
        "uploads cadence must have deleted the stale upload",
    );
    let merge_row = db
        .find_location_for_entry("entry-merge")
        .await
        .unwrap()
        .unwrap();
    assert!(
        merge_row.merge_started_at.is_some(),
        "hourly cadence is pinned at 60 s — claim must still be set",
    );
}
