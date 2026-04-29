//! Background cleanup orchestration (issue #18).
//!
//! Houses the per-task modules (`uploads`, `merges`, `parts`,
//! `entries`, `locations`) — each a port of an upstream
//! `tasks/cleanup/*.ts` file — plus the single hourly scheduler that
//! drives them all in sequence.
//!
//! # Scheduler shape
//!
//! Per the issue's explicit direction, the scheduler is one
//! `tokio::spawn` running an `interval(Duration::from_secs(3600))`
//! that calls [`run_all`] every tick. We don't pull in cron expressions
//! (upstream uses `croner`); if operators ever need per-task cadences
//! we can add them later. The first interval tick fires immediately —
//! we skip it so startup isn't blocked on a cleanup pass.
//!
//! # Per-task ordering inside `run_all`
//!
//! 1. **merges** — reset stale claims first so any download waiting
//!    on `mergeStartedAt` can retry on the next request.
//! 2. **uploads** — drop abandoned uploads (re-checks staleness inside
//!    each row's tx; see [`crate::db::DbTx::delete_upload_if_stale`]).
//! 3. **parts** — reap `<folder>/parts/` for already-merged rows.
//! 4. **entries** — delete `storage_locations` whose
//!    `lastDownloadedAt` is past the operator-configured retention.
//! 5. **locations** — pick up any orphan `storage_locations` rows
//!    (overwrites, imports, etc.).

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::db::Db;
use crate::db::id::now_ms;
use crate::storage::StorageAdapter;

pub(crate) mod entries;
pub(crate) mod locations;
pub(crate) mod merges;
pub(crate) mod parts;
#[cfg(test)]
mod test_utils;
pub(crate) mod uploads;

/// Scheduler cadence used by the production binary. Matches the
/// issue's "single `tokio::spawn` with `interval(3600)`" direction.
pub const PRODUCTION_INTERVAL: Duration = Duration::from_secs(3600);

/// Per-task counts produced by a single [`run_all`] pass. Logged at
/// `info` after every cycle so operators can see what was reaped.
///
/// Serialised as the JSON body of the `POST /management/cleanup/trigger`
/// endpoint (issue #19), which is why the fields stay `pub` and the
/// derive includes `Serialize`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CleanupReport {
    pub merges_reset: u64,
    pub uploads_deleted: u64,
    pub parts_deleted: u64,
    pub entries_deleted: u64,
    pub locations_deleted: u64,
}

/// Runs every cleanup task in sequence against `db` + `storage`.
///
/// Each task is best-effort and logs its own errors at `warn`; this
/// function never propagates a `Result` because a partial pass is
/// strictly better than aborting and waiting for the next tick.
pub async fn run_all(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    now_ms: i64,
    cache_cleanup_older_than_days: u32,
) -> CleanupReport {
    let merges_reset = merges::run(db, now_ms).await;
    let uploads_deleted = uploads::run(db, storage, now_ms).await;
    let parts_deleted = parts::run(db, storage, now_ms).await;
    let entries_deleted = entries::run(db, storage, now_ms, cache_cleanup_older_than_days).await;
    let locations_deleted = locations::run(db, storage).await;
    CleanupReport {
        merges_reset,
        uploads_deleted,
        parts_deleted,
        entries_deleted,
        locations_deleted,
    }
}

/// Spawns the background scheduler. The returned handle resolves once
/// `shutdown` is cancelled and the in-flight cycle (if any) finishes.
///
/// Tests pass a small `interval` (e.g. 50 ms) plus a token they own
/// so a few ticks fire deterministically before cancellation.
pub fn spawn_scheduler(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    cache_cleanup_older_than_days: u32,
    interval: Duration,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // First tick fires immediately — consume it so startup isn't
        // blocked on a cleanup pass.
        ticker.tick().await;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!("cleanup scheduler shutting down");
                    break;
                }
                _ = ticker.tick() => {
                    let report = run_all(
                        &*db,
                        &*storage,
                        now_ms(),
                        cache_cleanup_older_than_days,
                    ).await;
                    tracing::info!(?report, "cleanup cycle complete");
                }
            }
        }
    })
}

/// Convenience wrapper used by `src/main.rs`.
///
/// Spawns the scheduler iff `disable_cleanup_jobs` is `false`, logging
/// the decision either way. Returns `None` when disabled. Tests bypass
/// this and call [`spawn_scheduler`] directly so they own the
/// `CancellationToken`.
pub fn maybe_spawn(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    cache_cleanup_older_than_days: u32,
    disable_cleanup_jobs: bool,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    if disable_cleanup_jobs {
        tracing::info!("DISABLE_CLEANUP_JOBS=true - cleanup scheduler not started");
        return None;
    }
    tracing::info!(
        interval_secs = PRODUCTION_INTERVAL.as_secs(),
        cache_cleanup_older_than_days,
        "cleanup scheduler starting",
    );
    Some(spawn_scheduler(
        db,
        storage,
        cache_cleanup_older_than_days,
        PRODUCTION_INTERVAL,
        shutdown,
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::test_utils::FakeStorage;
    use super::{CleanupReport, run_all};
    use crate::db::entities::{CacheEntryCoord, NewUpload};
    use crate::db::id::new_upload_id;
    use crate::db::{Db, SqliteDb};

    async fn fresh_db() -> SqliteDb {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db
    }

    /// Bundles the four `&str` plus `i64` part-count fields of
    /// `seed_loc_with_entry` to stay under the 5-arg clippy threshold.
    struct LocSeed<'a> {
        loc: &'a str,
        folder: &'a str,
        entry: &'a str,
        scope: &'a str,
        part_count: i64,
    }

    async fn seed_loc_with_entry(db: &SqliteDb, seed: LocSeed<'_>) {
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(seed.loc, seed.folder, seed.part_count)
            .await
            .unwrap();
        tx.seed_cache_entry(
            seed.entry,
            CacheEntryCoord {
                key: "k",
                version: "v",
                scope: seed.scope,
                repo_id: "r",
            },
            0,
            seed.loc,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    async fn seed_orphan_loc(db: &SqliteDb, loc: &str, folder: &str) {
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(loc, folder, 1).await.unwrap();
        tx.commit().await.unwrap();
    }

    async fn seed_one_per_task(db: &SqliteDb) {
        // merges: stale claim
        seed_loc_with_entry(
            db,
            LocSeed {
                loc: "loc-m",
                folder: "fldr-m",
                entry: "entry-m",
                scope: "scn-m",
                part_count: 1,
            },
        )
        .await;
        assert!(db.try_mark_merge_started("loc-m", 0).await.unwrap());

        // uploads: stale upload
        db.create_upload(NewUpload {
            id: new_upload_id(),
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

        // parts: merged + parts not yet deleted
        seed_loc_with_entry(
            db,
            LocSeed {
                loc: "loc-p",
                folder: "fldr-p",
                entry: "entry-p",
                scope: "scn-p",
                part_count: 7,
            },
        )
        .await;
        assert!(db.try_mark_merge_started("loc-p", 100).await.unwrap());
        db.mark_merged("loc-p", 200).await.unwrap();

        // entries: expired location (lastDownloadedAt = 0, retention 1 day)
        seed_loc_with_entry(
            db,
            LocSeed {
                loc: "loc-e",
                folder: "fldr-e",
                entry: "entry-e",
                scope: "scn-e",
                part_count: 1,
            },
        )
        .await;
        db.touch_location_downloaded("loc-e", 0).await.unwrap();

        // locations: orphan with no cache_entries row
        seed_orphan_loc(db, "loc-o", "fldr-o").await;
    }

    /// `run_all` returns a combined `CleanupReport` summing every
    /// per-task pass, and each task actually runs (we seed one row
    /// per task and watch them all transition).
    #[tokio::test]
    async fn run_all_returns_combined_report_and_drives_every_task() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        seed_one_per_task(&db).await;

        let now = 2 * 86_400_000;
        let report = run_all(&db, &storage, now, 1).await;

        assert_eq!(
            report,
            CleanupReport {
                merges_reset: 1,
                uploads_deleted: 1,
                parts_deleted: 7,
                entries_deleted: 1,
                locations_deleted: 1,
            }
        );

        // Storage observed every per-row delete (one for uploads + one
        // for parts/parts + one for entries + one for locations).
        let mut deleted: Vec<String> = storage.deleted_folders();
        deleted.sort();
        assert_eq!(
            deleted,
            vec![
                "fldr-e".to_string(),
                "fldr-o".to_string(),
                "fldr-p/parts".to_string(),
                "fldr-up".to_string(),
            ]
        );
    }
}
