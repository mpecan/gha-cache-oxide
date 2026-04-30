//! Background cleanup orchestration (issues #18, #73).
//!
//! Houses the per-task modules (`uploads`, `merges`, `parts`,
//! `entries`, `locations`) — each a port of an upstream
//! `tasks/cleanup/*.ts` file — plus the three independent schedulers
//! that drive them (see [`scheduler`]).
//!
//! # Three cadences (port of upstream `nitro.config.ts:25`)
//!
//! Upstream schedules its five cleanup tasks across three cron lines;
//! we mirror that with three `tokio::spawn`s sharing one
//! [`tokio_util::sync::CancellationToken`] for graceful shutdown:
//!
//! | Cadence  | Default | Tasks                             | Cron upstream |
//! |----------|---------|-----------------------------------|---------------|
//! | uploads  | 5 min   | `cleanup:uploads`                 | `*/5 * * * *` |
//! | hourly   | 1 h     | `cleanup:parts`, `cleanup:merges` | `0 * * * *`   |
//! | daily    | 24 h    | `cleanup:cache-entries`,          | `0 0 * * *`   |
//! |          |         | `cleanup:storage-locations`       |               |
//!
//! Defaults match upstream cron exactly. Operators can override each
//! cadence via `CLEANUP_{UPLOADS,HOURLY,DAILY}_SCHEDULE` env vars
//! (5-field upstream syntax or 6-field with explicit seconds). The
//! `DISABLE_CLEANUP_JOBS` env var still gates all three.
//!
//! # On-demand
//!
//! [`run_all`] still drives every per-task `run` in sequence, used by
//! `POST /management/cleanup/trigger` for operators who want a
//! one-shot reap without waiting for the next tick.
//! [`spawn_locations_sweep`] handles the issue #71 on-demand orphan
//! reap fired off after a management cache-entry DELETE.

use serde::Serialize;
use tokio::task::JoinHandle;

use crate::db::Db;
use crate::storage::StorageAdapter;

pub(crate) mod entries;
pub(crate) mod locations;
pub(crate) mod merges;
pub(crate) mod parts;
mod scheduler;
#[cfg(test)]
mod test_utils;
pub(crate) mod uploads;

pub use scheduler::{
    CleanupSchedules, Schedule, SchedulerSpawn, Schedulers, maybe_spawn, spawn_schedulers,
};

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

/// Runs every cleanup task in sequence against `db` + `storage`. Used
/// by `POST /management/cleanup/trigger` (an on-demand pass — does
/// not affect the background schedulers).
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

/// Spawns one detached `cleanup:storage-locations` pass.
///
/// Used by the management cache-entry DELETE handlers (issue #71) —
/// the route returns immediately while the orphan sweep runs in the
/// background, mirroring upstream's
/// `event.waitUntil(runTask('cleanup:storage-locations'))` at
/// `lib/api/cache-entries.ts:142,164`.
///
/// Honours `DISABLE_CLEANUP_JOBS`: if cleanup is disabled, the spawn
/// returns a future that resolves to `0` immediately without touching
/// the DB or storage. Upstream's `runTask` flow short-circuits the
/// same way (`tasks/cleanup/storage-locations.ts:13`) — an operator
/// who has explicitly opted out of cleanup shouldn't see surprise
/// background work running on every management delete.
///
/// The spawned future swallows its own errors: per-row failures are
/// already logged at `warn` inside `locations::run`, and the
/// `JoinHandle` is dropped at the call site so a panic during the
/// sweep is reported by tokio's default panic hook but doesn't surface
/// to the HTTP client.
///
/// Returns the `JoinHandle` rather than `()` so tests that need
/// determinism can await the sweep; production call sites discard it.
pub fn spawn_locations_sweep(state: &crate::state::AppState) -> JoinHandle<u64> {
    if state.config.disable_cleanup_jobs {
        tracing::debug!("DISABLE_CLEANUP_JOBS=true - on-demand sweep is a no-op",);
        return tokio::spawn(async { 0 });
    }
    let db = state.db.clone();
    let storage = state.storage.clone();
    tokio::spawn(async move {
        let deleted = locations::run(&*db, &*storage).await;
        tracing::info!(
            deleted,
            "on-demand cleanup:storage-locations sweep complete",
        );
        deleted
    })
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
