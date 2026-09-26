//! `cleanup:cache-entries` — port of upstream
//! `tasks/cleanup/cache-entries.ts`.
//!
//! Walks `storage_locations` rows whose `lastDownloadedAt` is older
//! than the operator-configured cutoff
//! (`CACHE_CLEANUP_OLDER_THAN_DAYS`). Per row, opens a transaction,
//! deletes the location row (`cache_entries` cascade via the FK
//! `ON DELETE CASCADE` enforced at runtime — see `src/db/sqlite.rs`),
//! commits, then deletes the folder from storage. Storage failure
//! after commit is logged but does **not** retry — same trade-off
//! upstream makes.
//!
//! NULL `lastDownloadedAt` is **not** treated as expired: the SQL
//! `<` operator returns unknown for NULL, which matches upstream's
//! Kysely query and means never-downloaded entries are never reaped
//! by that pass.
//!
//! Opt-in second pass (not in upstream): with
//! `CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS` set, never-downloaded entries
//! committed before that window are reaped too — write-isolated PR
//! caches and superseded keys are often saved and never restored, and
//! would otherwise live forever. Unset, behaviour is upstream's.

use crate::db::Db;
use crate::db::entities::StorageLocation;
use crate::storage::{StorageAdapter, StorageError};

pub(super) const PAGE_SIZE: i64 = 10;

/// Computes the cutoff timestamp for the given retention window.
///
/// `i64::from(u32::MAX) * 86_400_000` is ~3.7×10^17, well under
/// `i64::MAX` (~9.2×10^18), so the multiplication cannot overflow for
/// any `u32`. The subtraction is `saturating_sub` to clamp to
/// `i64::MIN` if `now_ms` ever sits at the bottom of the i64 range
/// (never in practice — `now_ms()` rejects pre-epoch clocks).
pub(super) fn cutoff_ms(now_ms: i64, older_than_days: u32) -> i64 {
    let window_ms = i64::from(older_than_days) * 86_400_000;
    now_ms.saturating_sub(window_ms)
}

/// Retention windows for `cleanup:cache-entries`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryRetention {
    /// `CACHE_CLEANUP_OLDER_THAN_DAYS`: reap entries last downloaded
    /// before this many days ago.
    pub older_than_days: u32,
    /// `CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS`: if set, also reap
    /// never-downloaded entries committed before this many days ago.
    pub unused_older_than_days: Option<u32>,
}

impl EntryRetention {
    pub const fn from_config(config: &crate::config::AppConfig) -> Self {
        Self {
            older_than_days: config.cache_cleanup_older_than_days,
            unused_older_than_days: config.cache_cleanup_unused_older_than_days,
        }
    }
}

/// Which rows a pass walks.
#[derive(Debug, Clone, Copy)]
enum Selector {
    /// `lastDownloadedAt < cutoff` (upstream).
    DownloadedBefore(i64),
    /// `lastDownloadedAt IS NULL` and committed before `cutoff`.
    NeverDownloadedCommittedBefore(i64),
}

impl Selector {
    async fn page(self, db: &dyn Db, offset: i64) -> Result<Vec<StorageLocation>, sqlx::Error> {
        match self {
            Self::DownloadedBefore(c) => db.find_expired_locations(c, PAGE_SIZE, offset).await,
            Self::NeverDownloadedCommittedBefore(c) => {
                db.find_unused_locations(c, PAGE_SIZE, offset).await
            }
        }
    }
}

/// Runs one cleanup pass (plus the opt-in unused pass). Returns the
/// number of `storage_locations` rows successfully deleted.
pub(super) async fn run(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    now_ms: i64,
    retention: EntryRetention,
) -> u64 {
    let expired = run_pass(
        db,
        storage,
        Selector::DownloadedBefore(cutoff_ms(now_ms, retention.older_than_days)),
    )
    .await;
    let Some(days) = retention.unused_older_than_days else {
        return expired;
    };
    let unused = run_pass(
        db,
        storage,
        Selector::NeverDownloadedCommittedBefore(cutoff_ms(now_ms, days)),
    )
    .await;
    if unused > 0 {
        tracing::info!(
            count = unused,
            unused_older_than_days = days,
            "cleanup:entries: reaped never-downloaded entries",
        );
    }
    expired + unused
}

async fn run_pass(db: &dyn Db, storage: &dyn StorageAdapter, selector: Selector) -> u64 {
    let page_size_usize = usize::try_from(PAGE_SIZE).unwrap_or(0);
    let mut deleted = 0_u64;
    let mut offset = 0_i64;
    loop {
        let page = match selector.page(db, offset).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, ?selector, "cleanup:entries: page query failed");
                return deleted;
            }
        };
        let page_len = page.len();
        let mut iter_failures = 0_i64;
        for location in page {
            match delete_one(db, storage, &location).await {
                Ok(()) => deleted += 1,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        location_id = %location.id,
                        folder = %location.folder_name,
                        "cleanup:entries: per-row delete failed",
                    );
                    iter_failures += 1;
                }
            }
        }
        offset = offset.saturating_add(iter_failures);
        if page_len < page_size_usize {
            break;
        }
        if iter_failures == i64::try_from(page_len).unwrap_or(i64::MAX) {
            break;
        }
    }
    deleted
}

async fn delete_one(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    location: &StorageLocation,
) -> Result<(), EntriesCleanupError> {
    let mut tx = db.begin().await?;
    tx.delete_storage_location(&location.id).await?;
    tx.commit().await?;
    storage.delete_folder(&location.folder_name).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum EntriesCleanupError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::test_utils::FakeStorage;
    use super::{EntryRetention, cutoff_ms, run};

    const fn days(older_than_days: u32) -> EntryRetention {
        EntryRetention {
            older_than_days,
            unused_older_than_days: None,
        }
    }
    use crate::db::entities::CacheEntryCoord;
    use crate::db::{Db, SqliteDb};

    async fn fresh_db() -> SqliteDb {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db
    }

    async fn seed_location(db: &SqliteDb, loc: &str, folder: &str, scope: &str) -> String {
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(loc, folder, 1).await.unwrap();
        let entry_id = format!("entry-{loc}");
        tx.seed_cache_entry(
            &entry_id,
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
        entry_id
    }

    #[tokio::test]
    async fn cutoff_arithmetic_matches_milliseconds_per_day() {
        // 90 days = 7_776_000_000 ms; with a 1_000_000_000_000 ms clock
        // the cutoff lands at 1_000_000_000_000 - 7_776_000_000 = 992_224_000_000.
        assert_eq!(
            cutoff_ms(1_000_000_000_000, 90),
            992_224_000_000,
            "cutoff = now_ms - days * 86_400_000"
        );
    }

    #[tokio::test]
    async fn cutoff_arithmetic_saturates_to_min_under_huge_window() {
        // The product fits in i64, but `0 - huge_window` saturates
        // rather than wrapping; the resulting cutoff is just very
        // negative — no SELECT row will match `lastDownloadedAt < cutoff`.
        let result = cutoff_ms(0, u32::MAX);
        assert!(result < 0, "saturating result should be negative");
    }

    #[tokio::test]
    async fn expired_entry_deleted_and_cache_entries_cascade() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let entry_id = seed_location(&db, "loc-old", "folder-old", "scn-old").await;
        // 2 days old, retention 1 day → expired.
        db.touch_location_downloaded("loc-old", 0).await.unwrap();
        let now = 2 * 86_400_000;

        let deleted = run(&db, &storage, now, days(1)).await;

        assert_eq!(deleted, 1);
        assert_eq!(storage.deleted_folders(), vec!["folder-old".to_string()]);
        assert!(
            db.find_location_for_entry(&entry_id)
                .await
                .unwrap()
                .is_none(),
            "FK CASCADE must remove the cache_entries row when the storage_location is deleted",
        );
    }

    #[tokio::test]
    async fn non_expired_entry_preserved() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let entry_id = seed_location(&db, "loc-fresh", "folder-fresh", "scn-fresh").await;
        // Touched today, retention 90 days → not expired.
        let now = 90 * 86_400_000;
        db.touch_location_downloaded("loc-fresh", now)
            .await
            .unwrap();

        let deleted = run(&db, &storage, now, days(90)).await;

        assert_eq!(deleted, 0);
        assert!(storage.deleted_folders().is_empty());
        assert!(
            db.find_location_for_entry(&entry_id)
                .await
                .unwrap()
                .is_some(),
        );
    }

    #[tokio::test]
    async fn run_with_huge_retention_window_is_noop() {
        // Operator-supplied `older_than_days = u32::MAX` saturates the
        // cutoff to the bottom of i64 — no row's `lastDownloadedAt`
        // can be less than that, so `run` is a noop. Pin this so a
        // future change that switched to wrapping arithmetic would be
        // caught by the test rather than by an emptied cache.
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let entry_id = seed_location(&db, "loc-x", "folder-x", "scn-x").await;
        db.touch_location_downloaded("loc-x", 0).await.unwrap();

        let deleted = run(&db, &storage, 0, days(u32::MAX)).await;

        assert_eq!(deleted, 0);
        assert!(storage.deleted_folders().is_empty());
        assert!(
            db.find_location_for_entry(&entry_id)
                .await
                .unwrap()
                .is_some(),
        );
    }

    #[tokio::test]
    async fn empty_db_run_is_noop() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        assert_eq!(run(&db, &storage, 1_000_000_000_000, days(90)).await, 0);
        assert!(storage.deleted_folders().is_empty());
    }

    #[tokio::test]
    async fn null_last_downloaded_at_is_preserved_upstream_parity() {
        // Match upstream `tasks/cleanup/cache-entries.ts:25` —
        // `where('lastDownloadedAt', '<', xDaysAgo)` excludes NULL.
        // Never-downloaded entries are never reaped by this task.
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let entry_id = seed_location(&db, "loc-null", "folder-null", "scn-null").await;
        // No touch_location_downloaded — lastDownloadedAt stays NULL.
        let now = 1_000 * 86_400_000;

        let deleted = run(&db, &storage, now, days(1)).await;

        assert_eq!(deleted, 0);
        assert!(storage.deleted_folders().is_empty());
        assert!(
            db.find_location_for_entry(&entry_id)
                .await
                .unwrap()
                .is_some(),
            "row with NULL lastDownloadedAt must survive (upstream parity)",
        );
    }

    // ---- opt-in never-downloaded pass (CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS)

    const DAY_MS: i64 = 86_400_000;

    fn unused(older_than_days: u32, unused_days: u32) -> EntryRetention {
        EntryRetention {
            older_than_days,
            unused_older_than_days: Some(unused_days),
        }
    }

    /// Seeds a location + entry committed at `committed_at`.
    async fn seed_committed(db: &SqliteDb, loc: &str, committed_at: i64) -> String {
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(loc, &format!("folder-{loc}"), 1)
            .await
            .unwrap();
        let entry_id = format!("entry-{loc}");
        let coord = CacheEntryCoord {
            key: loc,
            version: "v",
            scope: "scn-unused",
            repo_id: "r",
        };
        tx.seed_cache_entry(&entry_id, coord, committed_at, loc)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        entry_id
    }

    #[tokio::test]
    async fn unused_pass_reaps_old_never_downloaded_and_keeps_the_rest() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let now = 1_000 * DAY_MS;
        let old_unused = seed_committed(&db, "old-unused", now - 8 * DAY_MS).await;
        let fresh_unused = seed_committed(&db, "fresh-unused", now - 6 * DAY_MS).await;
        // Committed long ago but downloaded recently: governed by the
        // 30-day last-download window, not the unused one.
        let old_used = seed_committed(&db, "old-used", now - 20 * DAY_MS).await;
        db.touch_location_downloaded("old-used", now - DAY_MS)
            .await
            .unwrap();

        let deleted = run(&db, &storage, now, unused(30, 7)).await;

        assert_eq!(deleted, 1);
        assert_eq!(
            storage.deleted_folders(),
            vec!["folder-old-unused".to_string()]
        );
        let present = |id: String| {
            let db = &db;
            async move { db.find_location_for_entry(&id).await.unwrap().is_some() }
        };
        assert!(!present(old_unused).await);
        assert!(present(fresh_unused).await);
        assert!(present(old_used).await);
    }

    #[tokio::test]
    async fn unused_pass_is_off_when_unset() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let now = 1_000 * DAY_MS;
        seed_committed(&db, "ancient", 0).await;
        assert_eq!(run(&db, &storage, now, days(30)).await, 0);
        assert!(storage.deleted_folders().is_empty());
    }

    #[tokio::test]
    async fn both_passes_count_into_one_total() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let now = 1_000 * DAY_MS;
        seed_committed(&db, "unused", now - 10 * DAY_MS).await;
        seed_committed(&db, "stale", now - 100 * DAY_MS).await;
        db.touch_location_downloaded("stale", now - 40 * DAY_MS)
            .await
            .unwrap();
        assert_eq!(run(&db, &storage, now, unused(30, 7)).await, 2);
    }
}
