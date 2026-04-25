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
//! by this task. Document divergence (none).

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

/// Runs one cleanup pass. Returns the number of `storage_locations`
/// rows successfully deleted.
pub(super) async fn run(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    now_ms: i64,
    older_than_days: u32,
) -> u64 {
    let cutoff = cutoff_ms(now_ms, older_than_days);
    let page_size_usize = usize::try_from(PAGE_SIZE).unwrap_or(0);
    let mut deleted = 0_u64;
    let mut offset = 0_i64;
    loop {
        let page = match db.find_expired_locations(cutoff, PAGE_SIZE, offset).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "cleanup:entries: find_expired_locations failed");
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
    use super::{cutoff_ms, run};
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

        let deleted = run(&db, &storage, now, 1).await;

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

        let deleted = run(&db, &storage, now, 90).await;

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

        let deleted = run(&db, &storage, 0, u32::MAX).await;

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
        assert_eq!(run(&db, &storage, 1_000_000_000_000, 90).await, 0);
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

        let deleted = run(&db, &storage, now, 1).await;

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
}
