//! `cleanup:parts` — port of upstream `tasks/cleanup/parts.ts`.
//!
//! Walks `storage_locations` rows where the merge has completed
//! (`mergedAt IS NOT NULL`) but the per-part folder hasn't been
//! reaped (`partsDeletedAt IS NULL`). Per row, opens a transaction,
//! marks `partsDeletedAt`, deletes `<folderName>/parts` from
//! storage, and commits. Storage failure rolls back so the
//! `parts_deleted_at = SOME ⇒ parts gone` invariant holds —
//! same pattern as `finalize_merge` in `src/merge.rs`.

use crate::db::Db;
use crate::db::entities::StorageLocation;
use crate::storage::{StorageAdapter, StorageError};

pub(super) const PAGE_SIZE: i64 = 10;

/// Runs one cleanup pass. Returns the sum of `partCount` across the
/// rows we successfully reaped — matches upstream's
/// `tasks/cleanup/parts.ts:40` (`deletedCount += location.partCount`).
///
/// `now_ms` is the timestamp written to each row's `partsDeletedAt` —
/// taking it as an argument (rather than reading the wall clock inside
/// `delete_one`) is the injection seam the task-level tests use to
/// assert exact column values.
///
/// `offset` paging works the same way as `uploads::run`: bumped only
/// by failures within the iteration, so successful reaps (which flip
/// `partsDeletedAt` and therefore drop the row from the next SELECT)
/// don't cause us to skip past rows that were previously beyond the
/// page edge. See [`super::uploads::run`] for the rationale.
pub(super) async fn run(db: &dyn Db, storage: &dyn StorageAdapter, now_ms: i64) -> u64 {
    let page_size_usize = usize::try_from(PAGE_SIZE).unwrap_or(0);
    let mut deleted = 0_u64;
    let mut offset = 0_i64;
    loop {
        let page = match db.find_merged_with_parts(PAGE_SIZE, offset).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "cleanup:parts: find_merged_with_parts failed");
                return deleted;
            }
        };
        let page_len = page.len();
        let mut iter_failures = 0_i64;
        for location in page {
            match delete_one(db, storage, &location, now_ms).await {
                Ok(()) => {
                    let pc = u64::try_from(location.part_count).unwrap_or(0);
                    deleted = deleted.saturating_add(pc);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        location_id = %location.id,
                        folder = %location.folder_name,
                        "cleanup:parts: per-row reap failed",
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
    now_ms: i64,
) -> Result<(), PartsCleanupError> {
    let mut tx = db.begin().await?;
    tx.mark_parts_deleted(&location.id, now_ms).await?;
    let parts_folder = format!("{}/parts", location.folder_name);
    if let Err(e) = storage.delete_folder(&parts_folder).await {
        // Roll back so the partsDeletedAt update never lands without
        // the folder actually being gone.
        let _ = tx.rollback().await;
        return Err(PartsCleanupError::Storage(e));
    }
    tx.commit().await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum PartsCleanupError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::test_utils::FakeStorage;
    use super::run;
    use crate::db::entities::CacheEntryCoord;
    use crate::db::{Db, SqliteDb};

    async fn fresh_db() -> SqliteDb {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db
    }

    /// Inserts a `storage_locations` row with a `cache_entries` row
    /// pointing at it (so `find_location_for_entry` works for read-back).
    /// `part_count` is what `run` reports back.
    async fn seed_location(
        db: &SqliteDb,
        loc: &str,
        folder: &str,
        scope: &str,
        part_count: i64,
    ) -> String {
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(loc, folder, part_count)
            .await
            .unwrap();
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

    async fn mark_merged(db: &SqliteDb, loc: &str) {
        assert!(db.try_mark_merge_started(loc, 100).await.unwrap());
        db.mark_merged(loc, 200).await.unwrap();
    }

    const NOW: i64 = 12_345_678;

    #[tokio::test]
    async fn merged_row_with_parts_gets_parts_deleted() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let entry_id = seed_location(&db, "loc1", "fldrA", "scn1", 3).await;
        mark_merged(&db, "loc1").await;

        let part_count = run(&db, &storage, NOW).await;

        assert_eq!(part_count, 3, "should report sum of partCount");
        assert_eq!(storage.deleted_folders(), vec!["fldrA/parts".to_string()]);
        let row = db
            .find_location_for_entry(&entry_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.parts_deleted_at,
            Some(NOW),
            "partsDeletedAt must equal the injected clock reading",
        );
    }

    #[tokio::test]
    async fn already_parts_deleted_row_is_ignored() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let entry_id = seed_location(&db, "loc1", "fldrA", "scn1", 3).await;
        mark_merged(&db, "loc1").await;
        let mut tx = db.begin().await.unwrap();
        tx.mark_parts_deleted("loc1", 300).await.unwrap();
        tx.commit().await.unwrap();

        let part_count = run(&db, &storage, NOW).await;

        assert_eq!(part_count, 0);
        assert!(storage.deleted_folders().is_empty());
        let row = db
            .find_location_for_entry(&entry_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.parts_deleted_at,
            Some(300),
            "previously committed value must be preserved"
        );
    }

    #[tokio::test]
    async fn not_yet_merged_row_is_ignored() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        // Idle location — no merge ever started.
        seed_location(&db, "loc-idle", "fldrI", "scn-idle", 1).await;
        // Claimed but not merged.
        seed_location(&db, "loc-claimed", "fldrC", "scn-claimed", 1).await;
        assert!(db.try_mark_merge_started("loc-claimed", 100).await.unwrap());

        let part_count = run(&db, &storage, NOW).await;

        assert_eq!(part_count, 0);
        assert!(storage.deleted_folders().is_empty());
    }

    #[tokio::test]
    async fn folder_delete_failure_rolls_back_parts_deleted_at() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let entry_id = seed_location(&db, "loc1", "fldrA", "scn1", 5).await;
        mark_merged(&db, "loc1").await;
        storage.fail_next_delete();

        let part_count = run(&db, &storage, NOW).await;

        assert_eq!(part_count, 0, "no rows should report success");
        let row = db
            .find_location_for_entry(&entry_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            row.parts_deleted_at.is_none(),
            "rollback must leave partsDeletedAt NULL when storage fails",
        );
    }

    #[tokio::test]
    async fn empty_db_run_is_noop() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        assert_eq!(run(&db, &storage, NOW).await, 0);
        assert!(storage.deleted_folders().is_empty());
    }
}
