//! `cleanup:storage-locations` — port of upstream
//! `tasks/cleanup/storage-locations.ts`.
//!
//! Walks `storage_locations` rows that no `cache_entries` row points
//! at — orphans left behind by overwrites or imported data. Per row,
//! deletes the location row in a transaction and then removes the
//! folder from storage.

use crate::db::Db;
use crate::db::entities::StorageLocation;
use crate::storage::{StorageAdapter, StorageError};

pub(super) const PAGE_SIZE: i64 = 10;

/// Runs one cleanup pass. Returns the number of orphan
/// `storage_locations` rows successfully deleted.
pub(super) async fn run(db: &dyn Db, storage: &dyn StorageAdapter) -> u64 {
    let page_size_usize = usize::try_from(PAGE_SIZE).unwrap_or(0);
    let mut deleted = 0_u64;
    let mut offset = 0_i64;
    loop {
        let page = match db.find_orphan_locations(PAGE_SIZE, offset).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "cleanup:locations: find_orphan_locations failed");
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
                        "cleanup:locations: per-row delete failed",
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
) -> Result<(), LocationsCleanupError> {
    let mut tx = db.begin().await?;
    tx.delete_storage_location(&location.id).await?;
    tx.commit().await?;
    storage.delete_folder(&location.folder_name).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum LocationsCleanupError {
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

    #[tokio::test]
    async fn orphan_location_is_deleted_and_folder_removed() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        // Orphan: no cache_entries row references it.
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location("loc-orphan", "folder-orphan", 1)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let deleted = run(&db, &storage).await;

        assert_eq!(deleted, 1);
        assert_eq!(storage.deleted_folders(), vec!["folder-orphan".to_string()]);
    }

    #[tokio::test]
    async fn empty_db_run_is_noop() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        assert_eq!(run(&db, &storage).await, 0);
        assert!(storage.deleted_folders().is_empty());
    }

    #[tokio::test]
    async fn referenced_location_is_preserved() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location("loc-ref", "folder-ref", 1)
            .await
            .unwrap();
        tx.seed_cache_entry(
            "entry-ref",
            CacheEntryCoord {
                key: "k",
                version: "v",
                scope: "scn-ref",
                repo_id: "r",
            },
            0,
            "loc-ref",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let deleted = run(&db, &storage).await;

        assert_eq!(deleted, 0);
        assert!(storage.deleted_folders().is_empty());
        assert!(
            db.find_location_for_entry("entry-ref")
                .await
                .unwrap()
                .is_some(),
        );
    }
}
