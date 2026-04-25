//! `cleanup:uploads` — port of upstream `tasks/cleanup/uploads.ts`.
//!
//! Walks `uploads` rows where `createdAt < cutoff` AND
//! (`lastPartUploadedAt` IS NULL OR `< cutoff`). Per row, opens a
//! transaction, re-checks the predicate via
//! [`DbTx::delete_upload_if_stale`](crate::db::DbTx::delete_upload_if_stale),
//! and on a successful delete removes the per-upload folder from
//! storage. The re-check guards a race with `complete_upload`, which
//! between SELECT and DELETE may have promoted the row by deleting it
//! and handing `folderName` off to a fresh `storage_locations` row.

use crate::db::Db;
use crate::db::entities::Upload;
use crate::storage::{StorageAdapter, StorageError};

/// Pagination matches upstream's `itemsPerPage = 10`.
pub(super) const PAGE_SIZE: i64 = 10;

/// 1-minute staleness threshold matches upstream
/// `tasks/cleanup/uploads.ts:16` ("we can be fairly aggressive in
/// cleaning up abandoned uploads"). Note: the background scheduler
/// runs hourly (issue #18) rather than every 5 min like upstream, so
/// in practice an abandoned upload survives up to ~1 h before this
/// threshold even gets evaluated. Documented divergence — see the
/// PR's parity-notes section.
pub(super) const STALENESS_MS: i64 = 60_000;

/// Runs one cleanup pass. Returns the number of `uploads` rows
/// successfully deleted.
///
/// # Pagination
///
/// `offset` is the count of rows in this pass that we **failed** to
/// delete (storage error, etc.). Successful deletes shrink the table,
/// so the next SELECT at the same offset returns the rows that were
/// previously past the page. Failed rows stay at the head of the
/// result set, so we skip past them. A full page where every row
/// failed breaks the loop to avoid an infinite retry loop within one
/// scheduler tick — the next tick re-tries from `offset = 0`.
///
/// Diverges from upstream `tasks/cleanup/uploads.ts` (which bumps
/// `offset` by a constant `itemsPerPage` per page and therefore
/// processes at most `itemsPerPage` rows per tick); the upstream
/// behaviour is observably a bug given our hourly cadence.
pub(super) async fn run(db: &dyn Db, storage: &dyn StorageAdapter, now_ms: i64) -> u64 {
    let cutoff = now_ms.saturating_sub(STALENESS_MS);
    let page_size_usize = usize::try_from(PAGE_SIZE).unwrap_or(0);
    let mut deleted = 0_u64;
    let mut offset = 0_i64;
    loop {
        let page = match db.find_stale_uploads(cutoff, PAGE_SIZE, offset).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "cleanup:uploads: find_stale_uploads failed");
                return deleted;
            }
        };
        let page_len = page.len();
        let mut iter_failures = 0_i64;
        for upload in page {
            match delete_one(db, storage, &upload, cutoff).await {
                Ok(true) => deleted += 1,
                Ok(false) => {
                    // Race lost: row was promoted between SELECT and DELETE.
                    // The next SELECT will skip it because it no longer
                    // matches the staleness predicate.
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        upload_id = upload.id,
                        folder = %upload.folder_name,
                        "cleanup:uploads: per-row delete failed",
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
            // Full page of failures: avoid infinite retry within this tick.
            break;
        }
    }
    deleted
}

async fn delete_one(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    upload: &Upload,
    cutoff: i64,
) -> Result<bool, UploadCleanupError> {
    let mut tx = db.begin().await?;
    let deleted = tx.delete_upload_if_stale(upload.id, cutoff).await?;
    if !deleted {
        let _ = tx.rollback().await;
        return Ok(false);
    }
    if let Err(e) = storage.delete_folder(&upload.folder_name).await {
        // Roll back the DB delete so a future run retries the same row;
        // matches upstream's auto-rollback-on-throw behaviour.
        let _ = tx.rollback().await;
        return Err(UploadCleanupError::Storage(e));
    }
    tx.commit().await?;
    Ok(true)
}

#[derive(Debug, thiserror::Error)]
enum UploadCleanupError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::test_utils::FakeStorage;
    use super::{PAGE_SIZE, STALENESS_MS, run};
    use crate::db::entities::{CacheEntryCoord, NewUpload};
    use crate::db::id::new_upload_id;
    use crate::db::{Db, SqliteDb};

    async fn fresh_db() -> SqliteDb {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db
    }

    async fn seed_upload(db: &SqliteDb, key: &str, folder: &str, created_at_ms: i64) -> i64 {
        let id = new_upload_id();
        db.create_upload(NewUpload {
            id,
            coord: CacheEntryCoord {
                key,
                version: "v",
                scope: "test",
                repo_id: "r",
            },
            folder_name: folder,
            created_at_ms,
        })
        .await
        .unwrap();
        id
    }

    #[tokio::test]
    async fn stale_upload_is_deleted_and_folder_removed() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let id = seed_upload(&db, "k1", "fldr1", 0).await;
        let now = STALENESS_MS + 1;

        let deleted = run(&db, &storage, now).await;

        assert_eq!(deleted, 1);
        assert!(db.find_upload_by_id(id).await.unwrap().is_none());
        assert_eq!(storage.deleted_folders(), vec!["fldr1".to_string()]);
    }

    #[tokio::test]
    async fn fresh_upload_is_preserved() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let id = seed_upload(&db, "k1", "fldr1", STALENESS_MS).await;
        let now = STALENESS_MS;

        let deleted = run(&db, &storage, now).await;

        assert_eq!(deleted, 0);
        assert!(db.find_upload_by_id(id).await.unwrap().is_some());
        assert!(storage.deleted_folders().is_empty());
    }

    #[tokio::test]
    async fn upload_with_recent_part_is_preserved() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let id = seed_upload(&db, "k1", "fldr1", 0).await;
        // Stale createdAt, but a recent part flips lastPartUploadedAt past cutoff.
        db.increment_upload_finished(id, STALENESS_MS + 1)
            .await
            .unwrap();
        let now = STALENESS_MS + 100;

        let deleted = run(&db, &storage, now).await;

        assert_eq!(
            deleted, 0,
            "row with recent lastPartUploadedAt must NOT be deleted"
        );
        assert!(db.find_upload_by_id(id).await.unwrap().is_some());
        assert!(storage.deleted_folders().is_empty());
    }

    #[tokio::test]
    async fn pagination_spans_multiple_pages() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let row_count = usize::try_from(PAGE_SIZE).unwrap() + 5;
        for i in 0..row_count {
            seed_upload(&db, &format!("k{i}"), &format!("fldr{i}"), 0).await;
        }
        let now = STALENESS_MS + 1;

        let deleted = run(&db, &storage, now).await;

        assert_eq!(deleted, row_count as u64);
        assert_eq!(storage.deleted_folders().len(), row_count);
    }

    #[tokio::test]
    async fn storage_failure_rolls_back_db_delete() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let id = seed_upload(&db, "k1", "fldr1", 0).await;
        storage.fail_next_delete();
        let now = STALENESS_MS + 1;

        let deleted = run(&db, &storage, now).await;

        assert_eq!(deleted, 0, "storage failure must surface as a no-delete");
        assert!(
            db.find_upload_by_id(id).await.unwrap().is_some(),
            "DB delete must roll back when storage fails"
        );
    }

    /// Asserts the `Ok(false)` branch of `delete_one` (race lost — the
    /// row was promoted between the SELECT and the in-tx DELETE
    /// re-check). The cleanup task must NOT bump `iter_failures` for
    /// this case because the row no longer matches the predicate and
    /// the next SELECT skips it naturally; bumping would silently
    /// strand other stale rows behind a false-positive offset.
    #[tokio::test]
    async fn race_lost_row_is_not_counted_as_failure() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        // Seed 11 rows. One starts stale; we promote it to fresh
        // *after* the SELECT but the tx-level DELETE re-check sees
        // fresh and returns false. The other 10 should still be deleted
        // in the same `run` invocation.
        let race_id = seed_upload(&db, "k-race", "fldr-race", 0).await;
        let mut other_ids = Vec::new();
        for i in 0..10 {
            other_ids.push(seed_upload(&db, &format!("k{i}"), &format!("fldr{i}"), 0).await);
        }
        // Promote the race row before run() — it's already in the DB
        // SELECT, but the in-tx re-check will see the promoted state
        // and refuse to delete.
        db.increment_upload_finished(race_id, STALENESS_MS + 1)
            .await
            .unwrap();
        let now = STALENESS_MS + 100;

        let deleted = run(&db, &storage, now).await;

        // 10 successes, 1 race-lost. The race-lost row stays.
        assert_eq!(deleted, 10);
        assert!(
            db.find_upload_by_id(race_id).await.unwrap().is_some(),
            "race-lost row must be preserved",
        );
        for id in other_ids {
            assert!(
                db.find_upload_by_id(id).await.unwrap().is_none(),
                "stale rows must all be deleted in a single pass",
            );
        }
    }

    /// Mixed success/failure: a single row whose folder delete fails
    /// stays at the head of the DB after rollback. With more than
    /// `PAGE_SIZE` rows queued, `offset` must advance past the failure
    /// so subsequent iterations don't refetch the same row forever.
    #[tokio::test]
    async fn mixed_success_failure_pagination_advances_offset() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        let total = usize::try_from(PAGE_SIZE).unwrap() + 3;
        let mut ids = Vec::new();
        for i in 0..total {
            ids.push(seed_upload(&db, &format!("k{i}"), &format!("fldr{i}"), 0).await);
        }
        // First storage delete fails; the rest succeed.
        storage.fail_next_delete();
        let now = STALENESS_MS + 1;

        let deleted = run(&db, &storage, now).await;

        assert_eq!(
            deleted,
            (total - 1) as u64,
            "all but the failing row must be deleted",
        );
        // Exactly one row survives; we don't know which (no ORDER BY),
        // but the surviving row count is the precise check.
        let mut survivors = 0_usize;
        for id in ids {
            if db.find_upload_by_id(id).await.unwrap().is_some() {
                survivors += 1;
            }
        }
        assert_eq!(survivors, 1, "exactly one row should remain");
    }

    #[tokio::test]
    async fn empty_db_run_is_noop() {
        let db = fresh_db().await;
        let storage = FakeStorage::new();
        assert_eq!(run(&db, &storage, STALENESS_MS + 1).await, 0);
        assert!(storage.deleted_folders().is_empty());
    }
}
