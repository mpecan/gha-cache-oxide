//! [`DbTx`], the transaction half of the database layer, split from
//! `mod.rs` to keep each file under the length limit.

use async_trait::async_trait;

use crate::db::entities::{CacheEntryCoord, PreviousLocation};

/// In-flight transaction handle. Driver-specific implementations live
/// in `sqlite_tx.rs` / `postgres_tx.rs` / `mysql_tx.rs`.
///
/// Consuming `commit` / `rollback` is modelled with `self: Box<Self>`
/// so a `Box<dyn DbTx>` can still commit once; the transaction-scoped
/// helper methods take `&mut self`.
#[async_trait]
pub trait DbTx: Send {
    /// Commits the underlying driver transaction.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on commit failure.
    async fn commit(self: Box<Self>) -> Result<(), sqlx::Error>;

    /// Rolls the underlying driver transaction back, discarding writes.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on rollback failure.
    async fn rollback(self: Box<Self>) -> Result<(), sqlx::Error>;

    /// Inserts a new `storage_locations` row into this transaction.
    /// All timestamp columns start NULL — they're populated by later
    /// steps (merge / parts-delete / download).
    ///
    /// # Errors
    /// Returns `sqlx::Error` on insert failure.
    async fn insert_storage_location(
        &mut self,
        id: &str,
        folder_name: &str,
        part_count: i64,
    ) -> Result<(), sqlx::Error>;

    /// Upserts the cache entry for the given coordinates inside this
    /// transaction.
    ///
    /// - If a row matching `(key, version, scope, repoId)` exists,
    ///   its `locationId` is repointed at `new_location_id`,
    ///   `updatedAt` is set to `now_ms`, and the **previous**
    ///   location's `(id, folderName)` is returned to the caller.
    /// - If no row exists, a new `cache_entries` row is inserted
    ///   pointing at `new_location_id`, and `None` is returned.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on any SQL failure.
    async fn upsert_cache_entry(
        &mut self,
        coord: CacheEntryCoord<'_>,
        new_location_id: &str,
        now_ms: i64,
    ) -> Result<Option<PreviousLocation>, sqlx::Error>;

    /// Deletes a `storage_locations` row by id inside this transaction.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    async fn delete_storage_location(&mut self, id: &str) -> Result<(), sqlx::Error>;

    /// Deletes a `storage_locations` row only if it is still unused:
    /// never downloaded and no merge *in flight* (`mergeStartedAt` unset,
    /// or the merge already finished — the post-commit background merge
    /// leaves every entry merged, and `mark_merged` does not clear
    /// `mergeStartedAt`). Returns `true` when it deleted. The
    /// unused-entries cleanup pass uses this so a first restore (and the
    /// lazy merge it starts) landing between its page query and the
    /// delete wins.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    async fn delete_location_if_unused(&mut self, id: &str) -> Result<bool, sqlx::Error>;

    /// Deletes an `uploads` row by id inside this transaction. Returns
    /// `true` when a row was removed; commit paths treat `false` as
    /// "someone else already consumed this upload" and roll back.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    async fn delete_upload(&mut self, id: i64) -> Result<bool, sqlx::Error>;

    /// Deletes an `uploads` row only if the staleness predicate the
    /// background `cleanup:uploads` task uses still holds at delete
    /// time — `createdAt < cutoff_ms` AND
    /// (`lastPartUploadedAt IS NULL` OR `lastPartUploadedAt < cutoff_ms`).
    /// Returns `true` iff the row was actually deleted.
    ///
    /// Re-checking the predicate inside the transaction prevents a
    /// race with `completeUpload`, which between the cleanup SELECT
    /// and DELETE may have deleted the row and handed `folderName`
    /// off to a fresh `storage_locations` row. Without the re-check,
    /// the cleanup task would also wipe the parts the new
    /// `cache_entries` row points at. Mirrors upstream
    /// `tasks/cleanup/uploads.ts:45-57` (which exists for the same
    /// reason — see the upstream comment).
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    async fn delete_upload_if_stale(
        &mut self,
        id: i64,
        cutoff_ms: i64,
    ) -> Result<bool, sqlx::Error>;

    /// Sets `partsDeletedAt` on a `storage_locations` row inside this
    /// transaction. The lazy-merge finisher calls this inside a
    /// transaction that also invokes `adapter.delete_folder`; if the
    /// folder delete fails, the caller rolls back so the DB invariant
    /// (`parts_deleted_at` set ⇒ parts gone) holds.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn mark_parts_deleted(
        &mut self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<(), sqlx::Error>;

    /// Test-only helper: seeds a `cache_entries` row inside this
    /// transaction. Used by the DB conformance suite and a handful of
    /// `match_cache_entry` unit tests that plant entries without
    /// routing through [`upsert_cache_entry`](DbTx::upsert_cache_entry).
    ///
    /// # Errors
    /// Returns `sqlx::Error` on insert failure.
    async fn seed_cache_entry(
        &mut self,
        id: &str,
        coord: CacheEntryCoord<'_>,
        updated_at_ms: i64,
        location_id: &str,
    ) -> Result<(), sqlx::Error>;
}
