//! CRUD query helpers over the three cache metadata tables.
//!
//! Each helper is a one-query operation (or a near-one-query one that
//! composes inside a caller-supplied transaction). We deliberately avoid
//! the `sqlx::query_as!` / `sqlx::query!` macros here so the crate builds
//! on a fresh clone without an offline-mode sqlx cache to keep in sync;
//! all queries are exercised by unit tests against an in-memory `SQLite`
//! which catches schema drift just as effectively.
//!
//! Time stamps (`now_ms`) are passed in explicitly so tests can use fixed
//! clocks rather than mocking out the system clock.

use sqlx::{Sqlite, Transaction};

use super::Db;
use super::entities::{CacheEntryCoord, NewUpload, PreviousLocation, StorageLocation, Upload};

// -- Uploads --------------------------------------------------------------

impl Db {
    /// Inserts a new `uploads` row and returns the fully-populated entity.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on insert or read-back failure.
    pub async fn create_upload(&self, upload: NewUpload<'_>) -> Result<Upload, sqlx::Error> {
        sqlx::query(
            "INSERT INTO uploads (id, key, version, scope, repoId, createdAt, folderName) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(upload.id)
        .bind(upload.coord.key)
        .bind(upload.coord.version)
        .bind(upload.coord.scope)
        .bind(upload.coord.repo_id)
        .bind(upload.created_at_ms)
        .bind(upload.folder_name)
        .execute(&self.pool)
        .await?;

        self.find_upload_by_id(upload.id)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    /// Reads a row from `uploads` by primary key.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    pub async fn find_upload_by_id(&self, id: i64) -> Result<Option<Upload>, sqlx::Error> {
        sqlx::query_as::<_, Upload>("SELECT * FROM uploads WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    /// Reads a row from `uploads` matching the given coordinates. Used by
    /// the reserve endpoint to detect in-flight uploads for the same
    /// `(key, version, scope, repoId)` tuple.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    pub async fn find_upload_by_coord(
        &self,
        coord: CacheEntryCoord<'_>,
    ) -> Result<Option<Upload>, sqlx::Error> {
        sqlx::query_as::<_, Upload>(
            "SELECT * FROM uploads WHERE key = ? AND version = ? AND scope = ? AND repoId = ?",
        )
        .bind(coord.key)
        .bind(coord.version)
        .bind(coord.scope)
        .bind(coord.repo_id)
        .fetch_optional(&self.pool)
        .await
    }

    /// Increments `startedPartUploadCount` for the given upload by one.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    pub async fn increment_upload_started(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE uploads SET startedPartUploadCount = startedPartUploadCount + 1 WHERE id = ?",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Increments `finishedPartUploadCount` and sets `lastPartUploadedAt`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    pub async fn increment_upload_finished(&self, id: i64, now_ms: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE uploads \
             SET finishedPartUploadCount = finishedPartUploadCount + 1, lastPartUploadedAt = ? \
             WHERE id = ?",
        )
        .bind(now_ms)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Deletes an upload row.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    pub async fn delete_upload(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM uploads WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

// -- Storage locations ----------------------------------------------------

impl Db {
    /// Locates the `storage_locations` row backing the given cache entry.
    /// Returns `None` when the cache entry does not exist.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    pub async fn find_location_for_entry(
        &self,
        cache_entry_id: &str,
    ) -> Result<Option<StorageLocation>, sqlx::Error> {
        sqlx::query_as::<_, StorageLocation>(
            "SELECT sl.* FROM storage_locations sl \
             INNER JOIN cache_entries ce ON ce.locationId = sl.id \
             WHERE ce.id = ?",
        )
        .bind(cache_entry_id)
        .fetch_optional(&self.pool)
        .await
    }

    /// Sets `lastDownloadedAt` on a storage location. Intended as
    /// fire-and-forget from the download handler — a failure here is
    /// observability noise, not a functional bug.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    pub async fn touch_location_downloaded(
        &self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE storage_locations SET lastDownloadedAt = ? WHERE id = ?")
            .bind(now_ms)
            .bind(location_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// Inserts a new `storage_locations` row into an existing transaction.
/// All timestamp columns start NULL — they're populated by later steps
/// (merge / parts-delete / download).
///
/// # Errors
/// Returns `sqlx::Error` on insert failure.
pub async fn insert_storage_location_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
    folder_name: &str,
    part_count: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO storage_locations (id, folderName, partCount, mergeStartedAt, mergedAt, partsDeletedAt, lastDownloadedAt) \
         VALUES (?, ?, ?, NULL, NULL, NULL, NULL)",
    )
    .bind(id)
    .bind(folder_name)
    .bind(part_count)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// -- Cache entries --------------------------------------------------------

/// Upserts the cache entry for the given coordinates inside a transaction.
///
/// - If a row matching `(key, version, scope, repoId)` exists, its
///   `locationId` is repointed at `new_location_id`, `updatedAt` is set to
///   `now_ms`, and the **previous** location's `(id, folderName)` is
///   returned to the caller. The caller — typically the finalize handler
///   (#8) — **must**, inside the same transaction, (1) `DELETE FROM
///   storage_locations WHERE id = previous.id` so the orphan metadata
///   row does not leak, and (2) call
///   `adapter.delete_folder(previous.folder_name)` to reclaim the blob
///   bytes. This helper intentionally does neither so the DB layer stays
///   decoupled from the storage adapter. Forgetting step 1 leaks DB rows
///   indefinitely (nothing in #18 cleanup is scoped to find them).
/// - If no row exists, a new `cache_entries` row is inserted pointing at
///   `new_location_id`, and `None` is returned.
///
/// The row id for new inserts is generated internally via `new_uuid()`.
///
/// # Errors
/// Returns `sqlx::Error` on any SQL failure.
pub async fn upsert_cache_entry_tx(
    tx: &mut Transaction<'_, Sqlite>,
    coord: CacheEntryCoord<'_>,
    new_location_id: &str,
    now_ms: i64,
) -> Result<Option<PreviousLocation>, sqlx::Error> {
    let existing: Option<(String, String, String)> = sqlx::query_as(
        "SELECT ce.id, ce.locationId, sl.folderName \
         FROM cache_entries ce \
         INNER JOIN storage_locations sl ON sl.id = ce.locationId \
         WHERE ce.key = ? AND ce.version = ? AND ce.scope = ? AND ce.repoId = ?",
    )
    .bind(coord.key)
    .bind(coord.version)
    .bind(coord.scope)
    .bind(coord.repo_id)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some((entry_id, old_location_id, old_folder_name)) = existing {
        sqlx::query("UPDATE cache_entries SET updatedAt = ?, locationId = ? WHERE id = ?")
            .bind(now_ms)
            .bind(new_location_id)
            .bind(&entry_id)
            .execute(&mut **tx)
            .await?;
        Ok(Some(PreviousLocation {
            id: old_location_id,
            folder_name: old_folder_name,
        }))
    } else {
        sqlx::query(
            "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(super::id::new_uuid())
        .bind(coord.key)
        .bind(coord.version)
        .bind(coord.scope)
        .bind(coord.repo_id)
        .bind(now_ms)
        .bind(new_location_id)
        .execute(&mut **tx)
        .await?;
        Ok(None)
    }
}

#[cfg(test)]
#[path = "queries_tests.rs"]
mod tests;
