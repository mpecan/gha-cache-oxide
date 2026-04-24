//! Transaction-level query helpers.
//!
//! Extracted from `queries.rs` to keep each file under the 700-line
//! hard limit. These helpers compose inside a caller-supplied
//! [`DbTx`] so the finalize transaction in `cache::complete_upload`
//! can sequence inserts + deletes + upserts atomically.
//!
//! Each helper dispatches on [`DbTx`] the same way `impl Db` methods
//! dispatch on [`Db`]: adjacent `SQLite` / Postgres arms with the
//! dialect-specific SQL string the only difference.

use crate::db::DbTx;
use crate::db::entities::{CacheEntryCoord, PreviousLocation};
use crate::db::id::new_uuid;

// ------------------------------------------------------------------------
// Transaction-level helpers
// ------------------------------------------------------------------------

/// Inserts a new `storage_locations` row into an existing transaction.
/// All timestamp columns start NULL — they're populated by later steps
/// (merge / parts-delete / download).
///
/// # Errors
/// Returns `sqlx::Error` on insert failure.
pub async fn insert_storage_location_tx(
    tx: &mut DbTx<'_>,
    id: &str,
    folder_name: &str,
    part_count: i64,
) -> Result<(), sqlx::Error> {
    match tx {
        DbTx::Sqlite(t) => {
            sqlx::query(
                "INSERT INTO storage_locations (id, folderName, partCount, mergeStartedAt, mergedAt, partsDeletedAt, lastDownloadedAt) \
                 VALUES (?, ?, ?, NULL, NULL, NULL, NULL)",
            )
            .bind(id)
            .bind(folder_name)
            .bind(part_count)
            .execute(&mut **t)
            .await?;
        }
        DbTx::Postgres(t) => {
            sqlx::query(
                "INSERT INTO storage_locations (id, \"folderName\", \"partCount\", \"mergeStartedAt\", \"mergedAt\", \"partsDeletedAt\", \"lastDownloadedAt\") \
                 VALUES ($1, $2, $3, NULL, NULL, NULL, NULL)",
            )
            .bind(id)
            .bind(folder_name)
            .bind(part_count)
            .execute(&mut **t)
            .await?;
        }
    }
    Ok(())
}

/// Upserts the cache entry for the given coordinates inside a transaction.
///
/// - If a row matching `(key, version, scope, repoId)` exists, its
///   `locationId` is repointed at `new_location_id`, `updatedAt` is set
///   to `now_ms`, and the **previous** location's `(id, folderName)`
///   is returned to the caller. The caller — typically the finalize
///   handler — **must**, inside the same transaction, delete the old
///   `storage_locations` row and call
///   `adapter.delete_folder(previous.folder_name)`.
/// - If no row exists, a new `cache_entries` row is inserted pointing
///   at `new_location_id`, and `None` is returned.
///
/// The row id for new inserts is generated internally via `new_uuid()`.
///
/// # Errors
/// Returns `sqlx::Error` on any SQL failure.
pub async fn upsert_cache_entry_tx(
    tx: &mut DbTx<'_>,
    coord: CacheEntryCoord<'_>,
    new_location_id: &str,
    now_ms: i64,
) -> Result<Option<PreviousLocation>, sqlx::Error> {
    if let Some((entry_id, old_location_id, old_folder_name)) =
        upsert_find_existing(tx, coord).await?
    {
        upsert_update_existing(tx, now_ms, new_location_id, &entry_id).await?;
        Ok(Some(PreviousLocation {
            id: old_location_id,
            folder_name: old_folder_name,
        }))
    } else {
        upsert_insert_new(tx, coord, new_location_id, now_ms).await?;
        Ok(None)
    }
}

async fn upsert_find_existing(
    tx: &mut DbTx<'_>,
    coord: CacheEntryCoord<'_>,
) -> Result<Option<(String, String, String)>, sqlx::Error> {
    match tx {
        DbTx::Sqlite(t) => {
            sqlx::query_as(
                "SELECT ce.id, ce.locationId, sl.folderName \
             FROM cache_entries ce \
             INNER JOIN storage_locations sl ON sl.id = ce.locationId \
             WHERE ce.key = ? AND ce.version = ? AND ce.scope = ? AND ce.repoId = ?",
            )
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .fetch_optional(&mut **t)
            .await
        }
        DbTx::Postgres(t) => {
            sqlx::query_as(
                "SELECT ce.id, ce.\"locationId\", sl.\"folderName\" \
             FROM cache_entries ce \
             INNER JOIN storage_locations sl ON sl.id = ce.\"locationId\" \
             WHERE ce.key = $1 AND ce.version = $2 AND ce.scope = $3 AND ce.\"repoId\" = $4",
            )
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .fetch_optional(&mut **t)
            .await
        }
    }
}

async fn upsert_update_existing(
    tx: &mut DbTx<'_>,
    now_ms: i64,
    new_location_id: &str,
    entry_id: &str,
) -> Result<(), sqlx::Error> {
    match tx {
        DbTx::Sqlite(t) => {
            sqlx::query("UPDATE cache_entries SET updatedAt = ?, locationId = ? WHERE id = ?")
                .bind(now_ms)
                .bind(new_location_id)
                .bind(entry_id)
                .execute(&mut **t)
                .await?;
        }
        DbTx::Postgres(t) => {
            sqlx::query(
                "UPDATE cache_entries SET \"updatedAt\" = $1, \"locationId\" = $2 WHERE id = $3",
            )
            .bind(now_ms)
            .bind(new_location_id)
            .bind(entry_id)
            .execute(&mut **t)
            .await?;
        }
    }
    Ok(())
}

async fn upsert_insert_new(
    tx: &mut DbTx<'_>,
    coord: CacheEntryCoord<'_>,
    new_location_id: &str,
    now_ms: i64,
) -> Result<(), sqlx::Error> {
    let new_entry_id = new_uuid();
    match tx {
        DbTx::Sqlite(t) => {
            sqlx::query(
                "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&new_entry_id)
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .bind(now_ms)
            .bind(new_location_id)
            .execute(&mut **t)
            .await?;
        }
        DbTx::Postgres(t) => {
            sqlx::query(
                "INSERT INTO cache_entries (id, key, version, scope, \"repoId\", \"updatedAt\", \"locationId\") \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(&new_entry_id)
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .bind(now_ms)
            .bind(new_location_id)
            .execute(&mut **t)
            .await?;
        }
    }
    Ok(())
}

/// Deletes a `storage_locations` row by id inside the caller's
/// transaction. Used by `cache::complete_upload::commit_upload_tx`
/// to drop the superseded location when overwriting an existing
/// cache entry.
///
/// # Errors
/// Returns `sqlx::Error` on delete failure.
pub async fn delete_storage_location_tx(tx: &mut DbTx<'_>, id: &str) -> Result<(), sqlx::Error> {
    match tx {
        DbTx::Sqlite(t) => {
            sqlx::query("DELETE FROM storage_locations WHERE id = ?")
                .bind(id)
                .execute(&mut **t)
                .await?;
        }
        DbTx::Postgres(t) => {
            sqlx::query("DELETE FROM storage_locations WHERE id = $1")
                .bind(id)
                .execute(&mut **t)
                .await?;
        }
    }
    Ok(())
}

/// Deletes an `uploads` row by id inside the caller's transaction.
/// Used by `cache::complete_upload::commit_upload_tx` to clean up the
/// driving upload inside the same tx as the cache-entry upsert.
///
/// # Errors
/// Returns `sqlx::Error` on delete failure.
pub async fn delete_upload_tx(tx: &mut DbTx<'_>, id: i64) -> Result<(), sqlx::Error> {
    match tx {
        DbTx::Sqlite(t) => {
            sqlx::query("DELETE FROM uploads WHERE id = ?")
                .bind(id)
                .execute(&mut **t)
                .await?;
        }
        DbTx::Postgres(t) => {
            sqlx::query("DELETE FROM uploads WHERE id = $1")
                .bind(id)
                .execute(&mut **t)
                .await?;
        }
    }
    Ok(())
}

/// Test-only helper: seeds a `cache_entries` row inside the caller's
/// transaction.
///
/// Used by `tests/db_conformance.rs` scenarios and by several
/// `match_cache_entry` unit tests that need to plant entries without
/// routing through `upsert_cache_entry_tx`. Lives here rather than
/// inside the test code so the dialect-specific column quoting tracks
/// the query module itself.
///
/// # Errors
/// Returns `sqlx::Error` on insert failure.
pub async fn seed_cache_entry_tx(
    tx: &mut DbTx<'_>,
    id: &str,
    coord: CacheEntryCoord<'_>,
    updated_at_ms: i64,
    location_id: &str,
) -> Result<(), sqlx::Error> {
    match tx {
        DbTx::Sqlite(t) => {
            sqlx::query(
                "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .bind(updated_at_ms)
            .bind(location_id)
            .execute(&mut **t)
            .await?;
        }
        DbTx::Postgres(t) => {
            sqlx::query(
                "INSERT INTO cache_entries (id, key, version, scope, \"repoId\", \"updatedAt\", \"locationId\") \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(id)
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .bind(updated_at_ms)
            .bind(location_id)
            .execute(&mut **t)
            .await?;
        }
    }
    Ok(())
}
