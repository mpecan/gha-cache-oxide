//! `SQLite` driver: concrete [`Db`] / [`DbTx`] implementations and all
//! `SQLite`-dialect SQL literals.
//!
//! Split from the trait definitions in `mod.rs` so every query lives
//! next to the pool it runs against. A reviewer comparing two
//! drivers diffs this file against `postgres.rs`.

use std::path::Path;

use async_trait::async_trait;
use sqlx::ConnectOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use super::entities::{
    CacheEntry, CacheEntryCoord, CacheEntryFilter, MergeState, NewUpload, RepoUsage,
    StorageLocation, Upload,
};
use super::{Db, DbError, DbTx, ScopeQuery, escape_like_pattern};

/// SQLite-backed [`Db`]. Wraps an `sqlx::SqlitePool`.
#[derive(Debug, Clone)]
pub struct SqliteDb {
    pool: SqlitePool,
}

impl SqliteDb {
    /// Opens (or creates) a database at the given path.
    ///
    /// Enables `foreign_keys` on every connection so the
    /// `ON DELETE CASCADE` on `cache_entries.locationId` is honoured.
    ///
    /// # Errors
    /// Returns [`DbError::Sqlx`] if the pool cannot be created.
    pub async fn connect(path: &Path) -> Result<Self, DbError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .disable_statement_logging();
        let pool = SqlitePoolOptions::new().connect_with(opts).await?;
        Ok(Self { pool })
    }

    /// Opens an in-memory database. Primarily for unit tests.
    ///
    /// # Errors
    /// Returns [`DbError::Sqlx`] on pool creation failure.
    pub async fn connect_in_memory() -> Result<Self, DbError> {
        let opts = SqliteConnectOptions::new()
            .in_memory(true)
            .foreign_keys(true)
            .disable_statement_logging();
        // Size 1 so every test operation hits the same underlying memory DB;
        // larger pools would give each connection a private empty schema.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }
}

#[path = "sqlite_tx.rs"]
mod tx;
pub use tx::SqliteTx;

#[async_trait]
impl Db for SqliteDb {
    async fn migrate(&self) -> Result<(), DbError> {
        sqlx::migrate!("./migrations/sqlite")
            .run(&self.pool)
            .await?;
        Ok(())
    }

    async fn begin(&self) -> Result<Box<dyn DbTx + '_>, sqlx::Error> {
        let tx = self.pool.begin().await?;
        Ok(Box::new(SqliteTx { tx }))
    }

    async fn create_upload(&self, u: NewUpload<'_>) -> Result<Upload, sqlx::Error> {
        sqlx::query(
            "INSERT INTO uploads (id, key, version, scope, repoId, createdAt, folderName) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(u.id)
        .bind(u.coord.key)
        .bind(u.coord.version)
        .bind(u.coord.scope)
        .bind(u.coord.repo_id)
        .bind(u.created_at_ms)
        .bind(u.folder_name)
        .execute(&self.pool)
        .await?;

        self.find_upload_by_id(u.id)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    async fn find_upload_by_id(&self, id: i64) -> Result<Option<Upload>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM uploads WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn find_upload_by_coord(
        &self,
        coord: CacheEntryCoord<'_>,
    ) -> Result<Option<Upload>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM uploads WHERE key = ? AND version = ? AND scope = ? AND repoId = ?",
        )
        .bind(coord.key)
        .bind(coord.version)
        .bind(coord.scope)
        .bind(coord.repo_id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn increment_upload_started(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE uploads SET startedPartUploadCount = startedPartUploadCount + 1 WHERE id = ?",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn increment_upload_finished(&self, id: i64, now_ms: i64) -> Result<(), sqlx::Error> {
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

    async fn touch_upload(&self, id: i64, now_ms: i64) -> Result<bool, sqlx::Error> {
        let done = sqlx::query("UPDATE uploads SET lastPartUploadedAt = ? WHERE id = ?")
            .bind(now_ms)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_upload(&self, id: i64) -> Result<bool, sqlx::Error> {
        let done = sqlx::query("DELETE FROM uploads WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn find_location_for_entry(
        &self,
        cache_entry_id: &str,
    ) -> Result<Option<StorageLocation>, sqlx::Error> {
        sqlx::query_as(
            "SELECT sl.* FROM storage_locations sl \
             INNER JOIN cache_entries ce ON ce.locationId = sl.id \
             WHERE ce.id = ?",
        )
        .bind(cache_entry_id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn touch_location_downloaded(
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

    async fn try_mark_merge_started(
        &self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query(
            "UPDATE storage_locations SET mergeStartedAt = ? \
             WHERE id = ? AND mergeStartedAt IS NULL AND mergedAt IS NULL",
        )
        .bind(now_ms)
        .bind(location_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn mark_merged(&self, location_id: &str, now_ms: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE storage_locations SET mergedAt = ? WHERE id = ?")
            .bind(now_ms)
            .bind(location_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn reset_merge_flags(&self, location_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE storage_locations SET mergeStartedAt = NULL, mergedAt = NULL WHERE id = ?",
        )
        .bind(location_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_merge_state(&self, location_id: &str) -> Result<Option<MergeState>, sqlx::Error> {
        sqlx::query_as::<_, MergeState>(
            "SELECT mergeStartedAt, mergedAt FROM storage_locations WHERE id = ?",
        )
        .bind(location_id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn clear_stale_merge_claims(&self, cutoff_ms: i64) -> Result<u64, sqlx::Error> {
        let rows = sqlx::query(
            "UPDATE storage_locations SET mergeStartedAt = NULL \
             WHERE mergeStartedAt IS NOT NULL \
               AND mergedAt IS NULL \
               AND mergeStartedAt < ?",
        )
        .bind(cutoff_ms)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(rows)
    }

    async fn find_stale_uploads(
        &self,
        cutoff_ms: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Upload>, sqlx::Error> {
        // ORDER BY id pins a stable page order across iterations so the
        // `offset = failures-so-far` arithmetic in `tasks::cleanup::uploads`
        // skips past the same row each time.
        sqlx::query_as(
            "SELECT * FROM uploads \
             WHERE createdAt < ? \
               AND (lastPartUploadedAt IS NULL OR lastPartUploadedAt < ?) \
             ORDER BY id \
             LIMIT ? OFFSET ?",
        )
        .bind(cutoff_ms)
        .bind(cutoff_ms)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn find_expired_locations(
        &self,
        cutoff_ms: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM storage_locations \
             WHERE lastDownloadedAt < ? \
             ORDER BY id \
             LIMIT ? OFFSET ?",
        )
        .bind(cutoff_ms)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn find_unused_locations(
        &self,
        cutoff_ms: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as(
            "SELECT sl.* FROM storage_locations sl \
             JOIN cache_entries ce ON ce.locationId = sl.id \
             WHERE sl.lastDownloadedAt IS NULL AND ce.updatedAt < ? \
             ORDER BY sl.id \
             LIMIT ? OFFSET ?",
        )
        .bind(cutoff_ms)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn find_orphan_locations(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as(
            "SELECT sl.* FROM storage_locations sl \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM cache_entries ce WHERE ce.locationId = sl.id \
             ) \
             ORDER BY sl.id \
             LIMIT ? OFFSET ?",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn find_merged_with_parts(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM storage_locations \
             WHERE mergedAt IS NOT NULL AND partsDeletedAt IS NULL \
             ORDER BY id \
             LIMIT ? OFFSET ?",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn find_unsized_locations(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as(
            "SELECT sl.* FROM storage_locations sl \
             WHERE sl.sizeBytes IS NULL AND EXISTS ( \
                 SELECT 1 FROM cache_entries ce WHERE ce.locationId = sl.id \
             ) \
             ORDER BY sl.id \
             LIMIT ? OFFSET ?",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn set_location_size(
        &self,
        location_id: &str,
        size_bytes: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE storage_locations SET sizeBytes = ? WHERE id = ?")
            .bind(size_bytes)
            .bind(location_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn find_repos_over_size(&self, max_bytes: i64) -> Result<Vec<RepoUsage>, sqlx::Error> {
        sqlx::query_as(
            "SELECT ce.repoId AS repoId, \
                    CAST(SUM(COALESCE(sl.sizeBytes, 0)) AS BIGINT) AS totalBytes \
             FROM cache_entries ce \
             JOIN storage_locations sl ON sl.id = ce.locationId \
             GROUP BY ce.repoId \
             HAVING SUM(COALESCE(sl.sizeBytes, 0)) > ? \
             ORDER BY ce.repoId",
        )
        .bind(max_bytes)
        .fetch_all(&self.pool)
        .await
    }

    async fn find_repo_locations_lru(
        &self,
        repo_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as(
            "SELECT sl.* FROM storage_locations sl \
             JOIN cache_entries ce ON ce.locationId = sl.id \
             WHERE ce.repoId = ? \
             ORDER BY MAX(ce.updatedAt, COALESCE(sl.lastDownloadedAt, 0)), sl.id \
             LIMIT ? OFFSET ?",
        )
        .bind(repo_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn list_cache_entries(
        &self,
        scope: Option<&str>,
        repo_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CacheEntry>, sqlx::Error> {
        // The `(? IS NULL OR col = ?)` idiom keeps a single SQL string
        // for all four filter combinations; each Option binds twice
        // (SQLite has no `$N`-style positional reuse like Postgres).
        sqlx::query_as(
            "SELECT * FROM cache_entries \
             WHERE (? IS NULL OR scope = ?) \
               AND (? IS NULL OR repoId = ?) \
             ORDER BY updatedAt DESC, id \
             LIMIT ? OFFSET ?",
        )
        .bind(scope)
        .bind(scope)
        .bind(repo_id)
        .bind(repo_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    async fn count_cache_entries(
        &self,
        scope: Option<&str>,
        repo_id: Option<&str>,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM cache_entries \
             WHERE (? IS NULL OR scope = ?) \
               AND (? IS NULL OR repoId = ?)",
        )
        .bind(scope)
        .bind(scope)
        .bind(repo_id)
        .bind(repo_id)
        .fetch_one(&self.pool)
        .await
    }

    async fn list_storage_locations(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM storage_locations ORDER BY id LIMIT ? OFFSET ?")
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
    }

    async fn count_storage_locations(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COUNT(*) FROM storage_locations")
            .fetch_one(&self.pool)
            .await
    }

    async fn find_cache_entry_by_id(&self, id: &str) -> Result<Option<CacheEntry>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM cache_entries WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn find_storage_location_by_id(
        &self,
        id: &str,
    ) -> Result<Option<StorageLocation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn delete_cache_entries_by_filter(
        &self,
        filter: CacheEntryFilter<'_>,
    ) -> Result<u64, sqlx::Error> {
        // SQLite uses positional `?` placeholders; each `?` consumes
        // the next bound value, so the optional-filter pattern binds
        // each filter twice (once for the IS-NULL check, once for the
        // equality predicate). Same shape as `mysql.rs`'s filter
        // queries.
        let rows = sqlx::query(
            "DELETE FROM cache_entries \
             WHERE (? IS NULL OR key = ?) \
               AND (? IS NULL OR version = ?) \
               AND (? IS NULL OR scope = ?) \
               AND (? IS NULL OR repoId = ?)",
        )
        .bind(filter.key)
        .bind(filter.key)
        .bind(filter.version)
        .bind(filter.version)
        .bind(filter.scope)
        .bind(filter.scope)
        .bind(filter.repo_id)
        .bind(filter.repo_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(rows)
    }

    async fn find_entry_by_exact_key(
        &self,
        key: &str,
        q: &ScopeQuery<'_>,
    ) -> Result<Option<CacheEntry>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM cache_entries \
             WHERE key = ? AND version = ? AND scope = ? AND repoId = ? \
             ORDER BY updatedAt DESC LIMIT 1",
        )
        .bind(key)
        .bind(q.version)
        .bind(q.scope)
        .bind(q.repo_id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn find_entry_by_prefix_key(
        &self,
        key: &str,
        q: &ScopeQuery<'_>,
    ) -> Result<Option<CacheEntry>, sqlx::Error> {
        let pattern = format!("{}%", escape_like_pattern(key));
        sqlx::query_as(
            "SELECT * FROM cache_entries \
             WHERE key LIKE ? ESCAPE '\\' AND version = ? AND scope = ? AND repoId = ? \
             ORDER BY updatedAt DESC LIMIT 1",
        )
        .bind(&pattern)
        .bind(q.version)
        .bind(q.scope)
        .bind(q.repo_id)
        .fetch_optional(&self.pool)
        .await
    }

    fn as_sqlite_pool(&self) -> Option<&SqlitePool> {
        Some(&self.pool)
    }
}

#[cfg(test)]
#[path = "queries_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "match_cache_entry_tests.rs"]
mod match_tests;

#[cfg(test)]
#[path = "merge_state_tests.rs"]
mod merge_state_tests;
