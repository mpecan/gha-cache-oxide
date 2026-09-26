//! Postgres driver: concrete [`Db`] / [`DbTx`] implementations and all
//! Postgres-dialect SQL literals.
//!
//! Split from the trait definitions in `mod.rs` so every query lives
//! next to the pool it runs against; a reviewer comparing drivers
//! diffs this file against `sqlite.rs`.
//!
//! # Column quoting
//!
//! Upstream's Kysely writes mixed-case columns verbatim
//! (`"folderName"`, `"locationId"`, etc.). Postgres folds unquoted
//! identifiers to lowercase, so every camelCase column name is
//! double-quoted below. A bucket populated by the upstream server
//! remains readable by this adapter and vice-versa.

use async_trait::async_trait;
use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{Postgres, Transaction};

use super::entities::{
    CacheEntry, CacheEntryCoord, CacheEntryFilter, MergeState, NewUpload, PreviousLocation,
    StorageLocation, Upload,
};
use super::id::new_uuid;
use super::{Db, DbError, DbTx, ScopeQuery, escape_like_pattern};

/// Upstream pins `max_connections: 10` for Postgres. Match it so self-
/// hostable deployments sitting behind upstream's helm chart or compose
/// file behave identically.
const POSTGRES_MAX_CONNECTIONS: u32 = 10;

/// Postgres-backed [`Db`]. Wraps an `sqlx::PgPool`.
#[derive(Debug, Clone)]
pub struct PostgresDb {
    pool: PgPool,
}

impl PostgresDb {
    /// Connects to Postgres with `max_connections = 10` (match upstream).
    ///
    /// # Errors
    /// Returns [`DbError::Sqlx`] if the URL is malformed or the pool
    /// cannot connect.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        let opts: PgConnectOptions = url.parse::<PgConnectOptions>()?.disable_statement_logging();
        let pool = PgPoolOptions::new()
            .max_connections(POSTGRES_MAX_CONNECTIONS)
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Db for PostgresDb {
    async fn migrate(&self) -> Result<(), DbError> {
        sqlx::migrate!("./migrations/postgres")
            .run(&self.pool)
            .await?;
        Ok(())
    }

    async fn begin(&self) -> Result<Box<dyn DbTx + '_>, sqlx::Error> {
        let tx = self.pool.begin().await?;
        Ok(Box::new(PostgresTx { tx }))
    }

    async fn create_upload(&self, u: NewUpload<'_>) -> Result<Upload, sqlx::Error> {
        sqlx::query(
            "INSERT INTO uploads (id, key, version, scope, \"repoId\", \"createdAt\", \"folderName\") \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
        sqlx::query_as("SELECT * FROM uploads WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn find_upload_by_coord(
        &self,
        coord: CacheEntryCoord<'_>,
    ) -> Result<Option<Upload>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM uploads WHERE key = $1 AND version = $2 AND scope = $3 AND \"repoId\" = $4",
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
            "UPDATE uploads SET \"startedPartUploadCount\" = \"startedPartUploadCount\" + 1 WHERE id = $1",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn increment_upload_finished(&self, id: i64, now_ms: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE uploads \
             SET \"finishedPartUploadCount\" = \"finishedPartUploadCount\" + 1, \
                 \"lastPartUploadedAt\" = $1 \
             WHERE id = $2",
        )
        .bind(now_ms)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn touch_upload(&self, id: i64, now_ms: i64) -> Result<bool, sqlx::Error> {
        let done = sqlx::query("UPDATE uploads SET \"lastPartUploadedAt\" = $1 WHERE id = $2")
            .bind(now_ms)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_upload(&self, id: i64) -> Result<bool, sqlx::Error> {
        let done = sqlx::query("DELETE FROM uploads WHERE id = $1")
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
             INNER JOIN cache_entries ce ON ce.\"locationId\" = sl.id \
             WHERE ce.id = $1",
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
        sqlx::query("UPDATE storage_locations SET \"lastDownloadedAt\" = $1 WHERE id = $2")
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
            "UPDATE storage_locations SET \"mergeStartedAt\" = $1 \
             WHERE id = $2 AND \"mergeStartedAt\" IS NULL AND \"mergedAt\" IS NULL",
        )
        .bind(now_ms)
        .bind(location_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn mark_merged(&self, location_id: &str, now_ms: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE storage_locations SET \"mergedAt\" = $1 WHERE id = $2")
            .bind(now_ms)
            .bind(location_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn reset_merge_flags(&self, location_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE storage_locations SET \"mergeStartedAt\" = NULL, \"mergedAt\" = NULL WHERE id = $1",
        )
        .bind(location_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_merge_state(&self, location_id: &str) -> Result<Option<MergeState>, sqlx::Error> {
        sqlx::query_as::<_, MergeState>(
            "SELECT \"mergeStartedAt\", \"mergedAt\" FROM storage_locations WHERE id = $1",
        )
        .bind(location_id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn clear_stale_merge_claims(&self, cutoff_ms: i64) -> Result<u64, sqlx::Error> {
        let rows = sqlx::query(
            "UPDATE storage_locations SET \"mergeStartedAt\" = NULL \
             WHERE \"mergeStartedAt\" IS NOT NULL \
               AND \"mergedAt\" IS NULL \
               AND \"mergeStartedAt\" < $1",
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
             WHERE \"createdAt\" < $1 \
               AND (\"lastPartUploadedAt\" IS NULL OR \"lastPartUploadedAt\" < $2) \
             ORDER BY id \
             LIMIT $3 OFFSET $4",
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
             WHERE \"lastDownloadedAt\" < $1 \
             ORDER BY id \
             LIMIT $2 OFFSET $3",
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
             JOIN cache_entries ce ON ce.\"locationId\" = sl.id \
             WHERE sl.\"lastDownloadedAt\" IS NULL AND ce.\"updatedAt\" < $1 \
             ORDER BY sl.id \
             LIMIT $2 OFFSET $3",
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
                 SELECT 1 FROM cache_entries ce WHERE ce.\"locationId\" = sl.id \
             ) \
             ORDER BY sl.id \
             LIMIT $1 OFFSET $2",
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
             WHERE \"mergedAt\" IS NOT NULL AND \"partsDeletedAt\" IS NULL \
             ORDER BY id \
             LIMIT $1 OFFSET $2",
        )
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
        // `$N::text IS NULL` makes the type explicit so Postgres can
        // resolve the placeholder; the same parameter is referenced
        // twice so each filter binds its `Option<&str>` exactly once.
        sqlx::query_as(
            "SELECT * FROM cache_entries \
             WHERE ($1::text IS NULL OR scope = $1) \
               AND ($2::text IS NULL OR \"repoId\" = $2) \
             ORDER BY \"updatedAt\" DESC, id \
             LIMIT $3 OFFSET $4",
        )
        .bind(scope)
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
             WHERE ($1::text IS NULL OR scope = $1) \
               AND ($2::text IS NULL OR \"repoId\" = $2)",
        )
        .bind(scope)
        .bind(repo_id)
        .fetch_one(&self.pool)
        .await
    }

    async fn list_storage_locations(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM storage_locations ORDER BY id LIMIT $1 OFFSET $2")
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
        sqlx::query_as("SELECT * FROM cache_entries WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn find_storage_location_by_id(
        &self,
        id: &str,
    ) -> Result<Option<StorageLocation>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM storage_locations WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn delete_cache_entries_by_filter(
        &self,
        filter: CacheEntryFilter<'_>,
    ) -> Result<u64, sqlx::Error> {
        // `$N::text IS NULL` matches the existing `list_cache_entries`
        // pattern: each parameter is referenced once for the
        // disable-filter check and again for the equality predicate.
        // Postgres can re-reference `$N` directly, so we bind each
        // filter exactly once.
        let rows = sqlx::query(
            "DELETE FROM cache_entries \
             WHERE ($1::text IS NULL OR key = $1) \
               AND ($2::text IS NULL OR version = $2) \
               AND ($3::text IS NULL OR scope = $3) \
               AND ($4::text IS NULL OR \"repoId\" = $4)",
        )
        .bind(filter.key)
        .bind(filter.version)
        .bind(filter.scope)
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
             WHERE key = $1 AND version = $2 AND scope = $3 AND \"repoId\" = $4 \
             ORDER BY \"updatedAt\" DESC LIMIT 1",
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
             WHERE key LIKE $1 ESCAPE '\\' AND version = $2 AND scope = $3 AND \"repoId\" = $4 \
             ORDER BY \"updatedAt\" DESC LIMIT 1",
        )
        .bind(&pattern)
        .bind(q.version)
        .bind(q.scope)
        .bind(q.repo_id)
        .fetch_optional(&self.pool)
        .await
    }
}

/// Postgres transaction handle. Wraps `sqlx::Transaction<'_, Postgres>`.
pub struct PostgresTx<'a> {
    tx: Transaction<'a, Postgres>,
}

#[async_trait]
impl DbTx for PostgresTx<'_> {
    async fn commit(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.commit().await
    }

    async fn rollback(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.rollback().await
    }

    async fn insert_storage_location(
        &mut self,
        id: &str,
        folder_name: &str,
        part_count: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO storage_locations (id, \"folderName\", \"partCount\", \"mergeStartedAt\", \"mergedAt\", \"partsDeletedAt\", \"lastDownloadedAt\") \
             VALUES ($1, $2, $3, NULL, NULL, NULL, NULL)",
        )
        .bind(id)
        .bind(folder_name)
        .bind(part_count)
        .execute(&mut *self.tx)
        .await?;
        Ok(())
    }

    async fn upsert_cache_entry(
        &mut self,
        coord: CacheEntryCoord<'_>,
        new_location_id: &str,
        now_ms: i64,
    ) -> Result<Option<PreviousLocation>, sqlx::Error> {
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT ce.id, ce.\"locationId\", sl.\"folderName\" \
             FROM cache_entries ce \
             INNER JOIN storage_locations sl ON sl.id = ce.\"locationId\" \
             WHERE ce.key = $1 AND ce.version = $2 AND ce.scope = $3 AND ce.\"repoId\" = $4",
        )
        .bind(coord.key)
        .bind(coord.version)
        .bind(coord.scope)
        .bind(coord.repo_id)
        .fetch_optional(&mut *self.tx)
        .await?;

        if let Some((entry_id, old_location_id, old_folder_name)) = existing {
            sqlx::query(
                "UPDATE cache_entries SET \"updatedAt\" = $1, \"locationId\" = $2 WHERE id = $3",
            )
            .bind(now_ms)
            .bind(new_location_id)
            .bind(&entry_id)
            .execute(&mut *self.tx)
            .await?;
            Ok(Some(PreviousLocation {
                id: old_location_id,
                folder_name: old_folder_name,
            }))
        } else {
            let new_entry_id = new_uuid();
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
            .execute(&mut *self.tx)
            .await?;
            Ok(None)
        }
    }

    async fn delete_location_if_unused(&mut self, id: &str) -> Result<bool, sqlx::Error> {
        let done = sqlx::query(
            "DELETE FROM storage_locations \
             WHERE id = $1 AND \"lastDownloadedAt\" IS NULL \
             AND (\"mergeStartedAt\" IS NULL OR \"mergedAt\" IS NOT NULL)",
        )
        .bind(id)
        .execute(&mut *self.tx)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_storage_location(&mut self, id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM storage_locations WHERE id = $1")
            .bind(id)
            .execute(&mut *self.tx)
            .await?;
        Ok(())
    }

    async fn delete_upload(&mut self, id: i64) -> Result<bool, sqlx::Error> {
        let done = sqlx::query("DELETE FROM uploads WHERE id = $1")
            .bind(id)
            .execute(&mut *self.tx)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_upload_if_stale(
        &mut self,
        id: i64,
        cutoff_ms: i64,
    ) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query(
            "DELETE FROM uploads \
             WHERE id = $1 \
               AND \"createdAt\" < $2 \
               AND (\"lastPartUploadedAt\" IS NULL OR \"lastPartUploadedAt\" < $3)",
        )
        .bind(id)
        .bind(cutoff_ms)
        .bind(cutoff_ms)
        .execute(&mut *self.tx)
        .await?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn mark_parts_deleted(
        &mut self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE storage_locations SET \"partsDeletedAt\" = $1 WHERE id = $2")
            .bind(now_ms)
            .bind(location_id)
            .execute(&mut *self.tx)
            .await?;
        Ok(())
    }

    async fn seed_cache_entry(
        &mut self,
        id: &str,
        coord: CacheEntryCoord<'_>,
        updated_at_ms: i64,
        location_id: &str,
    ) -> Result<(), sqlx::Error> {
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
        .execute(&mut *self.tx)
        .await?;
        Ok(())
    }
}
