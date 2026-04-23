//! Database access layer for the cache's metadata tables.
//!
//! Wraps a single `SqlitePool` behind the [`Db`] struct. Query helpers
//! live in [`queries`] and operate against the pool (or a supplied
//! transaction for multi-step compositions). Entities are in [`entities`].

pub mod entities;
pub mod id;
pub mod queries;

use std::path::Path;

use sqlx::ConnectOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

/// Errors produced by the database layer. New variants are added as
/// specific queries start producing their own error kinds.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

/// Handle to the application's database connection pool. Cheap to clone —
/// the inner `SqlitePool` is an `Arc`.
#[derive(Debug, Clone)]
pub struct Db {
    pool: SqlitePool,
}

impl Db {
    /// Opens (or creates) a `SQLite` database at the given path.
    ///
    /// Enables `foreign_keys` on every connection so the
    /// `ON DELETE CASCADE` on `cache_entries.locationId` is honoured.
    ///
    /// # Errors
    /// Returns `DbError::Sqlx` if the pool cannot be created (invalid path,
    /// permission denied, disk full, etc.).
    pub async fn connect_sqlite(path: &Path) -> Result<Self, DbError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            // Disable sqlx's log-at-info-or-above query logging; tower-http's
            // TraceLayer already captures request-scoped observability.
            .disable_statement_logging();
        let pool = SqlitePoolOptions::new().connect_with(opts).await?;
        Ok(Self { pool })
    }

    /// Opens an in-memory `SQLite` database. Primarily for unit tests.
    ///
    /// # Errors
    /// Returns `DbError::Sqlx` on pool creation failure.
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

    /// Runs pending migrations against the pool. Idempotent.
    ///
    /// # Errors
    /// Returns `DbError::Migrate` if any migration fails. On failure the
    /// database may be in a partially-migrated state; operators should not
    /// restart against the same file expecting auto-recovery.
    pub async fn migrate(&self) -> Result<(), DbError> {
        sqlx::migrate!("./migrations/sqlite")
            .run(&self.pool)
            .await?;
        Ok(())
    }

    /// Access to the underlying pool, for query helpers and test harnesses.
    /// Gated on `#[cfg(test)]` because only tests reach for the raw pool;
    /// production code goes through typed query methods.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_in_memory_and_migrate_is_idempotent() {
        let db = Db::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db.migrate().await.unwrap(); // no-op second time
    }

    #[tokio::test]
    async fn migrations_produce_expected_tables() {
        let db = Db::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();

        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_sqlx%' ESCAPE '\\' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();

        assert_eq!(
            tables,
            vec!["cache_entries", "storage_locations", "uploads"]
        );
    }

    #[tokio::test]
    async fn cache_entries_foreign_key_is_on_delete_cascade() {
        let db = Db::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();

        // PRAGMA foreign_key_list returns one row per FK. We expect one on
        // cache_entries pointing at storage_locations with ON DELETE CASCADE.
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT \"table\", \"from\", \"on_delete\" FROM pragma_foreign_key_list('cache_entries')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();

        assert_eq!(rows.len(), 1, "expected exactly one FK on cache_entries");
        let (table, from, on_delete) = &rows[0];
        assert_eq!(table, "storage_locations");
        assert_eq!(from, "locationId");
        assert_eq!(on_delete, "CASCADE");
    }

    #[tokio::test]
    async fn foreign_keys_are_enforced_at_runtime() {
        let db = Db::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();

        // Inserting a cache_entries row with an unknown locationId must fail.
        let result = sqlx::query(
            "INSERT INTO cache_entries (id, key, version, updatedAt, locationId, scope, repoId)
             VALUES ('e1', 'k', 'v', 0, 'missing-location', 's', 'r')",
        )
        .execute(db.pool())
        .await;

        let err = result.expect_err("FK violation should fail the insert");
        assert!(
            format!("{err}").to_lowercase().contains("foreign key"),
            "expected FK error, got: {err}"
        );
    }
}
