//! Database access layer for the cache's metadata tables.
//!
//! [`Db`] is an enum over the supported driver pools (`SqlitePool`,
//! `PgPool`). Public query methods on `Db` match on the variant and
//! dispatch to a generic helper that takes `impl Executor<'_, Database
//! = DB>` — so the bind chain is written once; only the dialect-
//! specific SQL-string literal differs between arms.
//!
//! Transactions are held in [`DbTx`], a parallel enum over
//! `Transaction<'_, Sqlite>` / `Transaction<'_, Postgres>`. The
//! module-level helpers `insert_storage_location_tx` and
//! `upsert_cache_entry_tx` take `&mut DbTx<'_>` and dispatch the same
//! way.
//!
//! # Why an enum rather than `sqlx::Any`?
//!
//! sqlx's `Any` pool passes query strings through verbatim to the
//! underlying driver — it doesn't rewrite `?` into `$N`, so single-
//! source queries aren't possible anyway. The enum keeps the two
//! dialects' quirks visible at the call site (the SQL literals next
//! to each other) while sharing every bind and every decode through
//! the driver-generic helper. Documented in the #13 conformance-
//! suite doc that this was the project's chosen trade-off.

pub mod entities;
pub mod id;
pub mod queries;
pub mod tx;

use std::path::Path;

use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::{Postgres, Sqlite, Transaction};

/// Upstream pins `max_connections: 10` for Postgres. Match it so self-
/// hostable deployments sitting behind upstream's helm chart or compose
/// file behave identically.
const POSTGRES_MAX_CONNECTIONS: u32 = 10;

/// Errors produced by the database layer. New variants are added as
/// specific queries start producing their own error kinds.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

/// Handle to the application's database connection pool. Cheap to clone —
/// the underlying `Pool` is an `Arc`.
#[derive(Debug, Clone)]
pub enum Db {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

/// In-flight transaction handle, parallel to [`Db`]. Returned by
/// [`Db::begin`] and accepted by the module-level `*_tx` helpers.
pub enum DbTx<'a> {
    Sqlite(Transaction<'a, Sqlite>),
    Postgres(Transaction<'a, Postgres>),
}

impl<'a> DbTx<'a> {
    /// Commits the underlying driver transaction.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on commit failure.
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        match self {
            Self::Sqlite(tx) => tx.commit().await,
            Self::Postgres(tx) => tx.commit().await,
        }
    }

    /// Rolls the underlying driver transaction back, discarding writes.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on rollback failure.
    pub async fn rollback(self) -> Result<(), sqlx::Error> {
        match self {
            Self::Sqlite(tx) => tx.rollback().await,
            Self::Postgres(tx) => tx.rollback().await,
        }
    }

    /// SQLite-only escape hatch for tests that need to run driver-
    /// specific raw SQL inside the same transaction as the dispatched
    /// helpers. Returns `None` on Postgres-backed variants.
    ///
    /// Same rationale as `Db::sqlite_pool`: keep production code on
    /// the dispatched API; SQLite-hardcoded tests can reach in.
    #[doc(hidden)]
    #[must_use]
    pub const fn sqlite_tx(&mut self) -> Option<&mut Transaction<'a, Sqlite>> {
        match self {
            Self::Sqlite(tx) => Some(tx),
            Self::Postgres(_) => None,
        }
    }
}

impl Db {
    /// Opens (or creates) a `SQLite` database at the given path.
    ///
    /// Enables `foreign_keys` on every connection so the
    /// `ON DELETE CASCADE` on `cache_entries.locationId` is honoured.
    ///
    /// # Errors
    /// Returns `DbError::Sqlx` if the pool cannot be created (invalid
    /// path, permission denied, disk full, etc.).
    pub async fn connect_sqlite(path: &Path) -> Result<Self, DbError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            // Disable sqlx's log-at-info-or-above query logging; tower-http's
            // TraceLayer already captures request-scoped observability.
            .disable_statement_logging();
        let pool = SqlitePoolOptions::new().connect_with(opts).await?;
        Ok(Self::Sqlite(pool))
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
        Ok(Self::Sqlite(pool))
    }

    /// Connects to Postgres with `max_connections = 10` (match upstream).
    ///
    /// # Errors
    /// Returns `DbError::Sqlx` if the URL is malformed or the pool
    /// cannot connect (network, credentials, missing database).
    pub async fn connect_postgres(url: &str) -> Result<Self, DbError> {
        let opts: PgConnectOptions = url.parse::<PgConnectOptions>()?.disable_statement_logging();
        let pool = PgPoolOptions::new()
            .max_connections(POSTGRES_MAX_CONNECTIONS)
            .connect_with(opts)
            .await?;
        Ok(Self::Postgres(pool))
    }

    /// Runs pending migrations from the driver's migration directory.
    /// Idempotent — calling twice is a no-op.
    ///
    /// # Errors
    /// Returns `DbError::Migrate` if any migration fails. On failure
    /// the database may be in a partially-migrated state; operators
    /// should not restart against the same file expecting auto-recovery.
    pub async fn migrate(&self) -> Result<(), DbError> {
        match self {
            Self::Sqlite(pool) => sqlx::migrate!("./migrations/sqlite").run(pool).await?,
            Self::Postgres(pool) => sqlx::migrate!("./migrations/postgres").run(pool).await?,
        }
        Ok(())
    }

    /// Begins a transaction on the underlying pool.
    ///
    /// # Errors
    /// Returns `sqlx::Error` if the pool cannot issue a new transaction
    /// (exhausted connections, closed pool, etc.).
    pub async fn begin(&self) -> Result<DbTx<'_>, sqlx::Error> {
        match self {
            Self::Sqlite(pool) => Ok(DbTx::Sqlite(pool.begin().await?)),
            Self::Postgres(pool) => Ok(DbTx::Postgres(pool.begin().await?)),
        }
    }

    /// SQLite-only escape hatch for tests that need to run raw
    /// `PRAGMA` queries or `SELECT`-style assertions against the
    /// underlying pool. Returns `None` on Postgres-backed variants.
    ///
    /// This is `pub` rather than `pub(crate)` so SQLite-hardcoded
    /// integration tests (`tests/blob.rs`, `tests/twirp_download.rs`,
    /// etc.) can reach in without going through a driver-agnostic
    /// helper for every schema-level check they do. Production code
    /// should use the dispatched `Db::*` methods and the `*_tx`
    /// helpers; reaching for `sqlite_pool` at the request path would
    /// defeat the Postgres driver wholesale.
    #[doc(hidden)]
    #[must_use]
    pub const fn sqlite_pool(&self) -> Option<&SqlitePool> {
        match self {
            Self::Sqlite(pool) => Some(pool),
            Self::Postgres(_) => None,
        }
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

        let Db::Sqlite(pool) = &db else {
            panic!("connect_in_memory must return Sqlite variant");
        };
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_sqlx%' ESCAPE '\\' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .fetch_all(pool)
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

        let Db::Sqlite(pool) = &db else {
            panic!("connect_in_memory must return Sqlite variant");
        };
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT \"table\", \"from\", \"on_delete\" FROM pragma_foreign_key_list('cache_entries')",
        )
        .fetch_all(pool)
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

        let Db::Sqlite(pool) = &db else {
            panic!("connect_in_memory must return Sqlite variant");
        };
        let result = sqlx::query(
            "INSERT INTO cache_entries (id, key, version, updatedAt, locationId, scope, repoId)
             VALUES ('e1', 'k', 'v', 0, 'missing-location', 's', 'r')",
        )
        .execute(pool)
        .await;

        let err = result.expect_err("FK violation should fail the insert");
        assert!(
            format!("{err}").to_lowercase().contains("foreign key"),
            "expected FK error, got: {err}"
        );
    }
}
