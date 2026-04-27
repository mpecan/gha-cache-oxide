//! Database access layer for the cache's metadata tables.
//!
//! [`Db`] and [`DbTx`] are traits implemented once per driver
//! ([`SqliteDb`] / [`PostgresDb`] in `sqlite.rs` / `postgres.rs`).
//! A running server picks exactly one driver via configuration, so the
//! enum-dispatch that used to live here produced two match arms on
//! every query for no runtime benefit; the trait split collapses each
//! query to a single pool operation, with the dialect-specific SQL
//! literal next to the pool it runs against.
//!
//! `AppState` stores `Arc<dyn Db>`, and functions in the request path
//! take `&dyn Db` / `&mut dyn DbTx`. Tests that need raw SQL against
//! the `SQLite` pool for setup or read-back reach in via
//! [`Db::as_sqlite_pool`] — it returns `None` for non-`SQLite`
//! implementations.
//!
//! # Postgres column quoting
//!
//! Upstream's Kysely writes mixed-case columns verbatim (`"folderName"`,
//! `"locationId"`, etc.). Postgres folds unquoted identifiers to
//! lowercase, so every camelCase column name is double-quoted in the
//! Postgres SQL strings in `postgres.rs`. A bucket populated by the
//! upstream server remains readable by this adapter and vice-versa.

pub mod entities;
pub mod id;
mod postgres;
mod sqlite;

use async_trait::async_trait;

use crate::db::entities::{
    CacheEntry, CacheEntryCoord, MatchRequest, MatchType, MatchedEntry, NewUpload,
    PreviousLocation, StorageLocation, Upload,
};

pub use postgres::PostgresDb;
pub use sqlite::SqliteDb;

/// Errors produced by the database layer. New variants are added as
/// specific queries start producing their own error kinds.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

/// Scope of a single-query lookup inside [`Db::match_cache_entry`].
/// Grouped so the per-driver `find_entry_by_*` methods stay at three
/// args.
#[derive(Debug, Clone, Copy)]
pub struct ScopeQuery<'a> {
    pub version: &'a str,
    pub scope: &'a str,
    pub repo_id: &'a str,
}

/// Database handle — the only interface the request path sees.
///
/// Implementors live in `sqlite.rs` and `postgres.rs`. The request
/// path never names a concrete type directly: handlers take
/// `&dyn Db`, and [`AppState`](crate::state::AppState) holds
/// `Arc<dyn Db>`.
///
/// # Default methods
///
/// [`Db::match_cache_entry`] is a default method — the walk across
/// scopes and restore keys is dialect-independent; only the two
/// lookup queries ([`find_entry_by_exact_key`](Db::find_entry_by_exact_key)
/// and [`find_entry_by_prefix_key`](Db::find_entry_by_prefix_key))
/// vary per driver.
#[async_trait]
pub trait Db: Send + Sync {
    /// Runs pending migrations from the driver's migration directory.
    /// Idempotent — calling twice is a no-op.
    ///
    /// # Errors
    /// Returns [`DbError::Migrate`] if any migration fails. On failure
    /// the database may be in a partially-migrated state.
    async fn migrate(&self) -> Result<(), DbError>;

    /// Begins a transaction on the underlying pool. The returned handle
    /// is `Box<dyn DbTx + '_>` so the caller never sees driver-specific
    /// types; transaction-scoped helpers live on [`DbTx`].
    ///
    /// # Errors
    /// Returns `sqlx::Error` if the pool cannot issue a new transaction.
    async fn begin(&self) -> Result<Box<dyn DbTx + '_>, sqlx::Error>;

    // ---- uploads -------------------------------------------------------

    /// Inserts a new `uploads` row and returns the fully-populated entity.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on insert or read-back failure.
    async fn create_upload(&self, u: NewUpload<'_>) -> Result<Upload, sqlx::Error>;

    /// Reads a row from `uploads` by primary key.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_upload_by_id(&self, id: i64) -> Result<Option<Upload>, sqlx::Error>;

    /// Reads a row from `uploads` matching the given coordinates.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_upload_by_coord(
        &self,
        coord: CacheEntryCoord<'_>,
    ) -> Result<Option<Upload>, sqlx::Error>;

    /// Increments `startedPartUploadCount` for the given upload by one.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn increment_upload_started(&self, id: i64) -> Result<(), sqlx::Error>;

    /// Increments `finishedPartUploadCount` and sets `lastPartUploadedAt`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn increment_upload_finished(&self, id: i64, now_ms: i64) -> Result<(), sqlx::Error>;

    /// Deletes an upload row.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    async fn delete_upload(&self, id: i64) -> Result<(), sqlx::Error>;

    // ---- storage locations --------------------------------------------

    /// Locates the `storage_locations` row backing the given cache entry.
    /// Returns `None` when the cache entry does not exist.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_location_for_entry(
        &self,
        cache_entry_id: &str,
    ) -> Result<Option<StorageLocation>, sqlx::Error>;

    /// Sets `lastDownloadedAt` on a storage location. Fire-and-forget
    /// from the download handler — a failure here is observability
    /// noise, not a functional bug.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn touch_location_downloaded(
        &self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<(), sqlx::Error>;

    // ---- lazy-merge state (issue #15) ----------------------------------

    /// Atomically claims the lazy-merge for `location_id`. Returns
    /// `true` if the caller is the exclusive winner —
    /// `mergeStartedAt` was NULL, `mergedAt` was NULL, and this call
    /// flipped `mergeStartedAt` to `now_ms`. Returns `false` if
    /// another request already claimed (or completed) the merge.
    ///
    /// # Divergence from upstream
    /// Upstream reads `mergeStartedAt` outside a transaction and races
    /// two concurrent first-downloads into two merges. The CAS here
    /// guarantees exactly one merger per location — required by issue
    /// #15's acceptance criterion.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn try_mark_merge_started(
        &self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<bool, sqlx::Error>;

    /// Records a successful lazy-merge by setting `mergedAt`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn mark_merged(&self, location_id: &str, now_ms: i64) -> Result<(), sqlx::Error>;

    /// Clears both `mergeStartedAt` and `mergedAt` so the next
    /// download retries the merge from scratch. Called by the
    /// background merger task on upload failure. Mirrors upstream
    /// `lib/storage.ts:265-273`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn reset_merge_flags(&self, location_id: &str) -> Result<(), sqlx::Error>;

    /// Clears `mergeStartedAt` on every row where a lazy-merge was
    /// claimed but never finalized and the claim is older than
    /// `cutoff_ms`. Returns the number of rows updated.
    ///
    /// Called once at server startup (issue #17). A process crash
    /// between the CAS claim and `finalize_merge` leaves
    /// `mergeStartedAt` set and `mergedAt` NULL forever — the download
    /// path would then read "another merger in flight" and stream
    /// parts on every request, never re-running the merge. Clearing
    /// the column restores the idle state so the next download wins
    /// the CAS and re-runs the merge; `mergedAt` (already NULL) is
    /// left alone so the schema invariant "both NULL ⇒ idle" holds.
    ///
    /// Also called from the background `cleanup:merges` task (issue
    /// #18) with a tighter 15-minute cutoff. The query body is
    /// driver-specific but the contract is identical.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    async fn clear_stale_merge_claims(&self, cutoff_ms: i64) -> Result<u64, sqlx::Error>;

    // ---- background cleanup finders (issue #18) ------------------------

    /// Returns one page of `uploads` rows considered stale by the
    /// background `cleanup:uploads` task. A row is stale when its
    /// `createdAt` is strictly less than `cutoff_ms` AND
    /// `lastPartUploadedAt` is either NULL or strictly less than
    /// `cutoff_ms`. Mirrors upstream `tasks/cleanup/uploads.ts:23-34`.
    ///
    /// `limit` / `offset` paginate across the table; the caller loops
    /// until a page returns fewer than `limit` rows.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_stale_uploads(
        &self,
        cutoff_ms: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Upload>, sqlx::Error>;

    /// Returns one page of `storage_locations` rows whose
    /// `lastDownloadedAt` is strictly less than `cutoff_ms`. NULL
    /// `lastDownloadedAt` is **not** considered expired (matches
    /// upstream `tasks/cleanup/cache-entries.ts:25` which uses the
    /// SQL `<` operator with three-valued NULL semantics — never-
    /// downloaded entries are never reaped by `cleanup:cache-entries`).
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_expired_locations(
        &self,
        cutoff_ms: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error>;

    /// Returns one page of `storage_locations` rows that no
    /// `cache_entries` row points at — i.e. orphan storage locations.
    /// Mirrors upstream `tasks/cleanup/storage-locations.ts:22-36`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_orphan_locations(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error>;

    /// Returns one page of `storage_locations` rows where the lazy
    /// merge has completed (`mergedAt IS NOT NULL`) but the per-part
    /// folder hasn't yet been reaped (`partsDeletedAt IS NULL`).
    /// Mirrors upstream `tasks/cleanup/parts.ts:21-28`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_merged_with_parts(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error>;

    // ---- management API listings (issue #19) --------------------------

    /// Returns one page of `cache_entries` rows, optionally filtered by
    /// `scope` and/or `repo_id`. `None` filter values disable the
    /// corresponding `WHERE` clause. Ordered by `updatedAt DESC, id`
    /// for newest-first browsing with stable pagination.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn list_cache_entries(
        &self,
        scope: Option<&str>,
        repo_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CacheEntry>, sqlx::Error>;

    /// Counts `cache_entries` rows matching the same optional filters
    /// as [`list_cache_entries`](Self::list_cache_entries).
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn count_cache_entries(
        &self,
        scope: Option<&str>,
        repo_id: Option<&str>,
    ) -> Result<i64, sqlx::Error>;

    /// Returns one page of `storage_locations` rows. Ordered by `id`
    /// for stable pagination (locations have no useful "recency"
    /// column — all timestamps are nullable).
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn list_storage_locations(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<StorageLocation>, sqlx::Error>;

    /// Counts every row in `storage_locations`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn count_storage_locations(&self) -> Result<i64, sqlx::Error>;

    // ---- match_cache_entry (dialect-specific queries, default walk) ----

    /// Fetches the single most-recently-updated `cache_entries` row
    /// with an exact key match for the given scope coordinates.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_entry_by_exact_key(
        &self,
        key: &str,
        q: &ScopeQuery<'_>,
    ) -> Result<Option<CacheEntry>, sqlx::Error>;

    /// Fetches the single most-recently-updated `cache_entries` row
    /// whose key starts with `key`, for the given scope coordinates.
    /// The `LIKE` pattern is properly escaped inside the implementation.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    async fn find_entry_by_prefix_key(
        &self,
        key: &str,
        q: &ScopeQuery<'_>,
    ) -> Result<Option<CacheEntry>, sqlx::Error>;

    /// Looks up a cache entry matching `req` — exact primary, then
    /// prefix primary, then (per scope, if `restore_keys` is
    /// non-empty) each restore key's exact and prefix variants.
    /// Returns the first hit together with a `MatchType`.
    ///
    /// Line-matches upstream `lib/storage.ts#matchCacheEntry`. The walk
    /// is dialect-agnostic; only the two lookup queries differ per
    /// driver.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on any query failure.
    async fn match_cache_entry(
        &self,
        req: MatchRequest<'_>,
    ) -> Result<Option<MatchedEntry>, sqlx::Error> {
        for scope in req.scopes {
            let q = ScopeQuery {
                version: req.version,
                scope,
                repo_id: req.repo_id,
            };

            if let Some(entry) = self.find_entry_by_exact_key(req.primary_key, &q).await? {
                return Ok(Some(MatchedEntry {
                    entry,
                    match_type: MatchType::ExactPrimary,
                }));
            }
            if let Some(entry) = self.find_entry_by_prefix_key(req.primary_key, &q).await? {
                return Ok(Some(MatchedEntry {
                    entry,
                    match_type: MatchType::PrefixedPrimary,
                }));
            }
            if req.restore_keys.is_empty() {
                return Ok(None);
            }
            for rk in req.restore_keys {
                if let Some(entry) = self.find_entry_by_exact_key(rk, &q).await? {
                    return Ok(Some(MatchedEntry {
                        entry,
                        match_type: MatchType::ExactRestore,
                    }));
                }
                if let Some(entry) = self.find_entry_by_prefix_key(rk, &q).await? {
                    return Ok(Some(MatchedEntry {
                        entry,
                        match_type: MatchType::PrefixedRestore,
                    }));
                }
            }
        }
        Ok(None)
    }

    // ---- test escape hatch --------------------------------------------

    /// SQLite-only escape hatch for tests that need to run raw SQL
    /// against the underlying pool. Returns `None` for Postgres.
    ///
    /// Production code should never call this — the trait's method
    /// surface is enough. It exists so SQLite-backed integration tests
    /// (the default `cargo test` path) can assert on schema-level
    /// state (`PRAGMA foreign_key_list`, `COUNT(*)`) without each
    /// test reaching for a driver-agnostic helper.
    #[doc(hidden)]
    fn as_sqlite_pool(&self) -> Option<&sqlx::SqlitePool> {
        None
    }
}

/// In-flight transaction handle. Driver-specific implementations live
/// next to their [`Db`] counterparts (`SqliteTx` / `PostgresTx`).
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

    /// Deletes an `uploads` row by id inside this transaction.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    async fn delete_upload(&mut self, id: i64) -> Result<(), sqlx::Error>;

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

/// Escapes `%`, `_` and `\` for a SQL `LIKE ... ESCAPE '\'` pattern.
///
/// Mirrors upstream `escapeLikePattern` in `lib/storage.ts`. Order
/// matters: the backslash must be doubled first, otherwise the escapes
/// we add for `%` and `_` would themselves be doubled.
pub(crate) fn escape_like_pattern(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_in_memory_and_migrate_is_idempotent() {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db.migrate().await.unwrap();
    }

    #[tokio::test]
    async fn migrations_produce_expected_tables() {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        let pool = db.as_sqlite_pool().expect("sqlite pool exposed");
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
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        let pool = db.as_sqlite_pool().expect("sqlite pool exposed");
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
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        let pool = db.as_sqlite_pool().expect("sqlite pool exposed");
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
