//! CRUD query helpers over the three cache metadata tables.
//!
//! Each `impl Db` method dispatches on [`Db`]'s variant via a `match`,
//! running the dialect-specific SQL against the matching pool. The two
//! arms are always adjacent so a reviewer can diff the `?` vs `$N`
//! placeholder and the camelCase quoting for Postgres at a glance.
//!
//! # Postgres column quoting
//!
//! Upstream's Kysely writes mixed-case columns verbatim (`"folderName"`,
//! `"locationId"`, etc.). Postgres folds unquoted identifiers to
//! lowercase, so we double-quote every camelCase column name in the
//! Postgres SQL strings. A bucket populated by the upstream server
//! remains readable by this adapter and vice-versa.
//!
//! Timestamps (`now_ms`) are passed in explicitly so tests can use
//! fixed clocks rather than mocking out the system clock.

use super::Db;
use super::entities::{
    CacheEntry, CacheEntryCoord, MatchRequest, MatchType, MatchedEntry, NewUpload, StorageLocation,
    Upload,
};

impl Db {
    /// Inserts a new `uploads` row and returns the fully-populated entity.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on insert or read-back failure.
    pub async fn create_upload(&self, u: NewUpload<'_>) -> Result<Upload, sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
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
                .execute(pool)
                .await?;
            }
            Self::Postgres(pool) => {
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
                .execute(pool)
                .await?;
            }
        }

        self.find_upload_by_id(u.id)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    /// Reads a row from `uploads` by primary key.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    pub async fn find_upload_by_id(&self, id: i64) -> Result<Option<Upload>, sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query_as("SELECT * FROM uploads WHERE id = ?")
                    .bind(id)
                    .fetch_optional(pool)
                    .await
            }
            Self::Postgres(pool) => {
                sqlx::query_as("SELECT * FROM uploads WHERE id = $1")
                    .bind(id)
                    .fetch_optional(pool)
                    .await
            }
        }
    }

    /// Reads a row from `uploads` matching the given coordinates.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    pub async fn find_upload_by_coord(
        &self,
        coord: CacheEntryCoord<'_>,
    ) -> Result<Option<Upload>, sqlx::Error> {
        match self {
            Self::Sqlite(pool) => sqlx::query_as(
                "SELECT * FROM uploads WHERE key = ? AND version = ? AND scope = ? AND repoId = ?",
            )
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .fetch_optional(pool)
            .await,
            Self::Postgres(pool) => sqlx::query_as(
                "SELECT * FROM uploads WHERE key = $1 AND version = $2 AND scope = $3 AND \"repoId\" = $4",
            )
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .fetch_optional(pool)
            .await,
        }
    }

    /// Increments `startedPartUploadCount` for the given upload by one.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    pub async fn increment_upload_started(&self, id: i64) -> Result<(), sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE uploads SET startedPartUploadCount = startedPartUploadCount + 1 WHERE id = ?",
                )
                .bind(id)
                .execute(pool)
                .await?;
            }
            Self::Postgres(pool) => {
                sqlx::query(
                    "UPDATE uploads SET \"startedPartUploadCount\" = \"startedPartUploadCount\" + 1 WHERE id = $1",
                )
                .bind(id)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    /// Increments `finishedPartUploadCount` and sets `lastPartUploadedAt`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    pub async fn increment_upload_finished(&self, id: i64, now_ms: i64) -> Result<(), sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE uploads \
                     SET finishedPartUploadCount = finishedPartUploadCount + 1, lastPartUploadedAt = ? \
                     WHERE id = ?",
                )
                .bind(now_ms)
                .bind(id)
                .execute(pool)
                .await?;
            }
            Self::Postgres(pool) => {
                sqlx::query(
                    "UPDATE uploads \
                     SET \"finishedPartUploadCount\" = \"finishedPartUploadCount\" + 1, \
                         \"lastPartUploadedAt\" = $1 \
                     WHERE id = $2",
                )
                .bind(now_ms)
                .bind(id)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    /// Deletes an upload row.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on delete failure.
    pub async fn delete_upload(&self, id: i64) -> Result<(), sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query("DELETE FROM uploads WHERE id = ?")
                    .bind(id)
                    .execute(pool)
                    .await?;
            }
            Self::Postgres(pool) => {
                sqlx::query("DELETE FROM uploads WHERE id = $1")
                    .bind(id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Locates the `storage_locations` row backing the given cache entry.
    /// Returns `None` when the cache entry does not exist.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on query failure.
    pub async fn find_location_for_entry(
        &self,
        cache_entry_id: &str,
    ) -> Result<Option<StorageLocation>, sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query_as(
                    "SELECT sl.* FROM storage_locations sl \
                 INNER JOIN cache_entries ce ON ce.locationId = sl.id \
                 WHERE ce.id = ?",
                )
                .bind(cache_entry_id)
                .fetch_optional(pool)
                .await
            }
            Self::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT sl.* FROM storage_locations sl \
                 INNER JOIN cache_entries ce ON ce.\"locationId\" = sl.id \
                 WHERE ce.id = $1",
                )
                .bind(cache_entry_id)
                .fetch_optional(pool)
                .await
            }
        }
    }

    /// Sets `lastDownloadedAt` on a storage location. Fire-and-forget
    /// from the download handler — a failure here is observability
    /// noise, not a functional bug.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on update failure.
    pub async fn touch_location_downloaded(
        &self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query("UPDATE storage_locations SET lastDownloadedAt = ? WHERE id = ?")
                    .bind(now_ms)
                    .bind(location_id)
                    .execute(pool)
                    .await?;
            }
            Self::Postgres(pool) => {
                sqlx::query("UPDATE storage_locations SET \"lastDownloadedAt\" = $1 WHERE id = $2")
                    .bind(now_ms)
                    .bind(location_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Looks up a cache entry matching `req` — exact primary, then
    /// prefix primary, then (per scope, if `restore_keys` is non-empty)
    /// each restore key's exact and prefix variants. Returns the first
    /// hit together with a `MatchType`.
    ///
    /// Line-matches upstream `lib/storage.ts#matchCacheEntry`.
    ///
    /// # Errors
    /// Returns `sqlx::Error` on any query failure.
    pub async fn match_cache_entry(
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
            if let Some(m) = self.walk_restore_keys(req.restore_keys, &q).await? {
                return Ok(Some(m));
            }
        }
        Ok(None)
    }

    async fn walk_restore_keys(
        &self,
        restore_keys: &[&str],
        q: &ScopeQuery<'_>,
    ) -> Result<Option<MatchedEntry>, sqlx::Error> {
        for rk in restore_keys {
            if let Some(entry) = self.find_entry_by_exact_key(rk, q).await? {
                return Ok(Some(MatchedEntry {
                    entry,
                    match_type: MatchType::ExactRestore,
                }));
            }
            if let Some(entry) = self.find_entry_by_prefix_key(rk, q).await? {
                return Ok(Some(MatchedEntry {
                    entry,
                    match_type: MatchType::PrefixedRestore,
                }));
            }
        }
        Ok(None)
    }

    async fn find_entry_by_exact_key(
        &self,
        key: &str,
        q: &ScopeQuery<'_>,
    ) -> Result<Option<CacheEntry>, sqlx::Error> {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query_as(
                    "SELECT * FROM cache_entries \
                 WHERE key = ? AND version = ? AND scope = ? AND repoId = ? \
                 ORDER BY updatedAt DESC LIMIT 1",
                )
                .bind(key)
                .bind(q.version)
                .bind(q.scope)
                .bind(q.repo_id)
                .fetch_optional(pool)
                .await
            }
            Self::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT * FROM cache_entries \
                 WHERE key = $1 AND version = $2 AND scope = $3 AND \"repoId\" = $4 \
                 ORDER BY \"updatedAt\" DESC LIMIT 1",
                )
                .bind(key)
                .bind(q.version)
                .bind(q.scope)
                .bind(q.repo_id)
                .fetch_optional(pool)
                .await
            }
        }
    }

    async fn find_entry_by_prefix_key(
        &self,
        key: &str,
        q: &ScopeQuery<'_>,
    ) -> Result<Option<CacheEntry>, sqlx::Error> {
        let pattern = format!("{}%", escape_like_pattern(key));
        match self {
            Self::Sqlite(pool) => {
                sqlx::query_as(
                    "SELECT * FROM cache_entries \
                 WHERE key LIKE ? ESCAPE '\\' AND version = ? AND scope = ? AND repoId = ? \
                 ORDER BY updatedAt DESC LIMIT 1",
                )
                .bind(&pattern)
                .bind(q.version)
                .bind(q.scope)
                .bind(q.repo_id)
                .fetch_optional(pool)
                .await
            }
            Self::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT * FROM cache_entries \
                 WHERE key LIKE $1 ESCAPE '\\' AND version = $2 AND scope = $3 AND \"repoId\" = $4 \
                 ORDER BY \"updatedAt\" DESC LIMIT 1",
                )
                .bind(&pattern)
                .bind(q.version)
                .bind(q.scope)
                .bind(q.repo_id)
                .fetch_optional(pool)
                .await
            }
        }
    }
}

// ------------------------------------------------------------------------
// Shared value types
// ------------------------------------------------------------------------

/// Escapes `%`, `_` and `\` for a SQL `LIKE ... ESCAPE '\'` pattern.
///
/// Mirrors upstream `escapeLikePattern` in `lib/storage.ts`. Order
/// matters: the backslash must be doubled first, otherwise the escapes
/// we add for `%` and `_` would themselves be doubled.
fn escape_like_pattern(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}

/// Scope of a single-query lookup inside `match_cache_entry`. Grouped
/// so `find_entry_by_exact_key` / `find_entry_by_prefix_key` stay at
/// three args.
struct ScopeQuery<'a> {
    version: &'a str,
    scope: &'a str,
    repo_id: &'a str,
}

#[cfg(test)]
#[path = "queries_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "match_cache_entry_tests.rs"]
mod match_tests;
