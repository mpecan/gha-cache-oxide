//! Typed rows for the three cache metadata tables.
//!
//! Column-name mapping uses `sqlx`'s `#[sqlx(rename = "...")]` attribute to
//! keep SQL `camelCase` (matching upstream) while Rust stays `snake_case`.
//!
//! `CacheEntry` and `StorageLocation` derive `serde::Serialize` so the
//! management API (issue #19) can emit them directly as JSON. The
//! parallel `#[serde(rename = ...)]` annotations keep the wire shape
//! identical to upstream's `cacheEntrySchema` / `storageLocationSchema`
//! (camelCase keys), so an operator scripting against either server
//! parses the same response shape.

use serde::Serialize;

/// One row in `cache_entries`.
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct CacheEntry {
    pub id: String,
    pub key: String,
    pub version: String,
    pub scope: String,
    #[sqlx(rename = "repoId")]
    #[serde(rename = "repoId")]
    pub repo_id: String,
    #[sqlx(rename = "updatedAt")]
    #[serde(rename = "updatedAt")]
    pub updated_at: i64,
    #[sqlx(rename = "locationId")]
    #[serde(rename = "locationId")]
    pub location_id: String,
}

/// One row in `storage_locations`.
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct StorageLocation {
    pub id: String,
    #[sqlx(rename = "folderName")]
    #[serde(rename = "folderName")]
    pub folder_name: String,
    #[sqlx(rename = "partCount")]
    #[serde(rename = "partCount")]
    pub part_count: i64,
    #[sqlx(rename = "mergeStartedAt")]
    #[serde(rename = "mergeStartedAt")]
    pub merge_started_at: Option<i64>,
    #[sqlx(rename = "mergedAt")]
    #[serde(rename = "mergedAt")]
    pub merged_at: Option<i64>,
    #[sqlx(rename = "partsDeletedAt")]
    #[serde(rename = "partsDeletedAt")]
    pub parts_deleted_at: Option<i64>,
    #[sqlx(rename = "lastDownloadedAt")]
    #[serde(rename = "lastDownloadedAt")]
    pub last_downloaded_at: Option<i64>,
}

/// One row in `uploads`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Upload {
    pub id: i64,
    pub key: String,
    pub version: String,
    pub scope: String,
    #[sqlx(rename = "repoId")]
    pub repo_id: String,
    #[sqlx(rename = "createdAt")]
    pub created_at: i64,
    #[sqlx(rename = "lastPartUploadedAt")]
    pub last_part_uploaded_at: Option<i64>,
    #[sqlx(rename = "folderName")]
    pub folder_name: String,
    #[sqlx(rename = "finishedPartUploadCount")]
    pub finished_part_upload_count: i64,
    #[sqlx(rename = "startedPartUploadCount")]
    pub started_part_upload_count: i64,
}

/// Coordinates identifying a cache entry: `(key, version, scope, repo_id)`.
/// Used as a single argument to reduce function-arg counts.
#[derive(Debug, Clone, Copy)]
pub struct CacheEntryCoord<'a> {
    pub key: &'a str,
    pub version: &'a str,
    pub scope: &'a str,
    pub repo_id: &'a str,
}

/// Identifies a storage location that has been superseded by an overwriting
/// upload. Returned by `upsert_cache_entry_tx` so the caller (finalize) can
/// delete the old blob from the storage adapter.
#[derive(Debug, Clone)]
pub struct PreviousLocation {
    pub id: String,
    pub folder_name: String,
}

/// Fields required to insert a new `uploads` row. Bundled to stay under
/// the 5-argument clippy threshold on `create_upload`.
#[derive(Debug, Clone)]
pub struct NewUpload<'a> {
    pub id: i64,
    pub coord: CacheEntryCoord<'a>,
    pub folder_name: &'a str,
    pub created_at_ms: i64,
}

/// Classifies how a `cache_entries` row was matched.
///
/// Mirrors upstream's four-valued `type` field in
/// `lib/storage.ts#matchCacheEntry` so the caller can surface the same
/// distinction back to `actions/cache`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchType {
    /// `key = primaryKey` on the first scope to yield a hit.
    ExactPrimary,
    /// `key LIKE primaryKey%` on the first scope to yield a hit.
    PrefixedPrimary,
    /// `key = restoreKey[i]` for some `i`, after primary failed.
    ExactRestore,
    /// `key LIKE restoreKey[i]%` for some `i`, after primary and exact
    /// restore failed.
    PrefixedRestore,
}

/// A cache entry returned by [`Db::match_cache_entry`](super::Db::match_cache_entry),
/// tagged with how it was matched.
#[derive(Debug, Clone)]
pub struct MatchedEntry {
    pub entry: CacheEntry,
    pub match_type: MatchType,
}

/// Inputs to [`Db::match_cache_entry`](super::Db::match_cache_entry).
///
/// `scopes` is in priority order — the first scope wins when multiple
/// scopes could match. `restore_keys` is ordered from highest to lowest
/// priority (upstream semantics).
#[derive(Debug, Clone, Copy)]
pub struct MatchRequest<'a> {
    pub primary_key: &'a str,
    pub restore_keys: &'a [&'a str],
    pub version: &'a str,
    pub scopes: &'a [&'a str],
    pub repo_id: &'a str,
}
