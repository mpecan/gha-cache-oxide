//! `/management/cache-entries` routes — list, get-one, match,
//! delete-by-id, delete-by-filter.

use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};

use crate::db::entities::{CacheEntry, CacheEntryFilter, MatchRequest, MatchType};
use crate::routes::errors::{bad_request, internal_error, not_found};
use crate::state::AppState;

use super::pagination::{Page, PageQuery};

#[derive(Debug, Deserialize)]
pub(super) struct ListQuery {
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default, rename = "repoId")]
    pub repo_id: Option<String>,
    #[serde(default)]
    pub page: Option<u32>,
    #[serde(default, rename = "itemsPerPage")]
    pub items_per_page: Option<u32>,
}

#[derive(Debug, Serialize)]
struct ListBody {
    total: i64,
    items: Vec<CacheEntry>,
    page: u32,
    #[serde(rename = "itemsPerPage")]
    items_per_page: u32,
}

/// `GET /management/cache-entries` — paginated list.
pub(super) async fn list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Response {
    let pq = PageQuery {
        page: query.page,
        items_per_page: query.items_per_page,
    };
    let Page {
        page,
        items_per_page,
        limit,
        offset,
    } = pq.resolve();

    let scope = query.scope.as_deref();
    let repo_id = query.repo_id.as_deref();

    let items = match state
        .db
        .list_cache_entries(scope, repo_id, limit, offset)
        .await
    {
        Ok(v) => v,
        Err(e) => return internal_error(&e.to_string()),
    };
    let total = match state.db.count_cache_entries(scope, repo_id).await {
        Ok(n) => n,
        Err(e) => return internal_error(&e.to_string()),
    };

    Json(ListBody {
        total,
        items,
        page,
        items_per_page,
    })
    .into_response()
}

/// `DELETE /management/cache-entries/{id}` — deletes the entry, its
/// backing storage location row, and the folder on the storage adapter.
///
/// The FK `cache_entries.locationId REFERENCES storage_locations(id) ON DELETE CASCADE`
/// (`migrations/sqlite/0001_initial_schema.sql:25`) handles the
/// `cache_entries` row removal when we delete the location, so this
/// handler issues a single `DELETE` against `storage_locations` then
/// asks the adapter to drop the folder.
///
/// Storage `delete_folder` failures are logged at `warn` but **don't
/// fail the request**: the DB transaction has already committed, the
/// adapter call is best-effort cleanup, and the operator can re-run
/// `POST /management/cleanup/trigger` to retry the orphan-folder reap
/// (cleanup:locations does the same `delete_folder` call).
pub(super) async fn delete(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let location = match state.db.find_location_for_entry(&id).await {
        Ok(Some(l)) => l,
        Ok(None) => return not_found("Cache entry not found"),
        Err(e) => return internal_error(&e.to_string()),
    };

    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return internal_error(&e.to_string()),
    };
    if let Err(e) = tx.delete_storage_location(&location.id).await {
        return internal_error(&e.to_string());
    }
    if let Err(e) = tx.commit().await {
        return internal_error(&e.to_string());
    }

    if let Err(e) = state.storage.delete_folder(&location.folder_name).await {
        tracing::warn!(
            error = %e,
            location_id = %location.id,
            folder = %location.folder_name,
            "management delete: storage folder removal failed; row already deleted",
        );
    }

    // On-demand orphan sweep (#71). Single-DELETE deletes the
    // `storage_locations` row directly so the FK CASCADE handles the
    // `cache_entries` removal — no orphan today. The hook still fires
    // for symmetry with `delete_many` and to protect against a future
    // refactor that switches this handler to delete `cache_entries`
    // directly. `drop(...)` (rather than `let _ = ...`) silences
    // clippy's `let_underscore_future` — `JoinHandle` is itself a
    // future, but tokio's drop-detaches semantics is the behaviour we
    // want.
    drop(crate::tasks::cleanup::spawn_locations_sweep(&state));

    StatusCode::NO_CONTENT.into_response()
}

/// `GET /management/cache-entries/{id}` — single-entry fetch.
/// Returns 200 with the entry body or 404 with the standard error body.
///
/// Note: the literal-segment route `GET /cache-entries/match` is
/// registered alongside this one. axum (matchit-based) prefers literal
/// segments over `{id}` params, so `/match` always reaches
/// `match_endpoint` even when an entry happened to have the literal id
/// "match". A test pin in `tests/management.rs` guards this routing
/// invariant.
pub(super) async fn get_one(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.db.find_cache_entry_by_id(&id).await {
        Ok(Some(entry)) => Json(entry).into_response(),
        Ok(None) => not_found("Cache entry not found"),
        Err(e) => internal_error(&e.to_string()),
    }
}

#[derive(Debug, Serialize)]
struct MatchResponse {
    #[serde(rename = "match")]
    match_field: CacheEntry,
    #[serde(rename = "type")]
    type_field: &'static str,
}

/// `GET /management/cache-entries/match` — runs the same algorithm
/// `GetCacheEntryDownloadURL` uses, but exposes the match-type on the
/// wire. Operators use this for incident triage: "given these
/// scopes/keys, which `cache_entries` row would be selected, and how?"
///
/// Multi-value query parameters (`scopes`, `restoreKeys`) are accepted
/// in two shapes:
/// - repeated keys: `?scopes=a&scopes=b`
/// - bracket notation: `?scopes[]=a&scopes[]=b`
///
/// Both forms are produced naturally by JavaScript clients —
/// `URLSearchParams.append` produces `scopes=a&scopes=b`, while
/// `qs.stringify` with `arrayFormat: 'brackets'` produces
/// `scopes[]=a&scopes[]=b`. Single-value forms work too: `?scopes=a`
/// is interpreted as a one-element array, mirroring upstream's
/// `(val) => Array.isArray(val) ? val : [val]` preprocess at
/// `lib/api/cache-entries.ts:46`.
pub(super) async fn match_endpoint(
    State(state): State<AppState>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.unwrap_or_default();
    let params = match parse_match_params(&raw) {
        Ok(p) => p,
        Err(msg) => return bad_request(msg),
    };

    let scope_slice: Vec<&str> = params.scopes.iter().map(String::as_str).collect();
    let restore_slice: Vec<&str> = params.restore_keys.iter().map(String::as_str).collect();
    let req = MatchRequest {
        primary_key: &params.primary_key,
        restore_keys: &restore_slice,
        version: &params.version,
        scopes: &scope_slice,
        repo_id: &params.repo_id,
    };

    match state.db.match_cache_entry(req).await {
        Ok(Some(matched)) => Json(MatchResponse {
            match_field: matched.entry,
            type_field: match_type_kebab(matched.match_type),
        })
        .into_response(),
        // Divergence: upstream returns `200 OK` with body `null` when
        // no entry matches (`lib/api/cache-entries.ts:66`'s
        // `cacheEntrySchema.nullable()`); we return `404 Not Found`
        // because nullable-200 obscures "matched nothing" vs
        // "endpoint returned a body with `match: null`" for clients
        // doing simple if-status checks. The `404` body still carries
        // the standard `{statusCode, message}` shape.
        Ok(None) => not_found("No matching cache entry"),
        Err(e) => internal_error(&e.to_string()),
    }
}

/// Maps [`MatchType`] to the upstream-compatible kebab-case enum
/// strings (`lib/api/cache-entries.ts:60`).
const fn match_type_kebab(t: MatchType) -> &'static str {
    match t {
        MatchType::ExactPrimary => "exact-primary",
        MatchType::PrefixedPrimary => "prefixed-primary",
        MatchType::ExactRestore => "exact-restore",
        MatchType::PrefixedRestore => "prefixed-restore",
    }
}

/// Parsed `/match` query parameters.
#[derive(Debug)]
struct MatchParams {
    primary_key: String,
    version: String,
    repo_id: String,
    scopes: Vec<String>,
    restore_keys: Vec<String>,
}

/// Parses the raw query string into [`MatchParams`]. Accepts both
/// repeated-key (`scopes=a&scopes=b`) and bracket-notation
/// (`scopes[]=a&scopes[]=b`) shapes for the multi-value parameters.
fn parse_match_params(raw: &str) -> Result<MatchParams, &'static str> {
    let mut primary_key: Option<String> = None;
    let mut version: Option<String> = None;
    let mut repo_id: Option<String> = None;
    let mut scopes: Vec<String> = Vec::new();
    let mut restore_keys: Vec<String> = Vec::new();

    for (k, v) in url::form_urlencoded::parse(raw.as_bytes()) {
        match k.as_ref() {
            "primaryKey" => primary_key = Some(v.into_owned()),
            "version" => version = Some(v.into_owned()),
            "repoId" => repo_id = Some(v.into_owned()),
            "scopes" | "scopes[]" => scopes.push(v.into_owned()),
            "restoreKeys" | "restoreKeys[]" => restore_keys.push(v.into_owned()),
            _ => {}
        }
    }

    let primary_key = primary_key.ok_or("missing required query parameter: primaryKey")?;
    let version = version.ok_or("missing required query parameter: version")?;
    let repo_id = repo_id.ok_or("missing required query parameter: repoId")?;
    if scopes.is_empty() {
        return Err("missing required query parameter: scopes (at least one)");
    }

    Ok(MatchParams {
        primary_key,
        version,
        repo_id,
        scopes,
        restore_keys,
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DeleteManyQuery {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub repo_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct DeleteManyResponse {
    deleted: u64,
}

/// `DELETE /management/cache-entries` — bulk delete by query filter.
///
/// At least one of `key`, `version`, `scope`, `repoId` must be set;
/// requests with all four unset return `400 Bad Request`. This is a
/// deliberate divergence from upstream — `lib/api/cache-entries.ts:163-168`
/// accepts an empty filter set and would silently delete every row.
///
/// Returns `200 OK` with `{"deleted": <count>}` on success. Upstream
/// returns no body; we surface the count because the alternative
/// (operator manually counting) is uncomfortable for a destructive op.
///
/// Storage cleanup runs out-of-band: the FK
/// `cache_entries.locationId REFERENCES storage_locations(id) ON DELETE CASCADE`
/// runs the OPPOSITE direction, so deleting `cache_entries` rows
/// leaves their `storage_locations` rows orphan-but-present. We spawn
/// a detached `cleanup:storage-locations` sweep at the end of the
/// success path (#71) — same shape as upstream's
/// `event.waitUntil(runTask('cleanup:storage-locations'))` at
/// `lib/api/cache-entries.ts:169`. The hourly background scheduler
/// keeps running as a safety net.
///
/// Honours `repoId` — divergent from upstream `deleteMany` which
/// silently drops the filter (`lib/api/cache-entries.ts:163-168`).
pub(super) async fn delete_many(
    State(state): State<AppState>,
    Query(query): Query<DeleteManyQuery>,
) -> Response {
    let filter = CacheEntryFilter {
        key: query.key.as_deref(),
        version: query.version.as_deref(),
        scope: query.scope.as_deref(),
        repo_id: query.repo_id.as_deref(),
    };
    if filter.key.is_none()
        && filter.version.is_none()
        && filter.scope.is_none()
        && filter.repo_id.is_none()
    {
        return bad_request("at least one filter required: key, version, scope, or repoId");
    }

    match state.db.delete_cache_entries_by_filter(filter).await {
        Ok(deleted) => {
            // On-demand orphan sweep (#71). Bulk delete removes
            // `cache_entries` rows directly; their `storage_locations`
            // rows + folders are now orphans. Spawn detached so the
            // HTTP response isn't blocked on the sweep — mirrors
            // upstream's `event.waitUntil(runTask(...))` at
            // `lib/api/cache-entries.ts:169`. `drop(...)` silences
            // `let_underscore_future`.
            drop(crate::tasks::cleanup::spawn_locations_sweep(&state));
            Json(DeleteManyResponse { deleted }).into_response()
        }
        Err(e) => internal_error(&e.to_string()),
    }
}
