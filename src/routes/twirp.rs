//! Twirp-style cache service v2 routes.
//!
//! Three POST endpoints under
//! `/twirp/github.actions.results.api.v1.CacheService/{Method}` —
//! path is hard-coded by the `actions/cache` SDK and MUST NOT change.
//! Each handler consumes the [`CacheScope`] extension attached by the
//! auth middleware and returns a body shaped to mirror upstream
//! `routes/twirp/.../CacheService/*.post.ts` exactly.
//!
//! # Error body shape
//!
//! Upstream's h3 `createError` produces two distinct body shapes
//! depending on which key the caller supplied:
//! - body-validation failures (zod parse errors) carry `statusMessage`
//!   (e.g. `CreateCacheEntry.post.ts:18-20`)
//! - auth, scope and not-found failures carry `message` (e.g.
//!   `CreateCacheEntry.post.ts:27`, `FinalizeCacheEntryUpload.ts:30-32`)
//!
//! The port mirrors that split: [`bad_request_body`] emits
//! `{statusCode, statusMessage}` for body-parse failures, while every
//! other error path uses [`error_response`] which emits
//! `{statusCode, message}`.
//!
//! # Deviations from upstream
//!
//! - Validation-class failures in `FinalizeCacheEntryUpload`
//!   (`NoPartsUploaded`, `PartsCountMismatch`, `DiskCountMismatch`)
//!   return 400 instead of upstream's implicit 500 via `throw new
//!   Error`. The upload row has already been purged at that point, so
//!   400 "client must re-reserve" is the truthful semantic.
//! - `GetCacheEntryDownloadURL` does NOT include upstream's
//!   missing-storage purge-and-retry loop (`lib/storage.ts:486-525`).
//!   The default download URL points at our own `/download/{id}`
//!   endpoint which itself is out of scope until #9 lands; until then
//!   there is nothing for that loop to detect or retry against.

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::middleware as axum_mw;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::post;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::auth::{CacheScope, ScopeEntry, require_github_token};
use crate::cache::{CompleteUploadError, CompleteUploadParams, complete_upload};
use crate::db::entities::{CacheEntry, CacheEntryCoord, MatchRequest, NewUpload};
use crate::db::id::{new_upload_id, now_ms};
use crate::routes::errors::{bad_request, forbidden, internal_error, not_found};
use crate::state::AppState;

/// Builds the Twirp sub-router with the auth middleware applied.
/// Expected to be `nest`ed under the full `CacheService` path in `lib.rs`.
pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/CreateCacheEntry", post(create_cache_entry))
        .route(
            "/FinalizeCacheEntryUpload",
            post(finalize_cache_entry_upload),
        )
        .route(
            "/GetCacheEntryDownloadURL",
            post(get_cache_entry_download_url),
        )
        .layer(axum_mw::from_fn_with_state(state, require_github_token))
}

// -- Request / response shapes -------------------------------------------

#[derive(Debug, Deserialize)]
struct KeyVersionBody {
    key: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct DownloadBody {
    key: String,
    version: String,
    #[serde(default)]
    restore_keys: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct CreateEntryOk {
    ok: bool,
    signed_upload_url: String,
}

#[derive(Debug, Serialize)]
struct FinalizeOk {
    ok: bool,
    entry_id: String,
}

#[derive(Debug, Serialize)]
struct DownloadOk {
    ok: bool,
    signed_download_url: String,
    matched_key: String,
}

#[derive(Debug, Serialize)]
struct OkFalse {
    ok: bool,
}

// -- Handlers ------------------------------------------------------------

async fn create_cache_entry(
    State(state): State<AppState>,
    Extension(scope): Extension<CacheScope>,
    body: Result<Json<KeyVersionBody>, JsonRejection>,
) -> Response {
    let body = match parse_body(body) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let Some(write_scope) = find_write_scope(&scope) else {
        return forbidden("No scope with write permission found");
    };

    let coord = CacheEntryCoord {
        key: &body.key,
        version: &body.version,
        scope: write_scope,
        repo_id: &scope.repo_id,
    };

    match state.db.find_upload_by_coord(coord).await {
        Ok(Some(_)) => return Json(OkFalse { ok: false }).into_response(),
        Ok(None) => {}
        Err(e) => return internal_error(&e.to_string()),
    }

    let id = new_upload_id();
    let folder = id.to_string();
    let new_upload = NewUpload {
        id,
        coord,
        folder_name: &folder,
        created_at_ms: now_ms(),
    };
    let upload = match state.db.create_upload(new_upload).await {
        Ok(u) => u,
        Err(e) => return internal_error(&e.to_string()),
    };

    let url = format!(
        "{}/devstoreaccount1/upload/{}",
        trim_slash(state.config.api_base_url.as_str()),
        upload.id
    );
    Json(CreateEntryOk {
        ok: true,
        signed_upload_url: url,
    })
    .into_response()
}

async fn finalize_cache_entry_upload(
    State(state): State<AppState>,
    Extension(scope): Extension<CacheScope>,
    body: Result<Json<KeyVersionBody>, JsonRejection>,
) -> Response {
    let body = match parse_body(body) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let Some(write_scope) = find_write_scope(&scope) else {
        return forbidden("No scope with write permission found");
    };

    let coord = CacheEntryCoord {
        key: &body.key,
        version: &body.version,
        scope: write_scope,
        repo_id: &scope.repo_id,
    };

    match complete_upload(
        &*state.db,
        state.storage.as_ref(),
        CompleteUploadParams {
            coord,
            now_ms: now_ms(),
        },
    )
    .await
    {
        Ok(upload) => Json(FinalizeOk {
            ok: true,
            entry_id: upload.id.to_string(),
        })
        .into_response(),
        Err(CompleteUploadError::UploadNotFound) => not_found("Upload not found"),
        Err(
            e @ (CompleteUploadError::NoPartsUploaded
            | CompleteUploadError::PartsCountMismatch { .. }
            | CompleteUploadError::DiskCountMismatch { .. }),
        ) => bad_request(&e.to_string()),
        Err(CompleteUploadError::Db(e)) => internal_error(&e.to_string()),
        Err(CompleteUploadError::Storage(e)) => internal_error(&e.to_string()),
    }
}

async fn get_cache_entry_download_url(
    State(state): State<AppState>,
    Extension(scope): Extension<CacheScope>,
    body: Result<Json<DownloadBody>, JsonRejection>,
) -> Response {
    let body = match parse_body(body) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let scopes_sorted = scopes_by_permission_desc(&scope.scopes);
    let scope_slice: Vec<&str> = scopes_sorted.iter().map(String::as_str).collect();
    let restore_owned = body.restore_keys.clone().unwrap_or_default();
    let restore_slice: Vec<&str> = restore_owned.iter().map(String::as_str).collect();

    let req = MatchRequest {
        primary_key: &body.key,
        restore_keys: &restore_slice,
        version: &body.version,
        scopes: &scope_slice,
        repo_id: &scope.repo_id,
    };

    match state.db.match_cache_entry(req).await {
        Ok(Some(m)) => match resolve_download_url(&state, &m.entry).await {
            Ok(url) => Json(DownloadOk {
                ok: true,
                signed_download_url: url,
                matched_key: m.entry.key,
            })
            .into_response(),
            Err(e) => internal_error(&e.to_string()),
        },
        Ok(None) => Json(OkFalse { ok: false }).into_response(),
        Err(e) => internal_error(&e.to_string()),
    }
}

/// Picks the URL to return for a matched cache entry. Ports
/// `lib/storage.ts:509-519`:
///
/// - Default URL is the server-proxied `/download/{entry_id}` that the
///   blob route serves (and that the lazy-merge path (#15) still needs
///   to mint the merged blob on the first download).
/// - When `ENABLE_DIRECT_DOWNLOADS` is on AND the adapter can sign URLs
///   AND the location's `mergedAt` is set, return a presigned URL so the
///   client streams direct from the object store (10-minute TTL, set
///   inside the adapter).
/// - Every other combination falls back to the default server URL so
///   the first-download / non-signing / flag-off paths all keep working.
///
/// Upstream's "purge missing storage" retry loop
/// (`lib/storage.ts:501-507`) is deliberately not ported — see the
/// module-level "Deviations from upstream" docstring.
async fn resolve_download_url(state: &AppState, entry: &CacheEntry) -> Result<String, sqlx::Error> {
    let default_url = format!(
        "{}/download/{}",
        trim_slash(state.config.api_base_url.as_str()),
        entry.id
    );
    if !state.config.enable_direct_downloads {
        return Ok(default_url);
    }
    let Some(location) = state.db.find_location_for_entry(&entry.id).await? else {
        return Ok(default_url);
    };
    if location.merged_at.is_none() {
        return Ok(default_url);
    }
    let merged_name = format!("{}/merged", location.folder_name);
    match state.storage.signed_url(&merged_name).await {
        Ok(Some(url)) => Ok(url.to_string()),
        Ok(None) => Ok(default_url),
        Err(e) => {
            // Signing failures for an otherwise-valid location are
            // observability-grade — fall back to the server URL so the
            // client still gets a working download rather than a 500.
            tracing::warn!(
                error = %e,
                location_id = %location.id,
                "signed_url failed; falling back to server-proxied download URL",
            );
            Ok(default_url)
        }
    }
}

// -- Helpers -------------------------------------------------------------

/// First scope with write permission (`Permission >= 2`), mirroring
/// upstream's `scopes.find(s => s.Permission >= 2)`. No sorting — the
/// order matches whatever the token carries.
fn find_write_scope(scope: &CacheScope) -> Option<&str> {
    scope
        .scopes
        .iter()
        .find(|s| s.permission >= 2)
        .map(|s| s.scope.as_str())
}

/// Stable sort of scopes by `permission` descending, mirroring
/// upstream's `sortBy([prop('Permission'), 'desc'])` used by
/// `GetCacheEntryDownloadURL`. Equal permissions retain input order.
fn scopes_by_permission_desc(scopes: &[ScopeEntry]) -> Vec<String> {
    let mut out: Vec<&ScopeEntry> = scopes.iter().collect();
    out.sort_by(|a, b| b.permission.cmp(&a.permission));
    out.into_iter().map(|s| s.scope.clone()).collect()
}

fn trim_slash(url: &str) -> &str {
    url.trim_end_matches('/')
}

/// Extracts the body from a `Result<Json<T>, JsonRejection>` extractor,
/// converting any rejection to the upstream-compatible 400 body shape
/// `{statusCode, statusMessage}`. Returned as `Err(Response)` so handlers
/// can `?`-chain or `match` on it.
///
/// `axum::http::Response` is ~128 bytes, which trips `result_large_err`;
/// the helper is called once per handler so the size is irrelevant — the
/// alternative (`Box<Response>`) trades an allocation for nothing.
#[allow(clippy::result_large_err)]
fn parse_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, Response> {
    match body {
        Ok(Json(b)) => Ok(b),
        Err(rej) => Err(bad_request_body(&format!(
            "Invalid body: {}",
            rej.body_text()
        ))),
    }
}

/// 400 with the upstream `statusMessage` key — used for body-validation
/// failures, mirroring `CreateCacheEntry.post.ts:18-20` and the other two
/// handlers' zod-rejection paths. Stays local to this module because
/// no other route produces h3/zod-shaped bodies (everything else uses
/// [`crate::routes::errors::bad_request`]).
fn bad_request_body(message: &str) -> Response {
    let body = Json(json!({
        "statusCode": StatusCode::BAD_REQUEST.as_u16(),
        "statusMessage": message,
    }));
    (StatusCode::BAD_REQUEST, body).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn find_write_scope_picks_first_with_permission_gte_2() {
        let scope = CacheScope {
            repo_id: "1".into(),
            scopes: vec![
                ScopeEntry {
                    scope: "read".into(),
                    permission: 1,
                },
                ScopeEntry {
                    scope: "write-a".into(),
                    permission: 2,
                },
                ScopeEntry {
                    scope: "write-b".into(),
                    permission: 3,
                },
            ],
        };
        assert_eq!(find_write_scope(&scope), Some("write-a"));
    }

    #[test]
    fn find_write_scope_returns_none_when_all_read_only() {
        let scope = CacheScope {
            repo_id: "1".into(),
            scopes: vec![ScopeEntry {
                scope: "r".into(),
                permission: 0,
            }],
        };
        assert!(find_write_scope(&scope).is_none());
    }

    #[test]
    fn scopes_by_permission_desc_is_stable() {
        let scopes = vec![
            ScopeEntry {
                scope: "low".into(),
                permission: 0,
            },
            ScopeEntry {
                scope: "high-1".into(),
                permission: 3,
            },
            ScopeEntry {
                scope: "mid".into(),
                permission: 1,
            },
            ScopeEntry {
                scope: "high-2".into(),
                permission: 3,
            },
        ];
        let out = scopes_by_permission_desc(&scopes);
        assert_eq!(out, vec!["high-1", "high-2", "mid", "low"]);
    }

    #[test]
    fn trim_slash_strips_trailing_slash_only() {
        assert_eq!(trim_slash("http://x:1/"), "http://x:1");
        assert_eq!(trim_slash("http://x:1"), "http://x:1");
        assert_eq!(trim_slash("http://x:1/path"), "http://x:1/path");
    }
}
