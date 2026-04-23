//! Twirp-style cache service v2 routes.
//!
//! Three POST endpoints under
//! `/twirp/github.actions.results.api.v1.CacheService/{Method}` —
//! path is hard-coded by the `actions/cache` SDK and MUST NOT change.
//! Each handler consumes the [`CacheScope`] extension attached by the
//! auth middleware and returns a body shaped to mirror upstream
//! `routes/twirp/.../CacheService/*.post.ts` exactly.
//!
//! Error bodies use the h3 `createError` shape —
//! `{ statusCode, message }` — so mixed upstream/port deployments see
//! consistent payloads. Validation-class failures in
//! `FinalizeCacheEntryUpload` return 400 (not upstream's implicit 500
//! via `throw new Error`); the issue calls this out as a deliberate
//! refinement because the upload row has already been purged by that
//! point, so 400 "client must re-reserve" is the truthful semantic.

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
use crate::db::entities::{CacheEntryCoord, MatchRequest, NewUpload};
use crate::db::id::{new_upload_id, now_ms};
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
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => return bad_request(&e.body_text()),
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
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => return bad_request(&e.body_text()),
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
        &state.db,
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
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => return bad_request(&e.body_text()),
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
        Ok(Some(m)) => {
            let url = format!(
                "{}/download/{}",
                trim_slash(state.config.api_base_url.as_str()),
                m.entry.id
            );
            Json(DownloadOk {
                ok: true,
                signed_download_url: url,
                matched_key: m.entry.key,
            })
            .into_response()
        }
        Ok(None) => Json(OkFalse { ok: false }).into_response(),
        Err(e) => internal_error(&e.to_string()),
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

fn error_response(status: StatusCode, message: &str) -> Response {
    let body = Json(json!({
        "statusCode": status.as_u16(),
        "message": message,
    }));
    (status, body).into_response()
}

fn bad_request(msg: &str) -> Response {
    error_response(StatusCode::BAD_REQUEST, msg)
}

fn forbidden(msg: &str) -> Response {
    error_response(StatusCode::FORBIDDEN, msg)
}

fn not_found(msg: &str) -> Response {
    error_response(StatusCode::NOT_FOUND, msg)
}

fn internal_error(msg: &str) -> Response {
    tracing::error!(message = msg, "cache-service route internal error");
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
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
