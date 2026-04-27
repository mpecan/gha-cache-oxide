//! `require_management_key` axum middleware.
//!
//! Gates the `/management/...` sub-router on the `MANAGEMENT_API_KEY`
//! env var. Three branches matter:
//!
//! 1. `management_api_key` is `None` → respond `501 Not Implemented`.
//!    This is upstream's "endpoint exists but is disabled" semantic
//!    transposed to a status code that doesn't lie about what's
//!    deployed (`404` would be wrong — the route IS there, just gated).
//! 2. Authorization header missing or malformed → `401 Unauthorized`.
//! 3. Bearer token doesn't match the configured key → `401`.
//!
//! Mirrors upstream `lib/api/base.ts:11-14` semantically, but uses the
//! `Authorization: Bearer <key>` header convention rather than upstream's
//! `x-api-key`. This is an explicit, documented deviation — see
//! `README.md` § "Management API".

use axum::Json;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::state::AppState;

/// Outcome of `authorize`. `Ok` lets the handler chain run; the two
/// error variants map to distinct status codes.
enum AuthOutcome {
    Disabled,
    Unauthorized(&'static str),
}

/// Axum middleware: validates `Authorization: Bearer <MANAGEMENT_API_KEY>`.
///
/// On success the request is passed through to `next`. On failure the
/// middleware short-circuits with a JSON body shaped like
/// `{statusCode, message}` — same shape `routes::errors::error_response`
/// emits, kept consistent across the server.
pub(super) async fn require_management_key(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    match authorize(&state, req.headers()) {
        Ok(()) => next.run(req).await,
        Err(AuthOutcome::Disabled) => disabled_response(),
        Err(AuthOutcome::Unauthorized(msg)) => unauthorized_response(msg),
    }
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), AuthOutcome> {
    let Some(configured) = state.config.management_api_key.as_ref() else {
        return Err(AuthOutcome::Disabled);
    };
    let token = extract_bearer(headers).ok_or(AuthOutcome::Unauthorized(
        "Authorization header missing or malformed",
    ))?;
    if token == configured.expose() {
        Ok(())
    } else {
        Err(AuthOutcome::Unauthorized("Invalid management API key"))
    }
}

fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn disabled_response() -> Response {
    let body = Json(json!({
        "statusCode": StatusCode::NOT_IMPLEMENTED.as_u16(),
        "message": "Management API not enabled - set MANAGEMENT_API_KEY",
    }));
    (StatusCode::NOT_IMPLEMENTED, body).into_response()
}

fn unauthorized_response(message: &str) -> Response {
    let body = Json(json!({
        "statusCode": StatusCode::UNAUTHORIZED.as_u16(),
        "message": message,
    }));
    (StatusCode::UNAUTHORIZED, body).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    //! Unit-level coverage for the three authorization branches.
    //! Integration tests in `tests/management.rs` exercise the
    //! middleware end-to-end against a real router.
    use std::path::PathBuf;
    use std::sync::Arc;

    use axum::http::HeaderMap;
    use tempfile::TempDir;

    use super::{AuthOutcome, authorize, extract_bearer};
    use crate::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
    use crate::config::{AppConfig, DbConfig, LogFormat, Secret, StorageConfig};
    use crate::db::SqliteDb;
    use crate::state::AppState;
    use crate::storage::FilesystemAdapter;

    struct StubFetcher;
    #[async_trait::async_trait]
    impl JwksFetcher for StubFetcher {
        async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
            Ok(vec![])
        }
    }

    /// Builds an `AppState` plus the `TempDir` that backs its
    /// filesystem adapter — the caller is responsible for keeping the
    /// dir alive (returned alongside the state) for the duration of
    /// the test.
    async fn make_state(key: Option<&str>) -> (AppState, TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
        let db = SqliteDb::connect_in_memory().await.unwrap();
        let jwks = Arc::new(JwksCache::new(Arc::new(StubFetcher)));
        let config = AppConfig {
            api_base_url: "http://localhost:3000".parse().unwrap(),
            port: 0,
            log_format: LogFormat::Text,
            cache_cleanup_older_than_days: 90,
            disable_cleanup_jobs: true,
            enable_direct_downloads: false,
            skip_token_validation: true,
            management_api_key: key.map(|k| Secret::new(k.to_string())),
            storage: StorageConfig::Filesystem {
                path: tmp.path().to_path_buf(),
            },
            database: DbConfig::Sqlite {
                path: PathBuf::from(":memory:"),
            },
        };
        (AppState::new(Arc::new(db), storage, jwks, config), tmp)
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        h
    }

    #[tokio::test]
    async fn disabled_when_key_unset() {
        let (state, _tmp) = make_state(None).await;
        let outcome = authorize(&state, &bearer("anything"));
        assert!(matches!(outcome, Err(AuthOutcome::Disabled)));
    }

    #[tokio::test]
    async fn unauthorized_when_header_missing() {
        let (state, _tmp) = make_state(Some("k")).await;
        let headers = HeaderMap::new();
        let outcome = authorize(&state, &headers);
        assert!(matches!(outcome, Err(AuthOutcome::Unauthorized(_))));
    }

    #[tokio::test]
    async fn unauthorized_on_wrong_key() {
        let (state, _tmp) = make_state(Some("correct")).await;
        let outcome = authorize(&state, &bearer("incorrect"));
        assert!(matches!(outcome, Err(AuthOutcome::Unauthorized(_))));
    }

    #[tokio::test]
    async fn ok_on_correct_key() {
        let (state, _tmp) = make_state(Some("correct")).await;
        let outcome = authorize(&state, &bearer("correct"));
        assert!(outcome.is_ok());
    }

    #[test]
    fn extract_bearer_strips_prefix_only() {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer abc".parse().unwrap(),
        );
        assert_eq!(extract_bearer(&h), Some("abc"));

        let mut h2 = HeaderMap::new();
        h2.insert(
            axum::http::header::AUTHORIZATION,
            "Basic abc".parse().unwrap(),
        );
        assert_eq!(extract_bearer(&h2), None);
    }
}
