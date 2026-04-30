//! Integration test for `GET /health`.
//!
//! Drives the router directly via `tower::ServiceExt::oneshot` so the test
//! does not need a bound TCP port — keeps tests parallel-safe.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use gha_cache_oxide::auth::AuthError;
use gha_cache_oxide::auth::{JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, StorageConfig};
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::FilesystemAdapter;
use tempfile::TempDir;
use tower::ServiceExt;

/// Stub fetcher for the integration test — the router never hits auth
/// because we don't apply the middleware to `/health`.
struct NullFetcher;

#[async_trait::async_trait]
impl JwksFetcher for NullFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        Ok(Vec::new())
    }
}

/// Returns both the router and the `TempDir` guard so the storage root
/// outlives any test operation.
async fn test_app() -> (axum::Router, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let storage = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(NullFetcher)));
    let config = AppConfig::test_defaults(
        StorageConfig::Filesystem {
            path: tmp.path().to_path_buf(),
        },
        DbConfig::Sqlite {
            path: PathBuf::from(":memory:"),
        },
    );
    let router = gha_cache_oxide::build_app(AppState::new(db, storage, jwks, config));
    (router, tmp)
}

#[tokio::test]
async fn health_endpoint_returns_ok_true() {
    let (app, _tmp) = test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json, serde_json::json!({ "ok": true }));
}
