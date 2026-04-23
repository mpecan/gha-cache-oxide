//! Integration test for `GET /health`.
//!
//! Drives the router directly via `tower::ServiceExt::oneshot` so the test
//! does not need a bound TCP port — keeps tests parallel-safe.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use gha_cache_oxide::config::{AppConfig, DbConfig, LogFormat, StorageConfig};
use gha_cache_oxide::db::Db;
use gha_cache_oxide::state::AppState;
use tower::ServiceExt;

async fn test_app() -> axum::Router {
    let db = Db::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let config = AppConfig {
        api_base_url: "http://localhost:3000".parse().unwrap(),
        port: 0,
        log_format: LogFormat::Text,
        cache_cleanup_older_than_days: 90,
        disable_cleanup_jobs: true,
        enable_direct_downloads: false,
        skip_token_validation: true,
        management_api_key: None,
        storage: StorageConfig::Filesystem {
            path: PathBuf::from("/tmp/gha-test"),
        },
        database: DbConfig::Sqlite {
            path: PathBuf::from(":memory:"),
        },
    };
    gha_cache_oxide::build_app(AppState::new(db, config))
}

#[tokio::test]
async fn health_endpoint_returns_ok_true() {
    let app = test_app().await;

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
