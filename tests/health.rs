//! Integration test for `GET /health`.
//!
//! Drives the router directly via `tower::ServiceExt::oneshot` so the test
//! does not need a bound TCP port — keeps tests parallel-safe.

#![allow(clippy::unwrap_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn health_endpoint_returns_ok_true() {
    let app = gha_cache_oxide::build_app();

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
