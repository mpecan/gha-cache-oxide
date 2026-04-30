//! `MANAGEMENT_API_KEY` gating: 501 when unset, 401 on missing/wrong key.
//!
//! Every route registered under `/management/...` flows through the
//! `require_management_key` middleware; the assertions here pin the
//! gate at the binary boundary so a future route added without
//! re-attaching the layer would surface as a regression.

use axum::http::{Method, StatusCode};
use tower::ServiceExt;

use super::common::{KEY, body_json, harness, req};

#[tokio::test]
async fn every_route_returns_501_when_management_key_unset() {
    let h = harness(None).await;

    for (method, uri) in [
        (Method::GET, "/management/cache-entries"),
        (Method::GET, "/management/cache-entries/anything"),
        (
            Method::GET,
            "/management/cache-entries/match?primaryKey=k&version=v&repoId=r&scopes=s",
        ),
        (Method::DELETE, "/management/cache-entries"),
        (Method::DELETE, "/management/cache-entries/anything"),
        (Method::GET, "/management/storage-locations"),
        (Method::GET, "/management/storage-locations/anything"),
        (Method::DELETE, "/management/storage-locations/anything"),
        (Method::POST, "/management/cleanup/trigger"),
    ] {
        let resp = h
            .router
            .clone()
            .oneshot(req(method.clone(), uri, Some(KEY)))
            .await
            .unwrap();
        let (status, body) = body_json(resp).await;
        assert_eq!(
            status,
            StatusCode::NOT_IMPLEMENTED,
            "{method} {uri} must 501 when MANAGEMENT_API_KEY is unset",
        );
        assert_eq!(body["statusCode"], 501);
        assert_eq!(
            body["message"],
            "Management API not enabled - set MANAGEMENT_API_KEY"
        );
    }
}

#[tokio::test]
async fn missing_authorization_returns_401() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/management/cache-entries", None))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["statusCode"], 401);
}

#[tokio::test]
async fn wrong_key_returns_401() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries",
            Some("not-the-key"),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "Invalid management API key");
}
