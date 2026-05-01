//! oRPC `_rpc` auth-gating + unknown-procedure + envelope-tolerance tests.
//!
//! Wire-format invariants split out from the per-procedure files so
//! a regression in the X-Api-Key middleware or the 404 fallback shows
//! up under a stable test name.

use axum::http::StatusCode;
use serde_json::json;
use tower::ServiceExt;

use super::common::{KEY, harness};
use super::rpc_common::{body_json, rpc_post};

#[tokio::test]
async fn missing_api_key_returns_orpc_unauthorized() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post("cacheEntries/findMany", None, json!({})))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let inner = &body["json"];
    assert_eq!(inner["defined"], json!(false));
    assert_eq!(inner["code"], json!("UNAUTHORIZED"));
    assert_eq!(inner["status"], json!(401));
    assert!(inner["message"].is_string());
}

#[tokio::test]
async fn wrong_api_key_returns_orpc_unauthorized() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post("cacheEntries/findMany", Some("wrong"), json!({})))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["json"]["code"], json!("UNAUTHORIZED"));
}

#[tokio::test]
async fn unconfigured_management_key_returns_service_unavailable() {
    // Upstream `routes/management-api/_rpc.ts:18`:
    // `createError({statusCode: 503, message: 'Management API is disabled'})`.
    let h = harness(None).await;
    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/findMany",
            Some("anything"),
            json!({}),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["json"]["code"], json!("SERVICE_UNAVAILABLE"));
    assert_eq!(body["json"]["status"], json!(503));
}

#[tokio::test]
async fn unknown_procedure_returns_orpc_not_found() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post("cacheEntries/nope", Some(KEY), json!({})))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["json"]["code"], json!("NOT_FOUND"));
}

#[tokio::test]
async fn meta_field_in_request_is_ignored() {
    // The orpc client may include a `meta` array for type-hint
    // encoding even when no special types are present. We accept and
    // ignore it.
    use axum::body::Body;
    use axum::http::{Method, Request, header};

    use super::common::seed_entry;
    use super::rpc_common::RPC_BASE;

    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-rpc-meta",
        "fldr-rpc-meta",
        "entry-rpc-meta",
        "scn-rpc-meta",
    )
    .await;

    let body = json!({
        "json": { "id": "entry-rpc-meta" },
        "meta": [],
    });
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("{RPC_BASE}/cacheEntries/get"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-api-key", KEY)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "meta array must not break parse");
    assert_eq!(body["json"]["id"], json!("entry-rpc-meta"));
}
