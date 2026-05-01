//! Shared helpers for the oRPC `_rpc` integration tests
//! (`tests/management/rpc_*.rs`).
//!
//! Mirrors what `@orpc/client` v1.x's `RPCLink` emits — `POST` to
//! `/_rpc/<group>/<procedure>` with body `{"json": <input>}` and the
//! `X-Api-Key` header — so the test files can stay focused on the
//! per-procedure happy / error / edge cases.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde_json::{Value, json};

pub const RPC_BASE: &str = "/management-api/_rpc";

/// Builds a `POST` against an `/_rpc/<group>/<procedure>` URL with
/// the `X-Api-Key` header (when supplied) and the orpc
/// `{"json": <input>}` envelope. Takes `input` by value so call
/// sites can pass `json!(...)` inline.
#[allow(clippy::needless_pass_by_value)]
pub fn rpc_post(procedure: &str, key: Option<&str>, input: Value) -> Request<Body> {
    let body = json!({ "json": input });
    let mut b = Request::builder()
        .method(Method::POST)
        .uri(format!("{RPC_BASE}/{procedure}"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(k) = key {
        b = b.header("x-api-key", k);
    }
    b.body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

/// Decodes a response body as JSON `Value` along with the status.
pub async fn body_json(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}
