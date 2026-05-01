//! Request and success envelope types for the oRPC wire format.
//!
//! See the crate-level docs for the full wire-format description.

use axum::Json;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Inbound request envelope: `{"json": <T>, "meta"?: <ignored>}`.
///
/// The `meta` field carries JS-special-type encodings in the
/// upstream client (e.g. for `Date`, `BigInt`, `Map`). This crate
/// doesn't implement that round-trip — `meta` is parsed permissively
/// (any JSON `Value`) and ignored. Callers that need typed inputs
/// declare them in `T`.
#[derive(Debug, Deserialize)]
pub struct RpcRequest<T> {
    /// The actual procedure input.
    pub json: T,
    /// Type-hint metadata from the client; tolerated but unused.
    #[serde(default)]
    pub meta: Option<Value>,
}

/// Outbound success envelope: `{"json": <T>}`.
///
/// Internal — callers use [`ok`] (or [`crate::RpcErr`] for failures)
/// to assemble responses.
#[derive(Debug, Serialize)]
pub struct RpcSuccess<T>
where
    T: Serialize,
{
    pub json: T,
}

/// Builds a `200 OK` axum response with the success envelope shape.
///
/// ```ignore
/// use orpc_server::ok;
/// let resp = ok(serde_json::json!({ "id": "abc" }));
/// // → HTTP 200, body: `{"json": {"id": "abc"}}`
/// ```
pub fn ok<T: Serialize>(value: T) -> Response {
    Json(RpcSuccess { json: value }).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{RpcRequest, RpcSuccess};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct GetInput {
        id: String,
    }

    #[test]
    fn rpc_request_parses_json_and_ignores_meta() {
        let body = json!({ "json": { "id": "abc" }, "meta": [["Date", 0]] });
        let parsed: RpcRequest<GetInput> = serde_json::from_value(body).unwrap();
        assert_eq!(
            parsed.json,
            GetInput {
                id: "abc".to_string()
            }
        );
        assert_eq!(parsed.meta, Some(json!([["Date", 0]])));
    }

    #[test]
    fn rpc_request_meta_default_when_absent() {
        let body = json!({ "json": { "id": "x" } });
        let parsed: RpcRequest<GetInput> = serde_json::from_value(body).unwrap();
        assert_eq!(parsed.meta, None);
    }

    #[test]
    fn rpc_success_envelope_serialises_to_json_field() {
        let env = RpcSuccess {
            json: GetInput {
                id: "y".to_string(),
            },
        };
        let serialised = serde_json::to_value(&env).unwrap();
        assert_eq!(serialised, json!({ "json": { "id": "y" } }));
    }
}
