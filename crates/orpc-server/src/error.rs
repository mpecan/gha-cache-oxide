//! oRPC error envelope + status-code mapping.
//!
//! Mirrors `@orpc/client`'s `ORPCErrorJSON` shape exactly:
//!
//! ```json
//! {
//!   "json": {
//!     "defined": false,
//!     "code": "NOT_FOUND",
//!     "status": 404,
//!     "message": "..."
//!   }
//! }
//! ```
//!
//! The HTTP status of the response equals `body.status` so clients
//! can decode errors from either side.
//!
//! # Codes
//!
//! [`RpcErr`] exposes constructor methods for the common-case error
//! codes from upstream's `COMMON_ORPC_ERROR_DEFS`
//! (`@orpc/client/dist/index.d.ts:20-97`):
//!
//! | constructor              | code                      | HTTP |
//! |--------------------------|---------------------------|------|
//! | [`RpcErr::bad_request`]  | `BAD_REQUEST`             | 400  |
//! | [`RpcErr::unauthorized`] | `UNAUTHORIZED`            | 401  |
//! | [`RpcErr::forbidden`]    | `FORBIDDEN`               | 403  |
//! | [`RpcErr::not_found`]    | `NOT_FOUND`               | 404  |
//! | [`RpcErr::conflict`]     | `CONFLICT`                | 409  |
//! | [`RpcErr::internal`]     | `INTERNAL_SERVER_ERROR`   | 500  |
//! | [`RpcErr::not_implemented`] | `NOT_IMPLEMENTED`      | 501  |
//! | [`RpcErr::service_unavailable`] | `SERVICE_UNAVAILABLE` | 503 |

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::envelope::RpcSuccess;

/// Outbound error body — the inner payload of `{"json": <body>}`.
///
/// Matches `@orpc/client`'s `ORPCErrorJSON` shape. We never declare
/// typed errors via `.errors({...})` upstream-side, so `defined` is
/// always `false`.
#[derive(Debug, Serialize)]
pub struct RpcErrorBody {
    /// `false` for unhandled / framework-level errors. Upstream sets
    /// this to `true` only for errors declared via `.errors({...})`.
    pub defined: bool,
    /// Upstream-compat code from `COMMON_ORPC_ERROR_DEFS`.
    pub code: &'static str,
    /// HTTP status that mirrors the body's status — repeated so
    /// clients can decode errors from the body alone.
    pub status: u16,
    /// Human-readable message.
    pub message: String,
}

/// An oRPC-shaped error response. Build with one of the static
/// constructors (e.g. [`RpcErr::not_found`]) and convert via
/// [`IntoResponse`].
///
/// Convention: pass a short human-readable `message` to each
/// constructor. The `internal` constructor additionally logs the
/// message at `error!` and replaces the body with a generic
/// "Internal Server Error" so diagnostics don't leak.
#[derive(Debug, Clone)]
pub struct RpcErr {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl RpcErr {
    /// `BAD_REQUEST` (HTTP 400).
    pub fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "BAD_REQUEST",
            message: message.to_string(),
        }
    }

    /// `UNAUTHORIZED` (HTTP 401).
    pub fn unauthorized(message: &str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "UNAUTHORIZED",
            message: message.to_string(),
        }
    }

    /// `FORBIDDEN` (HTTP 403).
    pub fn forbidden(message: &str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "FORBIDDEN",
            message: message.to_string(),
        }
    }

    /// `NOT_FOUND` (HTTP 404).
    pub fn not_found(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: message.to_string(),
        }
    }

    /// `CONFLICT` (HTTP 409).
    pub fn conflict(message: &str) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "CONFLICT",
            message: message.to_string(),
        }
    }

    /// `INTERNAL_SERVER_ERROR` (HTTP 500). Logs the supplied message
    /// at `tracing::error!` and replaces the client-facing message
    /// with a generic string so internals don't leak.
    pub fn internal(message: &str) -> Self {
        tracing::error!(message, "rpc internal error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL_SERVER_ERROR",
            message: "Internal Server Error".to_string(),
        }
    }

    /// `NOT_IMPLEMENTED` (HTTP 501).
    pub fn not_implemented(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            code: "NOT_IMPLEMENTED",
            message: message.to_string(),
        }
    }

    /// `SERVICE_UNAVAILABLE` (HTTP 503). Useful for "the service is
    /// configured-off" responses (matches upstream's
    /// `_rpc.ts` "Management API is disabled" pattern).
    pub fn service_unavailable(message: &str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "SERVICE_UNAVAILABLE",
            message: message.to_string(),
        }
    }

    /// Returns the HTTP status this error will produce.
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns the upstream-compat error code.
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl IntoResponse for RpcErr {
    fn into_response(self) -> Response {
        let body = RpcSuccess {
            json: RpcErrorBody {
                defined: false,
                code: self.code,
                status: self.status.as_u16(),
                message: self.message,
            },
        };
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::RpcErr;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use serde_json::{Value, json};

    /// Helper: turns an `IntoResponse` into `(status, parsed-body)`.
    async fn collect(resp: axum::response::Response) -> (StatusCode, Value) {
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        (status, body)
    }

    #[tokio::test]
    async fn not_found_response_has_orpc_envelope_and_status() {
        let (status, body) = collect(RpcErr::not_found("missing").into_response()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            body,
            json!({
                "json": {
                    "defined": false,
                    "code": "NOT_FOUND",
                    "status": 404,
                    "message": "missing",
                }
            })
        );
    }

    #[tokio::test]
    async fn bad_request_uses_400_and_code() {
        let (status, body) = collect(RpcErr::bad_request("nope").into_response()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["json"]["code"], json!("BAD_REQUEST"));
        assert_eq!(body["json"]["status"], json!(400));
    }

    #[tokio::test]
    async fn service_unavailable_uses_503_and_code() {
        let (status, body) = collect(RpcErr::service_unavailable("disabled").into_response()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["json"]["code"], json!("SERVICE_UNAVAILABLE"));
    }

    #[tokio::test]
    async fn internal_redacts_message() {
        let (status, body) = collect(RpcErr::internal("stack trace").into_response()).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        // Message is redacted — caller's diagnostic doesn't leak.
        assert_eq!(body["json"]["message"], json!("Internal Server Error"));
    }

    #[test]
    fn accessors() {
        let err = RpcErr::not_found("x");
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "NOT_FOUND");
    }
}
