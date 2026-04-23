//! JSON error-response helpers shared across route modules.
//!
//! Body shape is `{statusCode, message}` — the same shape upstream's
//! h3 `createError({ message: ... })` emits for auth, scope, not-found
//! and internal failures. The Twirp body-parse variant
//! (`{statusCode, statusMessage}`) lives in
//! [`crate::routes::twirp::bad_request_body`] because it's specific to
//! zod-rejection bodies and nothing else uses it.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Builds a response with the `{statusCode, message}` body.
pub fn error_response(status: StatusCode, message: &str) -> Response {
    let body = Json(json!({
        "statusCode": status.as_u16(),
        "message": message,
    }));
    (status, body).into_response()
}

pub fn bad_request(msg: &str) -> Response {
    error_response(StatusCode::BAD_REQUEST, msg)
}

pub fn forbidden(msg: &str) -> Response {
    error_response(StatusCode::FORBIDDEN, msg)
}

pub fn not_found(msg: &str) -> Response {
    error_response(StatusCode::NOT_FOUND, msg)
}

/// 500 Internal Server Error. The caller's `msg` is logged as an error
/// event; the client sees only a generic "Internal error" string so we
/// don't leak diagnostics.
pub fn internal_error(msg: &str) -> Response {
    tracing::error!(message = msg, "route internal error");
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
}
