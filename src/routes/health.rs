//! Health check endpoint.

use axum::Json;
use serde_json::{Value, json};

/// Handler for `GET /health`. Always responds 200 OK with `{"ok": true}`.
///
/// Intentionally infallible — this endpoint's job is to let load balancers,
/// container orchestrators, and humans confirm the process is up and serving.
/// Deeper probes (DB reachable, storage reachable) belong on a separate
/// readiness endpoint if/when we grow one.
pub async fn handler() -> Json<Value> {
    Json(json!({ "ok": true }))
}
