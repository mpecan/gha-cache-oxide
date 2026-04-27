//! Management REST API (issue #19).
//!
//! Operator-facing routes nested under `/management`:
//!
//! - `GET    /cache-entries`      — paginated list, filterable by `scope` / `repo_id`
//! - `DELETE /cache-entries/{id}` — deletes the entry, its storage location, and the folder
//! - `GET    /storage-locations`  — paginated list
//! - `POST   /cleanup/trigger`    — runs one full cleanup pass; returns the report body
//!
//! Every route is gated by [`auth::require_management_key`]:
//! - `MANAGEMENT_API_KEY` env var unset → `501 Not Implemented`
//! - Missing / malformed `Authorization: Bearer <key>` → `401 Unauthorized`
//! - Wrong key → `401 Unauthorized`
//!
//! # Deviation from upstream
//!
//! Upstream (`lib/api/*.ts`) exposes the same surface via oRPC under
//! `/management-api`, gated by an `x-api-key` header. This port:
//!
//! - speaks plain REST/JSON (no oRPC dependency, callable from `curl`),
//! - uses `Authorization: Bearer <KEY>` per the issue spec.
//!
//! See `README.md` § "Management API" for the operator-facing
//! description of the divergence.

mod auth;
mod cache_entries;
mod cleanup;
mod pagination;
mod storage_locations;

use axum::Router;
use axum::middleware as axum_mw;
use axum::routing::{delete, get, post};

use crate::state::AppState;

/// Builds the management sub-router. The caller `nest`s this under
/// `/management` in `build_app`.
pub(crate) fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/cache-entries", get(cache_entries::list))
        .route("/cache-entries/{id}", delete(cache_entries::delete))
        .route("/storage-locations", get(storage_locations::list))
        .route("/cleanup/trigger", post(cleanup::trigger))
        .layer(axum_mw::from_fn_with_state(
            state,
            auth::require_management_key,
        ))
}
