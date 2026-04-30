//! Management REST API (issues #19, #70).
//!
//! Operator-facing routes nested under `/management`:
//!
//! - `GET    /cache-entries`         — paginated list, filterable by `scope` / `repo_id`
//! - `GET    /cache-entries/{id}`    — single entry by id (#70)
//! - `GET    /cache-entries/match`   — run the `match_cache_entry` algorithm (#70)
//! - `DELETE /cache-entries`         — bulk delete by `key`/`version`/`scope`/`repoId` filter (#70)
//! - `DELETE /cache-entries/{id}`    — delete entry + storage location + folder
//! - `GET    /storage-locations`     — paginated list
//! - `GET    /storage-locations/{id}` — single location by id (#70)
//! - `DELETE /storage-locations/{id}` — explicit orphan removal (#70)
//! - `POST   /cleanup/trigger`       — runs one full cleanup pass; returns the report body
//!
//! Every route is gated by the crate-internal `auth::require_management_key`:
//! - `MANAGEMENT_API_KEY` env var unset → `501 Not Implemented`
//! - Missing / malformed `Authorization: Bearer <key>` → `401 Unauthorized`
//! - Wrong key → `401 Unauthorized`
//!
//! # Routing precedence note
//!
//! axum 0.8 (matchit-based) prefers literal segments over `{id}`
//! parameters, so `GET /cache-entries/match` always reaches the
//! `match_endpoint` handler — even if a `cache_entries` row's `id`
//! field literally equalled "match". The integration test
//! `match_endpoint_wins_over_get_one_routing` in `tests/management.rs`
//! pins this invariant.
//!
//! # Deviation from upstream
//!
//! Upstream (`lib/api/*.ts`) exposes the same surface via oRPC under
//! `/management-api`, gated by an `x-api-key` header. This port:
//!
//! - speaks plain REST/JSON (no oRPC dependency, callable from `curl`),
//! - uses `Authorization: Bearer <KEY>` per the issue spec.
//!
//! `DELETE /cache-entries` (bulk) **rejects an empty filter** with
//! `400 Bad Request` — upstream would silently delete every row. It
//! also honours the `repoId` filter, which upstream's `deleteMany`
//! drops on the floor (`lib/api/cache-entries.ts:163-168`).
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
use axum::routing::{get, post};

use crate::state::AppState;

/// Builds the management sub-router. The caller `nest`s this under
/// `/management` in `build_app`.
pub(crate) fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route(
            "/cache-entries",
            get(cache_entries::list).delete(cache_entries::delete_many),
        )
        .route("/cache-entries/match", get(cache_entries::match_endpoint))
        .route(
            "/cache-entries/{id}",
            get(cache_entries::get_one).delete(cache_entries::delete),
        )
        .route("/storage-locations", get(storage_locations::list))
        .route(
            "/storage-locations/{id}",
            get(storage_locations::get_one).delete(storage_locations::delete_one),
        )
        .route("/cleanup/trigger", post(cleanup::trigger))
        .layer(axum_mw::from_fn_with_state(
            state,
            auth::require_management_key,
        ))
}
