//! Forgejo runner cache dialect (GitHub Actions cache API v1,
//! `/_apis/artifactcache/*`).
//!
//! The Forgejo runner routes cache traffic to an external server only
//! through its v1 cache proxy (`cache.external_server`); its v2 twirp
//! path points at Forgejo itself, which has no cache service. So this
//! surface exists to let oxide back a fleet of Forgejo runners. It is
//! mounted only when `FORGEJO_CACHE_SECRET` is set, and leaves the v2
//! surface untouched.
//!
//! **Specification:** `forgejo-runner` v13.2.0,
//! `act/artifactcache/{handler,caches,mac,storage}.go` and
//! `act/cacheproxy/handler.go` — `forgejo-runner cache-server` serves
//! exactly this surface from a local directory. Upstream
//! `github-actions-cache-server` dropped v1 in v9.0.0, so act is the
//! reference here, not upstream.
//!
//! # Model mapping
//!
//! | act                         | oxide                                  |
//! |-----------------------------|----------------------------------------|
//! | `Cache.Repo`                | `repoId` = `"forgejo:" + repo`         |
//! | `Cache.WriteIsolationKey`   | `scope` (`""` when absent)             |
//! | reserved, incomplete cache  | `uploads` row; `cacheId` = upload id   |
//! | complete cache              | `cache_entries` row + storage location |
//! | temp file at offset         | `<folder>/chunks/<offset:016x>` object |
//!
//! # Deviations from act
//!
//! - A repository mismatch on `caches/:id` / `artifacts/:id` returns
//!   **404**. act's `readCache` surfaces it as a fatal 500 that also
//!   terminates the runner daemon.
//! - `PATCH` / `POST caches/:id` on an already-committed id returns
//!   **404** (not act's 400 "already complete"): committing consumes the
//!   `uploads` row.
//! - Commit rejects gaps / overlaps between chunks (400) and a size
//!   mismatch (400, act: 500). act concatenates whatever it has.
//! - `artifacts/:id` ids are oxide's UUID cache-entry ids. The id is
//!   opaque to the proxy and to `@actions/cache` (it only follows
//!   `archiveLocation`); `cacheId` from reserve stays numeric and
//!   non-zero (the client treats a falsy id as a failed reserve).
//! - Empty keys in `?keys=` are ignored. act would turn one into a
//!   match-anything prefix.
//! - GC follows oxide's own cleanup tasks, not act's 7d/30d policy.

mod auth;
mod handlers;

pub use auth::compute_mac;

use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::middleware as axum_mw;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::json;

use crate::state::AppState;

/// Path the router must be nested under — hard-coded by
/// `@actions/cache` v1 and the Forgejo cache proxy.
pub const BASE_PATH: &str = "/_apis/artifactcache";

/// Builds the v1 sub-router with MAC validation applied to every route.
pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/cache", get(handlers::find))
        .route("/caches", post(handlers::reserve))
        .route(
            "/caches/{id}",
            post(handlers::commit).patch(handlers::upload),
        )
        .route("/artifacts/{id}", get(handlers::get_artifact))
        .route("/clean", post(handlers::clean))
        .layer(axum_mw::from_fn_with_state(
            state,
            auth::require_forgejo_mac,
        ))
}

/// act's error body: `{"error": "<message>"}`.
pub(crate) fn json_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// act's empty success body: `{}`.
pub(crate) fn json_empty() -> Response {
    (StatusCode::OK, Json(json!({}))).into_response()
}
