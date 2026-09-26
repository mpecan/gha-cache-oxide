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
//!   terminates the runner daemon; DB errors likewise return 500 here
//!   without exiting.
//! - `PATCH` / `POST caches/:id` on an already-committed id returns
//!   **404** (not act's 400 "already complete"): committing consumes the
//!   `uploads` row. Of two overlapping commits of one id (a client
//!   retry), exactly one succeeds; the other gets 404. A chunk still
//!   streaming when its upload is committed or reaped is discarded and
//!   answered 404 rather than left as an orphaned object.
//! - **Commit size.** act checks the assembled size against reserve's
//!   `cacheSize` (and skips the check when that is 0, as old clients
//!   such as `actions/cache@v2` send). Oxide checks the `size` in the
//!   commit body, which every v1 client sends, and skips the check when
//!   it is absent. A mismatch is 400 (act: 500). Commit also rejects
//!   gaps / overlaps between chunks (400); act concatenates whatever
//!   temp files it has.
//! - A hit whose blob has vanished is purged and the next candidate is
//!   tried (up to three probes, then the shared scope), like the v2
//!   download path (#72); act purges and answers 204 at once.
//! - "Newest first" means most recently *committed* (`updatedAt`); act
//!   orders by reservation time. Re-committing the same
//!   key/version/isolation key replaces the entry (and deletes its old
//!   blobs immediately — a download of the old blob still streaming is
//!   cut off; act keeps superseded entries for 5 minutes).
//! - `artifacts/:id` ids are oxide's UUID cache-entry ids. The id is
//!   opaque to the proxy and to `@actions/cache` (it only follows
//!   `archiveLocation`); `cacheId` from reserve stays numeric and
//!   non-zero (the client treats a falsy id as a failed reserve). The
//!   same UUID also works on the unauthenticated v2 `/download/{id}`
//!   route — ids are capabilities there, as in upstream.
//! - Commit starts a background merge of the new entry (act has one
//!   file per cache and nothing to merge; oxide's v2 surface merges
//!   lazily on first download, like upstream). See
//!   `merge::start_background_merge`.
//! - Downloads stream without `Content-Length` and without `Range`
//!   support (act uses `http.ServeFile`). `@actions/cache` v1 only uses
//!   Range against Azure hosts, so restores are unaffected, but its
//!   client-side truncation check is skipped.
//! - `cacheKey` on an exact hit is the requested key as the client
//!   spelled it; act returns the stored, lowercased key, which makes
//!   case-sensitive clients (`actions/setup-node`: `primaryKey ===
//!   matchedKey`) treat every hit as a miss and re-upload the cache.
//!   Matching itself stays case-insensitive, as in act. A prefix hit
//!   returns the stored (lowercased) key.
//! - Empty keys in `?keys=` are ignored. act would turn one into a
//!   match-anything prefix.
//! - GC follows oxide's own cleanup tasks, not act's 7d/30d policy.
//!   Entries that are never downloaded are only expired when
//!   `CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS` is set (unset = upstream
//!   parity; 7 with `CACHE_CLEANUP_OLDER_THAN_DAYS=30` ≈ act's policy).
//!
//! Same as act, worth knowing: the MAC timestamp is minted once per job
//! on the runner and never expires, so there is no replay window but
//! also no clock-skew tolerance — oxide's clock must not lag the
//! runners'. A single chunk above 5 GiB fails on S3 at commit
//! (`CopyObject` limit); `@actions/cache` sends 32 MiB chunks.

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
        // Unknown sub-paths answer here instead of falling through to the
        // catch-all proxy (which would forward them to GitHub).
        .fallback(|| async { json_error(StatusCode::NOT_FOUND, "not found") })
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
