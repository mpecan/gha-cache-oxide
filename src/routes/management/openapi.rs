//! `OpenAPI` 3.1 spec for the management API (issue #77 part 1).
//!
//! Assembled at compile time from `#[utoipa::path]` annotations on the
//! handlers in this directory. Served at
//! `GET /management/_docs/spec.json` and snapshotted to
//! `docs/openapi.json` (a no-drift test in `tests/management/openapi.rs`
//! pins them together).

// utoipa's `OpenApi` derive expands to code that uses `for_each` in a
// way clippy considers "needless". Not actionable from here; allow it
// for the whole file rather than peppering individual lines.
#![allow(clippy::needless_for_each)]

use axum::Json;
use axum::response::{IntoResponse, Response};
use utoipa::OpenApi;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

use super::cache_entries;
use super::cleanup;
use super::storage_locations;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "gha-cache-oxide management API",
        description = "Operator-facing REST surface for the gha-cache-oxide cache server. \
                       All endpoints require an `Authorization: Bearer <MANAGEMENT_API_KEY>` header.",
        version = env!("CARGO_PKG_VERSION"),
    ),
    paths(
        cache_entries::list,
        cache_entries::get_one,
        cache_entries::match_endpoint,
        cache_entries::delete,
        cache_entries::delete_many,
        storage_locations::list,
        storage_locations::get_one,
        storage_locations::delete_one,
        cleanup::trigger,
    ),
    components(schemas(
        crate::db::entities::CacheEntry,
        crate::db::entities::StorageLocation,
        crate::db::entities::MatchType,
        crate::routes::errors::ErrorBody,
        crate::tasks::cleanup::CleanupReport,
        cache_entries::ListBody,
        cache_entries::MatchResponse,
        cache_entries::DeleteManyResponse,
        storage_locations::ListBody,
    )),
    tags(
        (name = "cache-entries", description = "Cache entry inspection + deletion"),
        (name = "storage-locations", description = "Storage location inspection + orphan removal"),
        (name = "cleanup", description = "Background cleanup orchestration"),
    ),
    servers((url = "/management", description = "Management API root")),
)]
pub(super) struct ManagementApiDoc;

impl ManagementApiDoc {
    /// Builds the spec, attaching the `bearer` security scheme by hand
    /// (utoipa's `Modify` trait is the more verbose alternative; this
    /// is fine for one scheme).
    pub(super) fn build() -> utoipa::openapi::OpenApi {
        let mut doc = <Self as OpenApi>::openapi();
        let bearer = SecurityScheme::Http(
            HttpBuilder::new()
                .scheme(HttpAuthScheme::Bearer)
                .description(Some(
                    "Bearer token equal to the server's `MANAGEMENT_API_KEY`.",
                ))
                .build(),
        );
        if let Some(components) = doc.components.as_mut() {
            components.add_security_scheme("bearer", bearer);
        } else {
            let mut components = utoipa::openapi::Components::new();
            components.add_security_scheme("bearer", bearer);
            doc.components = Some(components);
        }
        doc
    }
}

/// `GET /management/_docs/spec.json` — returns the spec as JSON.
///
/// Sits behind the same Bearer-auth middleware the rest of the
/// management surface uses (see `super::router`); operators who want
/// the spec without authenticating can read the committed snapshot
/// at `docs/openapi.json`.
pub(super) async fn spec_json() -> Response {
    Json(ManagementApiDoc::build()).into_response()
}
