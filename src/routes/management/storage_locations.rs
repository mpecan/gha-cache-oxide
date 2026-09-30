//! `/management/storage-locations` routes — list, get-one, delete-one.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;
use utoipa::ToSchema;

use crate::db::entities::StorageLocation;
use crate::routes::errors::{ErrorBody, internal_error, not_found};
use crate::state::AppState;

use super::pagination::{Page, PageQuery};

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ListBody {
    total: i64,
    items: Vec<StorageLocation>,
    page: u32,
    #[serde(rename = "itemsPerPage")]
    items_per_page: u32,
}

#[utoipa::path(
    get,
    path = "/storage-locations",
    tag = "storage-locations",
    summary = "List storage locations",
    description = "Paginated list of storage_locations rows.",
    params(
        ("page" = Option<u32>, Query, description = "1-based page number (default 1)"),
        ("itemsPerPage" = Option<u32>, Query, description = "Page size (default 100, max 500)"),
    ),
    responses(
        (status = 200, description = "Page of storage locations", body = ListBody),
        (status = 401, description = "Missing or invalid Bearer token", body = ErrorBody),
        (status = 501, description = "MANAGEMENT_API_KEY env var unset", body = ErrorBody),
    ),
    security(("bearer" = []))
)]
pub(super) async fn list(
    State(state): State<AppState>,
    Query(query): Query<PageQuery>,
) -> Response {
    let Page {
        page,
        items_per_page,
        limit,
        offset,
    } = query.resolve();

    let items = match state.db.list_storage_locations(limit, offset).await {
        Ok(v) => v,
        Err(e) => return internal_error(&e.to_string()),
    };
    let total = match state.db.count_storage_locations().await {
        Ok(n) => n,
        Err(e) => return internal_error(&e.to_string()),
    };

    Json(ListBody {
        total,
        items,
        page,
        items_per_page,
    })
    .into_response()
}

/// `GET /management/storage-locations/{id}` — single-location fetch.
/// Returns `200` with the row body or `404` if missing.
#[utoipa::path(
    get,
    path = "/storage-locations/{id}",
    tag = "storage-locations",
    summary = "Get storage location",
    params(("id" = String, Path, description = "Storage location id")),
    responses(
        (status = 200, description = "The storage location", body = StorageLocation),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "No row matched", body = ErrorBody),
        (status = 501, description = "Management API disabled (MANAGEMENT_API_KEY is not set)", body = ErrorBody),
    ),
    security(("bearer" = []))
)]
pub(super) async fn get_one(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.db.find_storage_location_by_id(&id).await {
        Ok(Some(loc)) => Json(loc).into_response(),
        Ok(None) => not_found("Storage location not found"),
        Err(e) => internal_error(&e.to_string()),
    }
}

/// `DELETE /management/storage-locations/{id}` — explicit orphan removal.
///
/// Removes the `storage_locations` row, which CASCADE-deletes any
/// `cache_entries` pointing at it (matching the cache-entry DELETE
/// handler's contract), then issues a best-effort
/// `adapter.delete_folder` to remove the underlying blob folder.
///
/// Storage failures are logged at `warn` and don't fail the request —
/// the DB row is already gone, the adapter call is best-effort, and
/// `cleanup:locations` will retry the orphan-folder reap on its
/// schedule.
///
/// Returns `204 No Content` on success, `404 Not Found` if no row
/// matched the supplied id.
#[utoipa::path(
    delete,
    path = "/storage-locations/{id}",
    tag = "storage-locations",
    summary = "Delete storage location",
    description = "Removes the storage_locations row (cascading to any cache_entries pointing at it) and best-effort deletes the underlying blob folder.",
    params(("id" = String, Path, description = "Storage location id")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
        (status = 501, description = "Management API disabled (MANAGEMENT_API_KEY is not set)", body = ErrorBody),
    ),
    security(("bearer" = []))
)]
pub(super) async fn delete_one(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let location = match state.db.find_storage_location_by_id(&id).await {
        Ok(Some(l)) => l,
        Ok(None) => return not_found("Storage location not found"),
        Err(e) => return internal_error(&e.to_string()),
    };

    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return internal_error(&e.to_string()),
    };
    if let Err(e) = tx.delete_storage_location(&location.id).await {
        return internal_error(&e.to_string());
    }
    if let Err(e) = tx.commit().await {
        return internal_error(&e.to_string());
    }

    if let Err(e) = state.storage.delete_folder(&location.folder_name).await {
        tracing::warn!(
            error = %e,
            location_id = %location.id,
            folder = %location.folder_name,
            "management storage-location delete: folder removal failed; row already deleted",
        );
    }

    StatusCode::NO_CONTENT.into_response()
}
