//! `GET /management/cache-entries` and `DELETE /management/cache-entries/{id}`.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};

use crate::db::entities::CacheEntry;
use crate::routes::errors::{internal_error, not_found};
use crate::state::AppState;

use super::pagination::{Page, PageQuery};

#[derive(Debug, Deserialize)]
pub(super) struct ListQuery {
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default, rename = "repoId")]
    pub repo_id: Option<String>,
    #[serde(default)]
    pub page: Option<u32>,
    #[serde(default, rename = "itemsPerPage")]
    pub items_per_page: Option<u32>,
}

#[derive(Debug, Serialize)]
struct ListBody {
    total: i64,
    items: Vec<CacheEntry>,
    page: u32,
    #[serde(rename = "itemsPerPage")]
    items_per_page: u32,
}

/// `GET /management/cache-entries` — paginated list.
pub(super) async fn list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Response {
    let pq = PageQuery {
        page: query.page,
        items_per_page: query.items_per_page,
    };
    let Page {
        page,
        items_per_page,
        limit,
        offset,
    } = pq.resolve();

    let scope = query.scope.as_deref();
    let repo_id = query.repo_id.as_deref();

    let items = match state
        .db
        .list_cache_entries(scope, repo_id, limit, offset)
        .await
    {
        Ok(v) => v,
        Err(e) => return internal_error(&e.to_string()),
    };
    let total = match state.db.count_cache_entries(scope, repo_id).await {
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

/// `DELETE /management/cache-entries/{id}` — deletes the entry, its
/// backing storage location row, and the folder on the storage adapter.
///
/// The FK `cache_entries.locationId REFERENCES storage_locations(id) ON DELETE CASCADE`
/// (`migrations/sqlite/0001_initial_schema.sql:25`) handles the
/// `cache_entries` row removal when we delete the location, so this
/// handler issues a single `DELETE` against `storage_locations` then
/// asks the adapter to drop the folder.
///
/// Storage `delete_folder` failures are logged at `warn` but **don't
/// fail the request**: the DB transaction has already committed, the
/// adapter call is best-effort cleanup, and the operator can re-run
/// `POST /management/cleanup/trigger` to retry the orphan-folder reap
/// (cleanup:locations does the same `delete_folder` call).
pub(super) async fn delete(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let location = match state.db.find_location_for_entry(&id).await {
        Ok(Some(l)) => l,
        Ok(None) => return not_found("Cache entry not found"),
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
            "management delete: storage folder removal failed; row already deleted",
        );
    }

    StatusCode::NO_CONTENT.into_response()
}
