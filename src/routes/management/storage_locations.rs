//! `GET /management/storage-locations` — paginated list.

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;

use crate::db::entities::StorageLocation;
use crate::routes::errors::internal_error;
use crate::state::AppState;

use super::pagination::{Page, PageQuery};

#[derive(Debug, Serialize)]
struct ListBody {
    total: i64,
    items: Vec<StorageLocation>,
    page: u32,
    #[serde(rename = "itemsPerPage")]
    items_per_page: u32,
}

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
