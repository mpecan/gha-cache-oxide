//! `POST /management/cleanup/trigger` — runs `tasks::cleanup::run_all`
//! once on demand and returns the resulting `CleanupReport` as JSON.
//!
//! Useful for operators who want a one-shot reap (e.g. after a manual
//! delete spree) rather than waiting for the hourly scheduler tick. The
//! cleanup pass is the same one issue #18 spawns at startup.

use axum::extract::State;
use axum::response::{IntoResponse, Json, Response};

use crate::db::id::now_ms;
use crate::routes::errors::ErrorBody;
use crate::state::AppState;
use crate::tasks::cleanup;
use crate::tasks::cleanup::CleanupReport;

#[utoipa::path(
    post,
    path = "/cleanup/trigger",
    tag = "cleanup",
    summary = "Run cleanup pass",
    description = "Runs the same cleanup pass the background scheduler runs on its cadence; returns the per-task counts.",
    responses(
        (status = 200, description = "Per-task cleanup counts", body = CleanupReport),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorBody),
        (status = 501, description = "Management API disabled (MANAGEMENT_API_KEY is not set)", body = ErrorBody),
    ),
    security(("bearer" = []))
)]
pub(super) async fn trigger(State(state): State<AppState>) -> Response {
    let report = cleanup::run_all(
        &*state.db,
        &*state.storage,
        now_ms(),
        cleanup::EntryRetention::from_config(&state.config),
    )
    .await;
    tracing::info!(?report, "management cleanup trigger complete");
    Json(report).into_response()
}
