//! `POST /management/cleanup/trigger` — runs `tasks::cleanup::run_all`
//! once on demand and returns the resulting `CleanupReport` as JSON.
//!
//! Useful for operators who want a one-shot reap (e.g. after a manual
//! delete spree) rather than waiting for the hourly scheduler tick. The
//! cleanup pass is the same one issue #18 spawns at startup.

use axum::extract::State;
use axum::response::{IntoResponse, Json, Response};

use crate::db::id::now_ms;
use crate::state::AppState;
use crate::tasks::cleanup;

pub(super) async fn trigger(State(state): State<AppState>) -> Response {
    let report = cleanup::run_all(
        &*state.db,
        &*state.storage,
        now_ms(),
        state.config.cache_cleanup_older_than_days,
    )
    .await;
    tracing::info!(?report, "management cleanup trigger complete");
    Json(report).into_response()
}
