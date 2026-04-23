//! gha-cache-oxide — a Rust port of github-actions-cache-server.
//!
//! This crate exposes the HTTP application factory and tracing bootstrap so
//! that the binary (`src/main.rs`) stays thin and integration tests can drive
//! the router directly without spawning the full process.

pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod routes;
pub mod state;
pub mod storage;

use axum::Router;
use axum::routing::get;
use tower_http::trace::TraceLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::{SubscriberInitExt, TryInitError};
use tracing_subscriber::{EnvFilter, Registry, fmt};

use crate::config::LogFormat;
use crate::state::AppState;

/// Builds the HTTP router. Call this from both the binary and tests so the
/// routing surface cannot drift between them.
///
/// Wires the tower-http `TraceLayer` at the top so every request emits a span
/// with method/uri; the layer's default response hook logs status and latency.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(routes::health::handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Initialises the global tracing subscriber.
///
/// The `RUST_LOG` env var (consumed by `EnvFilter`) overrides the default
/// filter when set. Format is selected from the resolved config rather than
/// re-reading the env, so a misconfigured `LOG_FORMAT` is caught in the
/// config layer instead of silently falling back here.
///
/// # Errors
/// Returns `TryInitError` when a global subscriber has already been installed
/// (usually because this was called twice).
pub fn init_tracing(format: LogFormat) -> Result<(), TryInitError> {
    // Write to stderr directly — the subscriber isn't installed yet, so
    // tracing::warn! would be swallowed. We fall back to a safe default
    // filter, but surface the reason loudly per the "never silently fall
    // back" rule.
    let filter = match EnvFilter::try_from_default_env() {
        Ok(f) => f,
        Err(e) if std::env::var_os("RUST_LOG").is_some() => {
            eprintln!(
                "warning: RUST_LOG is set but could not be parsed ({e}); \
                 falling back to default filter",
            );
            EnvFilter::new("gha_cache_oxide=info,tower_http=info")
        }
        Err(_) => EnvFilter::new("gha_cache_oxide=info,tower_http=info"),
    };
    match format {
        LogFormat::Json => Registry::default()
            .with(filter)
            .with(fmt::layer().json())
            .try_init(),
        LogFormat::Text => Registry::default()
            .with(filter)
            .with(fmt::layer())
            .try_init(),
    }
}
