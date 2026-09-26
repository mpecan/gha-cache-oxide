//! Shared application state carried on the axum router.
//!
//! Holds a `Db` handle, a storage adapter (trait object), a JWKS cache
//! for OIDC middleware, an `Arc`-wrapped `AppConfig`, and a shared
//! outbound `reqwest::Client`. Cloned cheaply — every field is itself
//! `Arc`-ish (including `reqwest::Client`, which is `Arc<Inner>`
//! internally).

use std::sync::Arc;

use crate::auth::JwksCache;
use crate::config::AppConfig;
use crate::db::Db;
use crate::merge::MergeTracker;
use crate::metrics::Metrics;
use crate::storage::StorageAdapter;

/// Application state. Cloned cheaply — every field is itself `Arc`-ish.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<dyn Db>,
    pub storage: Arc<dyn StorageAdapter>,
    pub jwks: Arc<JwksCache>,
    pub config: Arc<AppConfig>,
    /// Shared outbound HTTP client used by the catch-all proxy and any
    /// future outbound HTTP user. Built with `redirect = none` so the
    /// proxy can mirror upstream status codes verbatim. `reqwest::Client`
    /// is internally `Arc`-wrapped, so cloning [`AppState`] is cheap.
    pub http_client: reqwest::Client,
    /// Tracks in-flight lazy-merge tasks so graceful shutdown can
    /// await them. Shared via `Clone` (internal `Arc`). `pub` because
    /// `main.rs` clones it out to drain after `axum::serve` returns.
    pub merges: MergeTracker,
    /// Prometheus counters served at `/metrics`.
    pub metrics: Arc<Metrics>,
}

impl AppState {
    /// Constructs the state from an already-connected DB, a storage
    /// adapter wrapped for shared access, a JWKS cache, and a config.
    /// Builds the shared HTTP client internally; if `Client::builder()`
    /// fails (TLS init / DNS configuration anomaly), the failure is
    /// logged once and the call falls back to `Client::new()` — the
    /// constitution forbids silent degradation.
    #[must_use]
    pub fn new(
        db: Arc<dyn Db>,
        storage: Arc<dyn StorageAdapter>,
        jwks: Arc<JwksCache>,
        config: AppConfig,
    ) -> Self {
        Self::with_http_client(db, storage, jwks, config, default_http_client())
    }

    /// Constructs the state with a caller-provided HTTP client. Used
    /// where a test wants to inject its own (e.g. for connection-pool
    /// tuning or recording).
    #[must_use]
    pub fn with_http_client(
        db: Arc<dyn Db>,
        storage: Arc<dyn StorageAdapter>,
        jwks: Arc<JwksCache>,
        config: AppConfig,
        http_client: reqwest::Client,
    ) -> Self {
        let metrics = Arc::new(Metrics::default());
        Self {
            db,
            storage,
            jwks,
            config: Arc::new(config),
            http_client,
            merges: MergeTracker::with_metrics(metrics.clone()),
            metrics,
        }
    }
}

/// Builds the shared outbound HTTP client. `redirect = none` so the
/// catch-all proxy mirrors upstream status verbatim instead of
/// transparently following 3xx responses.
fn default_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap_or_else(|err| {
            tracing::error!(
                error = %err,
                "AppState: reqwest::Client::builder() failed; falling back to Client::new() with library defaults",
            );
            reqwest::Client::new()
        })
}
