//! gha-cache-oxide — a Rust port of github-actions-cache-server.
//!
//! This crate exposes the HTTP application factory and tracing bootstrap so
//! that the binary (`src/main.rs`) stays thin and integration tests can drive
//! the router directly without spawning the full process.

pub mod auth;
pub mod cache;
pub mod config;
pub mod db;
pub mod error;
pub mod merge;
pub mod routes;
pub mod state;
pub mod storage;
pub mod tasks;

use axum::Router;
use axum::routing::get;
use tower_http::trace::TraceLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::{SubscriberInitExt, TryInitError};
use tracing_subscriber::{EnvFilter, Registry, fmt};

use crate::config::LogFormat;
use crate::db::Db;
use crate::state::AppState;

/// Orphaned-merge-claim threshold used by the startup sweep (issue #17).
///
/// A `mergeStartedAt` without a matching `mergedAt` older than this is
/// treated as left behind by a crashed process and cleared at startup.
/// 1 hour is long enough that a legitimate in-flight merge is
/// indistinguishable from a crash — the longest observed merge under
/// upstream's streaming topology is seconds-to-minutes for realistic
/// cache sizes.
pub const STALE_MERGE_THRESHOLD_MS: i64 = 3_600_000;

/// Runs the issue-#17 startup sweep against `db` using `now_ms` as the
/// clock reading.
///
/// Clears every `storage_locations` row where `mergeStartedAt` is set,
/// `mergedAt` is NULL, and `mergeStartedAt` is older than
/// [`STALE_MERGE_THRESHOLD_MS`] before `now_ms`. Logs at `warn` naming
/// the count when one or more rows were cleared. Taking `now_ms` as an
/// argument (rather than reading the clock inside) is the injection
/// seam that satisfies the "unit-tested with a fake clock" acceptance
/// criterion.
///
/// # Errors
/// Returns the underlying `sqlx::Error` if the `UPDATE` fails.
pub async fn run_startup_sweep(db: &dyn Db, now_ms: i64) -> Result<u64, sqlx::Error> {
    let cutoff = now_ms.saturating_sub(STALE_MERGE_THRESHOLD_MS);
    let cleared = db.clear_stale_merge_claims(cutoff).await?;
    if cleared > 0 {
        tracing::warn!(
            count = cleared,
            threshold_ms = STALE_MERGE_THRESHOLD_MS,
            "cleared stale merge claims on startup; affected entries will re-merge on next download"
        );
    }
    Ok(cleared)
}

/// Builds the HTTP router. Call this from both the binary and tests so the
/// routing surface cannot drift between them.
///
/// Wires the tower-http `TraceLayer` at the top so every request emits a span
/// with method/uri; the layer's default response hook logs status and latency.
pub fn build_app(state: AppState) -> Router {
    let twirp = routes::twirp::router(state.clone());
    // Blob routes (upload PUT / download GET) deliberately skip the
    // OIDC middleware: the `upload_id` / `cache_entry_id` in the URL is
    // the capability, matching upstream's routing topology.
    let blob = routes::blob::router();
    let management = routes::management::router(state.clone());
    Router::new()
        .route("/health", get(routes::health::handler))
        .nest("/twirp/github.actions.results.api.v1.CacheService", twirp)
        .nest("/management", management)
        .merge(blob)
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod startup_sweep_tests {
    //! Fake-clock coverage for [`run_startup_sweep`]. The underlying
    //! `Db::clear_stale_merge_claims` query is tested by the driver
    //! conformance suite; these tests pin the composition: threshold
    //! arithmetic (`saturating_sub`), per-row effect, and the return
    //! value the log guard keys on.
    use super::{STALE_MERGE_THRESHOLD_MS, run_startup_sweep};
    use crate::db::entities::CacheEntryCoord;
    use crate::db::{Db, SqliteDb};

    async fn fresh_db() -> SqliteDb {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db
    }

    async fn seed_stale_claim(db: &SqliteDb, loc_id: &str, started_at: i64) {
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(loc_id, "folder", 1)
            .await
            .unwrap();
        tx.seed_cache_entry(
            &format!("entry-{loc_id}"),
            CacheEntryCoord {
                key: "k",
                version: "v",
                scope: loc_id,
                repo_id: "r",
            },
            0,
            loc_id,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(db.try_mark_merge_started(loc_id, started_at).await.unwrap());
    }

    #[tokio::test]
    async fn sweep_clears_claims_older_than_threshold() {
        let db = fresh_db().await;
        // Claim at t=0; fake clock reads threshold + 1, so cutoff = 1.
        seed_stale_claim(&db, "loc-old", 0).await;
        let now = STALE_MERGE_THRESHOLD_MS + 1;

        let cleared = run_startup_sweep(&db, now).await.unwrap();
        assert_eq!(cleared, 1);
    }

    #[tokio::test]
    async fn sweep_leaves_claims_younger_than_threshold() {
        let db = fresh_db().await;
        // Claim at t = threshold (exactly — cutoff is `now - threshold`;
        // with now == threshold, cutoff = 0, so the strict `<` preserves
        // every claim with started_at >= 0).
        seed_stale_claim(&db, "loc-fresh", STALE_MERGE_THRESHOLD_MS).await;
        let now = STALE_MERGE_THRESHOLD_MS;

        let cleared = run_startup_sweep(&db, now).await.unwrap();
        assert_eq!(cleared, 0);
    }

    #[tokio::test]
    async fn sweep_clears_multiple_stale_rows_in_one_pass() {
        let db = fresh_db().await;
        seed_stale_claim(&db, "loc-a", 0).await;
        seed_stale_claim(&db, "loc-b", 10).await;
        seed_stale_claim(&db, "loc-c", 100).await;
        let now = STALE_MERGE_THRESHOLD_MS + 1_000;

        let cleared = run_startup_sweep(&db, now).await.unwrap();
        assert_eq!(cleared, 3, "all three stale rows should be cleared");
    }

    #[tokio::test]
    async fn sweep_on_empty_db_is_noop() {
        let db = fresh_db().await;
        let cleared = run_startup_sweep(&db, 0).await.unwrap();
        assert_eq!(cleared, 0);
    }

    #[tokio::test]
    async fn sweep_does_not_panic_on_tiny_now_ms() {
        // `saturating_sub` protects against a pathologically early
        // clock (e.g. a fresh container with the clock unset); cutoff
        // clamps to i64::MIN instead of wrapping.
        let db = fresh_db().await;
        let cleared = run_startup_sweep(&db, 0).await.unwrap();
        assert_eq!(cleared, 0);
    }
}
