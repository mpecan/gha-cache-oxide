//! Binary entry point. Keep this thin — orchestration only, no logic. All
//! testable behaviour lives in `gha_cache_oxide::*`.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use gha_cache_oxide::auth::{HttpJwksFetcher, JwksCache};
use gha_cache_oxide::config::{AppConfig, DbConfig, PostgresConfig, StorageConfig};
use gha_cache_oxide::db::{Db, PostgresDb, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::{FilesystemAdapter, S3Adapter, S3Config, StorageAdapter};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = AppConfig::from_env().context("loading configuration from environment")?;
    gha_cache_oxide::init_tracing(config.log_format).context("initialising tracing subscriber")?;

    // Debug-format the whole config — Secret fields redact themselves, so
    // this is safe. Drop to info so it surfaces at the default log level.
    tracing::info!(?config, "resolved configuration");

    let db = connect_db(&config.database).await?;
    db.migrate().await.context("running database migrations")?;
    tracing::info!("database ready");

    let storage = connect_storage(&config.storage).await?;
    tracing::info!("storage ready");

    // JWKS cache is lazy: first incoming request triggers the initial
    // fetch. Doing it here avoids blocking startup on an external host.
    let jwks = Arc::new(JwksCache::new(Arc::new(HttpJwksFetcher::github_default())));

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let state = AppState::new(db, storage, jwks, config);
    // Hold a tracker clone so we can drain in-flight merges after
    // axum::serve returns; `state` itself is consumed by `build_app`.
    let state_merges_clone = state.merges.clone();
    let app = gha_cache_oxide::build_app(state);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding TCP listener on {addr}"))?;

    tracing::info!(%addr, "gha-cache-oxide listening");
    // Clone the tracker so the server can still close it after serve()
    // returns. `merges` lives on `AppState` which was moved into the
    // router; we keep our own handle for the post-serve drain.
    let merge_tracker = state_merges_clone;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum::serve terminated with an error")?;

    // Port of upstream's `nitro.hooks.hook('close', () =>
    // storage.waitForOngoingMerges())` in `plugins/setup.ts`.
    // Finish any lazy-merge tasks that were still streaming to the
    // merged blob when the shutdown signal arrived; the CLI only
    // exits once this is done so a SIGTERM mid-download still lands
    // the merged blob.
    tracing::info!("awaiting in-flight lazy merges");
    merge_tracker.shutdown().await;
    tracing::info!("shutdown complete");
    Ok(())
}

/// Completes when the process receives a shutdown signal: `SIGINT`
/// (Ctrl-C) on every platform, plus `SIGTERM` on Unix. Either closes
/// the listener so axum stops accepting new connections; the
/// `serve(...).with_graceful_shutdown(...)` future returns once the
/// in-flight requests drain.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => tracing::info!("SIGINT received, shutting down"),
        () = terminate => tracing::info!("SIGTERM received, shutting down"),
    }
}

/// Creates a `Db` handle for the configured driver. `SQLite` and Postgres
/// are wired up; `MySQL` is defined in the config contract but deferred.
async fn connect_db(cfg: &DbConfig) -> anyhow::Result<Arc<dyn Db>> {
    match cfg {
        DbConfig::Sqlite { path } => {
            let db = SqliteDb::connect(path)
                .await
                .with_context(|| format!("opening sqlite database at {}", path.display()))?;
            Ok(Arc::new(db))
        }
        DbConfig::Postgres(pg) => {
            let url = postgres_url(pg);
            let db = PostgresDb::connect(&url)
                .await
                .context("connecting to postgres database")?;
            Ok(Arc::new(db))
        }
        DbConfig::Mysql { .. } => {
            anyhow::bail!("DB_DRIVER=mysql is deferred; use DB_DRIVER=sqlite or DB_DRIVER=postgres")
        }
    }
}

/// Builds a libpq-style URL from the two-form Postgres config.
/// [`PostgresConfig::Url`] is used verbatim (operators typically carry
/// their own query parameters — `sslmode`, `connect_timeout`, etc.).
/// [`PostgresConfig::Parts`] is assembled into `postgres://user:pw@host:port/db`.
fn postgres_url(cfg: &PostgresConfig) -> String {
    match cfg {
        PostgresConfig::Url(url) => url.expose().to_string(),
        PostgresConfig::Parts {
            host,
            port,
            user,
            password,
            database,
        } => {
            // Percent-encode the password so special characters (`@`, `/`,
            // `:`, `?`, `#`) don't rewrite the URL authority. The other
            // fields are trusted to contain URL-safe characters (host,
            // user, database normally come from config / compose files).
            // `NON_ALPHANUMERIC` is a conservative set — matches what
            // `std::process::Command` escaping reviewers expect to see.
            use url::form_urlencoded::byte_serialize;
            let pw: String = byte_serialize(password.expose().as_bytes()).collect();
            format!("postgres://{user}:{pw}@{host}:{port}/{database}")
        }
    }
}

/// Builds a storage adapter for the configured driver. Filesystem and
/// S3 are wired up; GCS is defined in the config contract but not yet
/// implemented — it surfaces an explicit startup error.
async fn connect_storage(cfg: &StorageConfig) -> anyhow::Result<Arc<dyn StorageAdapter>> {
    match cfg {
        StorageConfig::Filesystem { path } => {
            let adapter = FilesystemAdapter::new(path).with_context(|| {
                format!("initialising filesystem storage at {}", path.display())
            })?;
            Ok(Arc::new(adapter))
        }
        StorageConfig::S3 {
            bucket,
            region,
            endpoint_url,
            access_key_id,
            secret_access_key,
        } => {
            let adapter = S3Adapter::new(S3Config {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                // Production uses the upstream-compatible default —
                // explicit `None` here documents the choice.
                key_prefix: None,
            })
            .await
            .with_context(|| format!("initialising s3 storage for bucket {bucket:?}"))?;
            Ok(Arc::new(adapter))
        }
        StorageConfig::Gcs { .. } => anyhow::bail!(
            "STORAGE_DRIVER=gcs is not yet implemented; use STORAGE_DRIVER=filesystem or s3"
        ),
    }
}
