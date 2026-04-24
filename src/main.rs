//! Binary entry point. Keep this thin — orchestration only, no logic. All
//! testable behaviour lives in `gha_cache_oxide::*`.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use gha_cache_oxide::auth::{HttpJwksFetcher, JwksCache};
use gha_cache_oxide::config::{AppConfig, DbConfig, StorageConfig};
use gha_cache_oxide::db::Db;
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
    let app = gha_cache_oxide::build_app(state);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding TCP listener on {addr}"))?;

    tracing::info!(%addr, "gha-cache-oxide listening");
    axum::serve(listener, app)
        .await
        .context("axum::serve terminated with an error")?;
    Ok(())
}

/// Creates a `Db` handle for the configured driver. Only `SQLite` is wired
/// up in M1; other drivers exist in the config contract but produce an
/// explicit startup error directing the operator to the sqlite driver.
async fn connect_db(cfg: &DbConfig) -> anyhow::Result<Db> {
    match cfg {
        DbConfig::Sqlite { path } => Db::connect_sqlite(path)
            .await
            .with_context(|| format!("opening sqlite database at {}", path.display())),
        DbConfig::Postgres(_) => anyhow::bail!(
            "DB_DRIVER=postgres is not yet implemented in M1 (landing in #14); \
             use DB_DRIVER=sqlite for now"
        ),
        DbConfig::Mysql { .. } => {
            anyhow::bail!("DB_DRIVER=mysql is not yet implemented; use DB_DRIVER=sqlite")
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
