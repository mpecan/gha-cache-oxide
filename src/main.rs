//! Binary entry point. Keep this thin — orchestration only, no logic. All
//! testable behaviour lives in `gha_cache_oxide::*`.

use std::net::SocketAddr;

use anyhow::Context;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = gha_cache_oxide::config::AppConfig::from_env()
        .context("loading configuration from environment")?;
    gha_cache_oxide::init_tracing(config.log_format).context("initialising tracing subscriber")?;

    let app = gha_cache_oxide::build_app();
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding TCP listener on {addr}"))?;

    tracing::info!(%addr, "gha-cache-oxide listening");
    axum::serve(listener, app)
        .await
        .context("axum::serve terminated with an error")?;
    Ok(())
}
