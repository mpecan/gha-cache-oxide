//! Runtime configuration loaded from environment variables.
//!
//! The public surface matches upstream `lib/schemas.ts` verbatim for env-var
//! names. `AppConfig` composes a base struct with driver-tagged `storage` and
//! `database` sub-configurations. Secrets are wrapped in [`Secret`] so they
//! redact themselves in `Debug`/`Display` output.

pub mod db;
mod env;
pub mod error;
pub mod secret;
pub mod storage;
mod test_defaults;

#[cfg(test)]
mod app_config_tests;
#[cfg(test)]
mod env_test;

pub use db::{DbConfig, MysqlConfig, PostgresConfig};
pub use error::ConfigError;
pub use secret::Secret;
pub use storage::StorageConfig;

use std::str::FromStr;
use url::Url;

/// Resolved application configuration. Field order mirrors issue #3's
/// proposal for easier review.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub api_base_url: Url,
    pub port: u16,
    pub log_format: LogFormat,
    pub cache_cleanup_older_than_days: u32,
    pub disable_cleanup_jobs: bool,
    /// Cron schedule for `cleanup:uploads`. Default `*/5 * * * *`,
    /// matching upstream `nitro.config.ts:25`. 5-field upstream syntax
    /// is normalised to 6-field (sec=0) before parsing; operators can
    /// also write 6-field directly for second-level precision. Use
    /// `DISABLE_CLEANUP_JOBS=true` to disable cleanup entirely.
    pub cleanup_uploads_cron: cron::Schedule,
    /// Cron schedule for the `cleanup:parts` + `cleanup:merges` cycle.
    /// Default `0 * * * *`. Same normalisation + global-disable rule
    /// as `cleanup_uploads_cron`.
    pub cleanup_hourly_cron: cron::Schedule,
    /// Cron schedule for the `cleanup:cache-entries` +
    /// `cleanup:storage-locations` cycle. Default `0 0 * * *`. Same
    /// normalisation + global-disable rule as `cleanup_uploads_cron`.
    pub cleanup_daily_cron: cron::Schedule,
    pub enable_direct_downloads: bool,
    pub skip_token_validation: bool,
    pub management_api_key: Option<Secret>,
    /// Receiver URL the catch-all proxy forwards unmatched requests to.
    /// Mirrors upstream `lib/schemas.ts:60-61` (issue #24). Default
    /// `https://results-receiver.actions.githubusercontent.com`.
    pub default_actions_results_url: Url,
    /// Upper bound on a request body forwarded through the catch-all
    /// proxy fallback (`PROXY_MAX_REQUEST_BODY_BYTES`, default 16 MiB).
    /// Oversized requests respond with `413 Payload Too Large`. Cache
    /// uploads use the explicit blob routes and never reach the
    /// fallback, so this only limits the small RPCs `actions/cache`
    /// makes against the receiver.
    ///
    /// **Deliberate divergence from upstream** (`routes/[...path].ts`
    /// forwards bodies without a cap; this is local `DoS` mitigation —
    /// see `parse_proxy_max_body` for the audit context). Set
    /// `PROXY_MAX_REQUEST_BODY_BYTES=0` to disable the cap entirely
    /// (full upstream parity); the parser normalises `0` to
    /// [`usize::MAX`].
    pub proxy_max_request_body_bytes: usize,
    /// Shared HMAC secret for the Forgejo runner cache dialect
    /// (`FORGEJO_CACHE_SECRET`, must equal the runner's `cache.secret`).
    /// When set, the v1 `/_apis/artifactcache/*` surface that the
    /// Forgejo runner's cache proxy speaks is mounted; unset leaves it
    /// off entirely. Not an upstream knob — upstream dropped v1 in
    /// v9.0.0 — see `src/routes/forgejo/mod.rs`.
    pub forgejo_cache_secret: Option<Secret>,
    pub storage: StorageConfig,
    pub database: DbConfig,
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

impl AppConfig {
    /// Parses configuration from process environment.
    ///
    /// # Errors
    /// See [`ConfigError`] — every variant names the specific env var that
    /// produced it so operators can fix the misconfiguration directly.
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            api_base_url: env::required_url("API_BASE_URL")?,
            port: parse_port()?,
            log_format: parse_log_format()?,
            cache_cleanup_older_than_days: env::u32_or_default(
                "CACHE_CLEANUP_OLDER_THAN_DAYS",
                90,
            )?,
            disable_cleanup_jobs: env::bool_or_default("DISABLE_CLEANUP_JOBS", false)?,
            cleanup_uploads_cron: cron_schedule_or_default(
                "CLEANUP_UPLOADS_SCHEDULE",
                "*/5 * * * *",
            )?,
            cleanup_hourly_cron: cron_schedule_or_default("CLEANUP_HOURLY_SCHEDULE", "0 * * * *")?,
            cleanup_daily_cron: cron_schedule_or_default("CLEANUP_DAILY_SCHEDULE", "0 0 * * *")?,
            enable_direct_downloads: env::bool_or_default("ENABLE_DIRECT_DOWNLOADS", false)?,
            skip_token_validation: env::bool_or_default("SKIP_TOKEN_VALIDATION", false)?,
            management_api_key: env::optional_secret("MANAGEMENT_API_KEY"),
            default_actions_results_url: env::url_with_default(
                "DEFAULT_ACTIONS_RESULTS_URL",
                "https://results-receiver.actions.githubusercontent.com",
            )?,
            proxy_max_request_body_bytes: parse_proxy_max_body()?,
            forgejo_cache_secret: env::optional_secret("FORGEJO_CACHE_SECRET"),
            storage: StorageConfig::from_env()?,
            database: DbConfig::from_env()?,
        })
    }
}

/// Reads `PROXY_MAX_REQUEST_BODY_BYTES`, normalising `0` to
/// [`usize::MAX`].
///
/// The cap is a deliberate divergence from upstream's uncapped
/// catch-all forwarding (`routes/[...path].ts`) — it's a `DoS`
/// mitigation, not a feature parity item. The audit at issue #76
/// found no current `actions/cache` v2 / `tonistiigi/go-actions-cache`
/// flow that legitimately POSTs >16 MiB through the catch-all path,
/// so 16 MiB is generous as a default. Operators who need full
/// upstream parity (or who hit a future flow that exceeds the cap)
/// set `PROXY_MAX_REQUEST_BODY_BYTES=0`, which we rewrite to
/// `usize::MAX` so `axum::body::to_bytes` accepts the entire buffer.
fn parse_proxy_max_body() -> Result<usize, ConfigError> {
    let raw = env::usize_or_default("PROXY_MAX_REQUEST_BODY_BYTES", 16 * 1024 * 1024)?;
    Ok(if raw == 0 { usize::MAX } else { raw })
}

fn parse_port() -> Result<u16, ConfigError> {
    // PORT has a typed default; reuse env::optional's empty-as-missing rule,
    // fall through to the default when unset.
    env::optional("PORT").map_or(Ok(3000), |s| {
        s.parse::<u16>().map_err(|e| ConfigError::Invalid {
            var: "PORT",
            value: s,
            reason: e.to_string(),
        })
    })
}

/// Reads a cron-schedule env var with a static default.
///
/// Operators write upstream-style 5-field cron (`*/5 * * * *`); we
/// prepend `0 ` so the [`cron`] crate's required seconds field gets a
/// sensible "fire at sec=0" default. 6-field input is parsed as-is so
/// operators can opt into second-level precision.
///
/// Empty/missing → parse `default`. Malformed → `ConfigError::Invalid`
/// pointing at the offending env var so operators see the typo at
/// startup (no silent fallback per constitution). The default literal
/// is parsed eagerly; a malformed default surfaces the same error.
fn cron_schedule_or_default(
    var: &'static str,
    default: &'static str,
) -> Result<cron::Schedule, ConfigError> {
    let raw = env::optional(var);
    let expr = raw.as_deref().unwrap_or(default);
    let normalised = match expr.split_whitespace().count() {
        5 => format!("0 {expr}"),
        _ => expr.to_string(),
    };
    cron::Schedule::from_str(&normalised).map_err(|source| ConfigError::Invalid {
        var,
        value: expr.to_string(),
        reason: format!("invalid cron expression: {source}"),
    })
}

fn parse_log_format() -> Result<LogFormat, ConfigError> {
    match env::optional("LOG_FORMAT").as_deref() {
        Some("json") => Ok(LogFormat::Json),
        Some("text") | None => Ok(LogFormat::Text),
        Some(other) => Err(ConfigError::InvalidLogFormat(other.to_string())),
    }
}
