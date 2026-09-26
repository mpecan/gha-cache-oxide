//! Test-only `AppConfig` constructor.
//!
//! Hoisted out of `mod.rs` to keep the public-API surface in one
//! place but the bulk of the defaults out of the env-parsing module.
//! Reachable from integration tests in `tests/` because it lives on
//! the public surface (no `#[cfg(test)]` gate).

use std::str::FromStr;

use super::{AppConfig, DbConfig, LogFormat, StorageConfig};

impl AppConfig {
    /// Build an `AppConfig` with sensible defaults for in-process tests.
    ///
    /// Tests typically only care about a handful of fields (often just
    /// `storage` and `database`); use struct-update syntax to override
    /// only what your test exercises:
    ///
    /// ```ignore
    /// let cfg = AppConfig {
    ///     skip_token_validation: false,
    ///     ..AppConfig::test_defaults(storage_cfg, db_cfg)
    /// };
    /// ```
    ///
    /// Lives on the public surface (rather than gated on
    /// `#[cfg(test)]`) so integration tests in `tests/` can see it
    /// without a feature flag. This is the single source of truth for
    /// every test fixture's defaults — when a new field lands on
    /// `AppConfig`, adding it here is enough; call sites stay
    /// untouched unless the new field needs a per-test override.
    ///
    /// The hardcoded URL and cron parses are infallible — every literal
    /// is a valid `Url` / `cron::Schedule` — so the `expect`s here can
    /// never fire. Allowed only because the alternative (returning
    /// `Result`) forces every caller into an `.unwrap()` and adds no
    /// safety.
    ///
    /// # Panics
    ///
    /// Never. The five `expect`s (`Url::parse` × 2, `cron::Schedule::from_str`
    /// × 3) cover hardcoded literals that have been verified to parse;
    /// clippy's `missing_panics_doc` requires the section even when
    /// the panic is statically unreachable.
    #[allow(clippy::expect_used, clippy::missing_panics_doc)]
    #[must_use]
    pub fn test_defaults(storage: StorageConfig, database: DbConfig) -> Self {
        Self {
            api_base_url: "http://localhost:3000"
                .parse()
                .expect("hardcoded valid URL literal"),
            port: 0,
            log_format: LogFormat::Text,
            cache_cleanup_older_than_days: 90,
            disable_cleanup_jobs: true,
            cleanup_uploads_cron: cron::Schedule::from_str("0 */5 * * * *")
                .expect("hardcoded valid cron"),
            cleanup_hourly_cron: cron::Schedule::from_str("0 0 * * * *")
                .expect("hardcoded valid cron"),
            cleanup_daily_cron: cron::Schedule::from_str("0 0 0 * * *")
                .expect("hardcoded valid cron"),
            enable_direct_downloads: false,
            skip_token_validation: true,
            management_api_key: None,
            forgejo_cache_secret: None,
            default_actions_results_url: "https://results-receiver.test/"
                .parse()
                .expect("hardcoded valid URL literal"),
            proxy_max_request_body_bytes: 16 * 1024 * 1024,
            storage,
            database,
        }
    }
}
