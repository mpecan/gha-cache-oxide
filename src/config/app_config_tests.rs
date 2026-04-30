//! Test coverage for `AppConfig::from_env`.
//!
//! Hoisted out of `mod.rs` to keep the env-parsing module under the
//! 500-line soft limit. Pure tests — no production code lives here.

#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::config::env_test::{set, with_env};

/// Minimum valid env for `AppConfig::from_env()` to succeed.
/// Individual tests layer additional overrides on top.
fn minimal_env() -> Vec<(&'static str, Option<&'static str>)> {
    vec![
        ("API_BASE_URL", Some("http://localhost:3000")),
        ("PORT", None),
        ("LOG_FORMAT", None),
        ("CACHE_CLEANUP_OLDER_THAN_DAYS", None),
        ("DISABLE_CLEANUP_JOBS", None),
        ("CLEANUP_UPLOADS_SCHEDULE", None),
        ("CLEANUP_HOURLY_SCHEDULE", None),
        ("CLEANUP_DAILY_SCHEDULE", None),
        ("ENABLE_DIRECT_DOWNLOADS", None),
        ("SKIP_TOKEN_VALIDATION", None),
        ("MANAGEMENT_API_KEY", None),
        ("DEFAULT_ACTIONS_RESULTS_URL", None),
        ("PROXY_MAX_REQUEST_BODY_BYTES", None),
        ("STORAGE_DRIVER", Some("filesystem")),
        ("STORAGE_FILESYSTEM_PATH", Some("/tmp/gha")),
        ("DB_DRIVER", Some("sqlite")),
        ("DB_SQLITE_PATH", Some("/tmp/gha.db")),
    ]
}

#[test]
fn happy_path_with_defaults() {
    with_env(&minimal_env(), || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(cfg.api_base_url.as_str(), "http://localhost:3000/");
        assert_eq!(cfg.port, 3000);
        assert_eq!(cfg.log_format, LogFormat::Text);
        assert_eq!(cfg.cache_cleanup_older_than_days, 90);
        assert!(!cfg.disable_cleanup_jobs);
        assert!(!cfg.enable_direct_downloads);
        assert!(!cfg.skip_token_validation);
        assert!(cfg.management_api_key.is_none());
        assert!(matches!(cfg.storage, StorageConfig::Filesystem { .. }));
        assert!(matches!(cfg.database, DbConfig::Sqlite { .. }));
    });
}

#[test]
fn api_base_url_required_when_missing() {
    let mut setup = minimal_env();
    set(&mut setup, "API_BASE_URL", None);
    with_env(&setup, || {
        let err = AppConfig::from_env().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Missing {
                var: "API_BASE_URL"
            }
        ));
    });
}

#[test]
fn api_base_url_rejects_non_url() {
    let mut setup = minimal_env();
    set(&mut setup, "API_BASE_URL", Some("not a url"));
    with_env(&setup, || {
        let err = AppConfig::from_env().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidUrl {
                var: "API_BASE_URL",
                ..
            }
        ));
    });
}

#[test]
fn port_defaults_to_3000() {
    with_env(&minimal_env(), || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(cfg.port, 3000);
    });
}

#[test]
fn port_parses_env() {
    let mut setup = minimal_env();
    set(&mut setup, "PORT", Some("9000"));
    with_env(&setup, || {
        assert_eq!(AppConfig::from_env().unwrap().port, 9000);
    });
}

#[test]
fn port_rejects_invalid() {
    let mut setup = minimal_env();
    set(&mut setup, "PORT", Some("abc"));
    with_env(&setup, || {
        let err = AppConfig::from_env().unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { var: "PORT", .. }));
    });
}

#[test]
fn log_format_text_and_json() {
    let mut setup = minimal_env();
    set(&mut setup, "LOG_FORMAT", Some("json"));
    with_env(&setup, || {
        assert_eq!(AppConfig::from_env().unwrap().log_format, LogFormat::Json);
    });
    set(&mut setup, "LOG_FORMAT", Some("text"));
    with_env(&setup, || {
        assert_eq!(AppConfig::from_env().unwrap().log_format, LogFormat::Text);
    });
}

#[test]
fn log_format_rejects_other() {
    let mut setup = minimal_env();
    set(&mut setup, "LOG_FORMAT", Some("xml"));
    with_env(&setup, || {
        let err = AppConfig::from_env().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidLogFormat(_)));
    });
}

#[test]
fn cache_cleanup_days_defaults_to_90() {
    with_env(&minimal_env(), || {
        assert_eq!(
            AppConfig::from_env().unwrap().cache_cleanup_older_than_days,
            90
        );
    });
}

#[test]
fn cache_cleanup_days_parses_custom() {
    let mut setup = minimal_env();
    set(&mut setup, "CACHE_CLEANUP_OLDER_THAN_DAYS", Some("14"));
    with_env(&setup, || {
        assert_eq!(
            AppConfig::from_env().unwrap().cache_cleanup_older_than_days,
            14
        );
    });
}

#[test]
fn bool_vars_parse_true_false() {
    let mut setup = minimal_env();
    set(&mut setup, "DISABLE_CLEANUP_JOBS", Some("true"));
    set(&mut setup, "ENABLE_DIRECT_DOWNLOADS", Some("true"));
    set(&mut setup, "SKIP_TOKEN_VALIDATION", Some("true"));
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        assert!(cfg.disable_cleanup_jobs);
        assert!(cfg.enable_direct_downloads);
        assert!(cfg.skip_token_validation);
    });
    set(&mut setup, "DISABLE_CLEANUP_JOBS", Some("false"));
    with_env(&setup, || {
        assert!(!AppConfig::from_env().unwrap().disable_cleanup_jobs);
    });
}

#[test]
fn bool_vars_reject_non_boolean() {
    let mut setup = minimal_env();
    set(&mut setup, "ENABLE_DIRECT_DOWNLOADS", Some("yes"));
    with_env(&setup, || {
        let err = AppConfig::from_env().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidBool {
                var: "ENABLE_DIRECT_DOWNLOADS",
                ..
            }
        ));
    });
}

#[test]
fn cleanup_schedules_default_to_upstream_cron() {
    use std::str::FromStr;
    with_env(&minimal_env(), || {
        let cfg = AppConfig::from_env().unwrap();
        // Defaults parse to the upstream cron lines in
        // `nitro.config.ts:25`, normalised by prepending the seconds
        // field (`0 ` → "at second zero").
        assert_eq!(
            cfg.cleanup_uploads_cron,
            cron::Schedule::from_str("0 */5 * * * *").unwrap(),
        );
        assert_eq!(
            cfg.cleanup_hourly_cron,
            cron::Schedule::from_str("0 0 * * * *").unwrap(),
        );
        assert_eq!(
            cfg.cleanup_daily_cron,
            cron::Schedule::from_str("0 0 0 * * *").unwrap(),
        );
    });
}

#[test]
fn cleanup_schedules_override_parses_5_field() {
    use std::str::FromStr;
    let mut setup = minimal_env();
    set(&mut setup, "CLEANUP_UPLOADS_SCHEDULE", Some("*/2 * * * *"));
    set(&mut setup, "CLEANUP_HOURLY_SCHEDULE", Some("0 */2 * * *"));
    set(&mut setup, "CLEANUP_DAILY_SCHEDULE", Some("0 12 * * *"));
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(
            cfg.cleanup_uploads_cron,
            cron::Schedule::from_str("0 */2 * * * *").unwrap(),
        );
        assert_eq!(
            cfg.cleanup_hourly_cron,
            cron::Schedule::from_str("0 0 */2 * * *").unwrap(),
        );
        assert_eq!(
            cfg.cleanup_daily_cron,
            cron::Schedule::from_str("0 0 12 * * *").unwrap(),
        );
    });
}

#[test]
fn cleanup_schedules_override_parses_6_field() {
    use std::str::FromStr;
    // 6-field input (with explicit seconds) is parsed verbatim — lets
    // operators opt into second-level precision when they need it.
    let mut setup = minimal_env();
    set(
        &mut setup,
        "CLEANUP_UPLOADS_SCHEDULE",
        Some("*/30 * * * * *"),
    );
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(
            cfg.cleanup_uploads_cron,
            cron::Schedule::from_str("*/30 * * * * *").unwrap(),
        );
    });
}

#[test]
fn cleanup_schedules_reject_invalid_expression() {
    for var in [
        "CLEANUP_UPLOADS_SCHEDULE",
        "CLEANUP_HOURLY_SCHEDULE",
        "CLEANUP_DAILY_SCHEDULE",
    ] {
        let mut setup = minimal_env();
        set(&mut setup, var, Some("not a cron"));
        with_env(&setup, || {
            let err = AppConfig::from_env().unwrap_err();
            let ConfigError::Invalid {
                var: got_var,
                value,
                reason,
            } = err
            else {
                panic!("expected ConfigError::Invalid, got {err:?}");
            };
            assert_eq!(got_var, var);
            assert_eq!(value, "not a cron");
            assert!(
                reason.contains("cron"),
                "reason should mention cron: {reason}"
            );
        });
    }
}

#[test]
fn default_actions_results_url_defaults_to_upstream() {
    with_env(&minimal_env(), || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(
            cfg.default_actions_results_url.as_str(),
            "https://results-receiver.actions.githubusercontent.com/",
            "must match upstream `lib/schemas.ts:60-61` default",
        );
    });
}

#[test]
fn default_actions_results_url_override_parses() {
    let mut setup = minimal_env();
    set(
        &mut setup,
        "DEFAULT_ACTIONS_RESULTS_URL",
        Some("https://results-receiver.actions.xxxxxx.ghe.com"),
    );
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(
            cfg.default_actions_results_url.as_str(),
            "https://results-receiver.actions.xxxxxx.ghe.com/",
        );
    });
}

#[test]
fn default_actions_results_url_rejects_non_url() {
    let mut setup = minimal_env();
    set(&mut setup, "DEFAULT_ACTIONS_RESULTS_URL", Some("not a url"));
    with_env(&setup, || {
        let err = AppConfig::from_env().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidUrl {
                var: "DEFAULT_ACTIONS_RESULTS_URL",
                ..
            }
        ));
    });
}

#[test]
fn proxy_max_request_body_bytes_defaults_to_16_mib() {
    with_env(&minimal_env(), || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(cfg.proxy_max_request_body_bytes, 16 * 1024 * 1024);
    });
}

#[test]
fn proxy_max_request_body_bytes_override_parses() {
    let mut setup = minimal_env();
    set(&mut setup, "PROXY_MAX_REQUEST_BODY_BYTES", Some("4194304"));
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        assert_eq!(cfg.proxy_max_request_body_bytes, 4 * 1024 * 1024);
    });
}

#[test]
fn proxy_max_request_body_bytes_rejects_non_numeric() {
    let mut setup = minimal_env();
    set(&mut setup, "PROXY_MAX_REQUEST_BODY_BYTES", Some("huge"));
    with_env(&setup, || {
        let err = AppConfig::from_env().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Invalid {
                var: "PROXY_MAX_REQUEST_BODY_BYTES",
                ..
            }
        ));
    });
}

#[test]
fn management_api_key_is_secret() {
    let mut setup = minimal_env();
    set(&mut setup, "MANAGEMENT_API_KEY", Some("super-admin-token"));
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        let key = cfg.management_api_key.as_ref().unwrap();
        assert_eq!(key.expose(), "super-admin-token");
    });
}

#[test]
fn debug_format_redacts_secrets() {
    let mut setup = minimal_env();
    set(&mut setup, "MANAGEMENT_API_KEY", Some("super-admin-token"));
    set(&mut setup, "DB_DRIVER", Some("postgres"));
    set(&mut setup, "DB_SQLITE_PATH", None);
    set(
        &mut setup,
        "DB_POSTGRES_URL",
        Some("postgres://u:SEKRET_DB_PASS@host/db"),
    );
    set(&mut setup, "STORAGE_DRIVER", Some("s3"));
    set(&mut setup, "STORAGE_FILESYSTEM_PATH", None);
    set(&mut setup, "STORAGE_S3_BUCKET", Some("bucket"));
    set(
        &mut setup,
        "AWS_SECRET_ACCESS_KEY",
        Some("SEKRET_AWS_KEY_MATERIAL"),
    );
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("super-admin-token"),
            "management key leaked: {dbg}"
        );
        assert!(
            !dbg.contains("SEKRET_DB_PASS"),
            "postgres password leaked: {dbg}"
        );
        assert!(
            !dbg.contains("SEKRET_AWS_KEY_MATERIAL"),
            "aws key leaked: {dbg}"
        );
        assert!(dbg.contains("[redacted]"), "no [redacted] marker: {dbg}");
    });
}

#[test]
fn debug_format_redacts_postgres_parts_password() {
    // Postgres parts form: password lives in DbConfig::Postgres::Parts.
    let mut setup = minimal_env();
    set(&mut setup, "DB_DRIVER", Some("postgres"));
    set(&mut setup, "DB_SQLITE_PATH", None);
    set(&mut setup, "DB_POSTGRES_HOST", Some("pg.internal"));
    set(&mut setup, "DB_POSTGRES_PORT", Some("5432"));
    set(&mut setup, "DB_POSTGRES_USER", Some("cache"));
    set(
        &mut setup,
        "DB_POSTGRES_PASSWORD",
        Some("SEKRET_PG_PARTS_PASS"),
    );
    set(&mut setup, "DB_POSTGRES_DATABASE", Some("gha"));
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("SEKRET_PG_PARTS_PASS"),
            "postgres parts password leaked: {dbg}"
        );
        assert!(dbg.contains("[redacted]"), "no [redacted] marker: {dbg}");
    });
}

#[test]
fn debug_format_redacts_mysql_password() {
    let mut setup = minimal_env();
    set(&mut setup, "DB_DRIVER", Some("mysql"));
    set(&mut setup, "DB_SQLITE_PATH", None);
    set(&mut setup, "DB_MYSQL_HOST", Some("mysql.internal"));
    set(&mut setup, "DB_MYSQL_PORT", Some("3306"));
    set(&mut setup, "DB_MYSQL_USER", Some("cache"));
    set(&mut setup, "DB_MYSQL_PASSWORD", Some("SEKRET_MYSQL_PASS"));
    set(&mut setup, "DB_MYSQL_DATABASE", Some("gha"));
    with_env(&setup, || {
        let cfg = AppConfig::from_env().unwrap();
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("SEKRET_MYSQL_PASS"),
            "mysql password leaked: {dbg}"
        );
        assert!(dbg.contains("[redacted]"), "no [redacted] marker: {dbg}");
    });
}
