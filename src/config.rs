//! Runtime configuration loaded from environment variables.
//!
//! Issue #2 lands the minimum needed by the scaffolding: `PORT` and
//! `LOG_FORMAT`. Issue #3 extends this with driver-tagged storage and
//! database configuration variants.

use std::num::ParseIntError;

/// Resolved application configuration.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// TCP port the server binds to. Default 3000.
    pub port: u16,
    /// Log output format. Default `Text`; set `LOG_FORMAT=json` for JSON lines.
    pub log_format: LogFormat,
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

/// Errors produced while parsing environment configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid PORT '{value}': {source}")]
    InvalidPort {
        value: String,
        #[source]
        source: ParseIntError,
    },
    #[error("invalid LOG_FORMAT '{0}': expected 'text' or 'json'")]
    InvalidLogFormat(String),
}

impl AppConfig {
    /// Parses configuration from process environment.
    ///
    /// Defaults:
    /// - `PORT` unset → 3000
    /// - `LOG_FORMAT` unset → `Text`
    ///
    /// # Errors
    /// Returns `ConfigError::InvalidPort` if `PORT` is set but not a valid
    /// `u16`, and `ConfigError::InvalidLogFormat` if `LOG_FORMAT` is set to
    /// anything other than `text` or `json`.
    pub fn from_env() -> Result<Self, ConfigError> {
        let port = match std::env::var("PORT") {
            Ok(s) => s
                .parse::<u16>()
                .map_err(|source| ConfigError::InvalidPort { value: s, source })?,
            Err(_) => 3000,
        };
        let log_format = match std::env::var("LOG_FORMAT").as_deref() {
            Ok("json") => LogFormat::Json,
            Ok("text") | Err(_) => LogFormat::Text,
            Ok(other) => return Err(ConfigError::InvalidLogFormat(other.to_string())),
        };
        Ok(Self { port, log_format })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serialise env-var mutations across tests — process env is shared.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(setup: &[(&str, Option<&str>)], f: F) {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Snapshot + mutate
        let saved: Vec<(String, Option<String>)> = setup
            .iter()
            .map(|(k, _)| ((*k).to_string(), std::env::var(*k).ok()))
            .collect();
        for (k, v) in setup {
            match v {
                Some(val) => unsafe { std::env::set_var(k, val) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        f();
        // Restore
        for (k, v) in saved {
            match v {
                Some(val) => unsafe { std::env::set_var(&k, val) },
                None => unsafe { std::env::remove_var(&k) },
            }
        }
    }

    #[test]
    fn from_env_defaults_to_port_3000() {
        with_env(&[("PORT", None), ("LOG_FORMAT", None)], || {
            let cfg = AppConfig::from_env().unwrap();
            assert_eq!(cfg.port, 3000);
            assert_eq!(cfg.log_format, LogFormat::Text);
        });
    }

    #[test]
    fn from_env_parses_port_env() {
        with_env(&[("PORT", Some("9000")), ("LOG_FORMAT", None)], || {
            let cfg = AppConfig::from_env().unwrap();
            assert_eq!(cfg.port, 9000);
        });
    }

    #[test]
    fn from_env_rejects_invalid_port() {
        with_env(&[("PORT", Some("abc")), ("LOG_FORMAT", None)], || {
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::InvalidPort { .. }));
        });
    }

    #[test]
    fn from_env_parses_log_format_json() {
        with_env(&[("PORT", None), ("LOG_FORMAT", Some("json"))], || {
            let cfg = AppConfig::from_env().unwrap();
            assert_eq!(cfg.log_format, LogFormat::Json);
        });
    }

    #[test]
    fn from_env_rejects_invalid_log_format() {
        with_env(&[("PORT", None), ("LOG_FORMAT", Some("xml"))], || {
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::InvalidLogFormat(_)));
        });
    }
}
