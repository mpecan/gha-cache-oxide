//! Errors produced while parsing environment configuration.
//!
//! Each variant names the specific env var that triggered the failure so the
//! operator can fix the misconfiguration without re-reading the source.

/// Errors produced while parsing environment configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("required environment variable {var} is not set")]
    Missing { var: &'static str },

    #[error("invalid environment variable {var}={value:?}: {reason}")]
    Invalid {
        var: &'static str,
        value: String,
        reason: String,
    },

    #[error("invalid URL in {var}: {source}")]
    InvalidUrl {
        var: &'static str,
        #[source]
        source: url::ParseError,
    },

    #[error("invalid boolean in {var}={value:?}: expected 'true' or 'false'")]
    InvalidBool { var: &'static str, value: String },

    #[error("invalid LOG_FORMAT {0:?}: expected 'text' or 'json'")]
    InvalidLogFormat(String),

    #[error("unknown STORAGE_DRIVER {0:?} (expected one of: filesystem, s3, gcs)")]
    UnknownStorageDriver(String),

    #[error("unknown DB_DRIVER {0:?} (expected one of: sqlite, postgres, mysql)")]
    UnknownDbDriver(String),

    #[error(
        "DB_POSTGRES_URL is mutually exclusive with DB_POSTGRES_{{HOST,PORT,USER,PASSWORD,DATABASE}}; set exactly one form"
    )]
    PostgresConflict,

    #[error(
        "DB_MYSQL_URL is mutually exclusive with DB_MYSQL_{{HOST,PORT,USER,PASSWORD,DATABASE}}; set exactly one form"
    )]
    MysqlConflict,
}
