//! Top-level error type. New variants are added as modules start producing
//! their own error kinds.

use crate::config::ConfigError;

/// Errors originating in the crate. Additional variants are added as each
/// milestone lands its own error kinds (DB, storage, auth).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias for crate-wide `Result`s.
pub type Result<T> = std::result::Result<T, Error>;
