//! Shared application state carried on the axum router.
//!
//! Kept small: a `Db` handle and an `Arc`-wrapped `AppConfig`. Adapters
//! (storage in #5, JWKS cache in #6) add their own fields here as they
//! land.

use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::Db;

/// Application state. Cloned cheaply — both fields are themselves `Arc`s.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub config: Arc<AppConfig>,
}

impl AppState {
    /// Constructs the state from an already-connected DB and a config.
    #[must_use]
    pub fn new(db: Db, config: AppConfig) -> Self {
        Self {
            db,
            config: Arc::new(config),
        }
    }
}
