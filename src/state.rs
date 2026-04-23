//! Shared application state carried on the axum router.
//!
//! Holds a `Db` handle, a storage adapter (trait object), and an
//! `Arc`-wrapped `AppConfig`. Cloned cheaply — every field is itself
//! `Arc`-ish. JWKS cache from #6 will add its own field.

use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::Db;
use crate::storage::StorageAdapter;

/// Application state. Cloned cheaply — every field is itself `Arc`-ish.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub storage: Arc<dyn StorageAdapter>,
    pub config: Arc<AppConfig>,
}

impl AppState {
    /// Constructs the state from an already-connected DB, a storage adapter
    /// wrapped for shared access, and a config.
    #[must_use]
    pub fn new(db: Db, storage: Arc<dyn StorageAdapter>, config: AppConfig) -> Self {
        Self {
            db,
            storage,
            config: Arc::new(config),
        }
    }
}
