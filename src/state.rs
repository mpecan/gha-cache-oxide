//! Shared application state carried on the axum router.
//!
//! Holds a `Db` handle, a storage adapter (trait object), a JWKS cache
//! for OIDC middleware, and an `Arc`-wrapped `AppConfig`. Cloned cheaply
//! — every field is itself `Arc`-ish.

use std::sync::Arc;

use crate::auth::JwksCache;
use crate::config::AppConfig;
use crate::db::Db;
use crate::storage::StorageAdapter;

/// Application state. Cloned cheaply — every field is itself `Arc`-ish.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<dyn Db>,
    pub storage: Arc<dyn StorageAdapter>,
    pub jwks: Arc<JwksCache>,
    pub config: Arc<AppConfig>,
}

impl AppState {
    /// Constructs the state from an already-connected DB, a storage
    /// adapter wrapped for shared access, a JWKS cache, and a config.
    #[must_use]
    pub fn new(
        db: Arc<dyn Db>,
        storage: Arc<dyn StorageAdapter>,
        jwks: Arc<JwksCache>,
        config: AppConfig,
    ) -> Self {
        Self {
            db,
            storage,
            jwks,
            config: Arc::new(config),
        }
    }
}
