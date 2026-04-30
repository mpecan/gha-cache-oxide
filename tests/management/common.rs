//! Shared harness for the management integration tests.
//!
//! `tests/management.rs` is the test-binary entry; the topical
//! submodules under `tests/management/` use `use super::common::*;`
//! to reach the harness builder, the request helper, and the seed
//! helpers below. Keeping every test in one binary preserves the
//! original `#19` test set's filename (`management::*` test paths)
//! while staying under the 700-line file limit per topic file.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, Secret, StorageConfig};
use gha_cache_oxide::db::entities::CacheEntryCoord;
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::{ByteStream, FilesystemAdapter, StorageAdapter};
use serde_json::Value;
use tempfile::TempDir;

pub const KEY: &str = "test-management-key";

struct NullFetcher;
#[async_trait::async_trait]
impl JwksFetcher for NullFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        Ok(Vec::new())
    }
}

pub struct Harness {
    pub router: axum::Router,
    pub db: Arc<dyn Db>,
    pub storage: Arc<dyn StorageAdapter>,
    pub _tmp: TempDir,
}

pub async fn harness(management_api_key: Option<&str>) -> Harness {
    harness_with(management_api_key, true).await
}

/// Variant of [`harness`] that wires up the on-demand cleanup sweeps
/// by flipping `disable_cleanup_jobs` to `false`. Used by the #71
/// orphan-sweep tests; everything else uses [`harness`] which keeps
/// cleanup config-disabled (matches the default harness shape used by
/// every pre-#71 test).
pub async fn harness_with_cleanup_enabled(management_api_key: Option<&str>) -> Harness {
    harness_with(management_api_key, false).await
}

async fn harness_with(management_api_key: Option<&str>, disable_cleanup_jobs: bool) -> Harness {
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let storage: Arc<dyn StorageAdapter> = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(NullFetcher)));
    let config = AppConfig {
        disable_cleanup_jobs,
        management_api_key: management_api_key.map(|k| Secret::new(k.to_string())),
        ..AppConfig::test_defaults(
            StorageConfig::Filesystem {
                path: tmp.path().to_path_buf(),
            },
            DbConfig::Sqlite {
                path: PathBuf::from(":memory:"),
            },
        )
    };
    let state = AppState::new(db.clone(), storage.clone(), jwks, config);
    let router = gha_cache_oxide::build_app(state);
    Harness {
        router,
        db,
        storage,
        _tmp: tmp,
    }
}

pub fn req(method: Method, uri: &str, key: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(k) = key {
        b = b.header(header::AUTHORIZATION, format!("Bearer {k}"));
    }
    b.body(Body::empty()).unwrap()
}

pub async fn body_json(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    if bytes.is_empty() {
        return (status, Value::Null);
    }
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (status, value)
}

pub async fn seed_entry(db: &dyn Db, loc_id: &str, folder: &str, entry_id: &str, scope: &str) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(loc_id, folder, 1).await.unwrap();
    tx.seed_cache_entry(
        entry_id,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope,
            repo_id: "r",
        },
        0,
        loc_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

pub async fn upload_test_file(storage: &dyn StorageAdapter, name: &str) {
    use bytes::Bytes;
    use futures::StreamExt;
    let chunk: Result<Bytes, std::io::Error> = Ok(Bytes::from_static(b"hello"));
    let stream: ByteStream = futures::stream::iter(vec![chunk]).boxed();
    storage.upload_stream(name, stream).await.unwrap();
}

/// Polls `check` repeatedly until it returns `true` or the `timeout`
/// elapses. Sleeps between polls so the surrounding tokio runtime can
/// drive other spawned futures forward — used by the on-demand
/// orphan-sweep tests (#71) to wait for `tokio::spawn`'d cleanup
/// passes without coupling the assertion to wall-clock timing.
///
/// # Panics
/// Panics with the supplied label after the timeout elapses with
/// `check` still returning false.
pub async fn poll_until<F, Fut>(timeout: Duration, label: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("poll_until timed out after {timeout:?}: {label}");
}
