//! Shared harness for the management integration tests.
//!
//! `tests/management.rs` is the test-binary entry; the topical
//! submodules under `tests/management/` use `use super::common::*;`
//! to reach the harness builder, the request helper, and the seed
//! helpers below. Keeping every test in one binary preserves the
//! original `#19` test set's filename (`management::*` test paths)
//! while staying under the 700-line file limit per topic file.

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, LogFormat, Secret, StorageConfig};
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
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let storage: Arc<dyn StorageAdapter> = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(NullFetcher)));
    let config = AppConfig {
        api_base_url: "http://localhost:3000".parse().unwrap(),
        port: 0,
        log_format: LogFormat::Text,
        cache_cleanup_older_than_days: 90,
        disable_cleanup_jobs: true,
        enable_direct_downloads: false,
        skip_token_validation: true,
        management_api_key: management_api_key.map(|k| Secret::new(k.to_string())),
        default_actions_results_url: "https://results-receiver.test/".parse().unwrap(),
        proxy_max_request_body_bytes: 16 * 1024 * 1024,
        storage: StorageConfig::Filesystem {
            path: tmp.path().to_path_buf(),
        },
        database: DbConfig::Sqlite {
            path: PathBuf::from(":memory:"),
        },
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
