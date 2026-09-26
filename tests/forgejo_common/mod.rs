//! Shared harness for `tests/forgejo.rs`: a real server on a random
//! port with the Forgejo v1 dialect enabled, and a client that signs
//! requests the way the Forgejo runner's cache proxy does.

#![allow(dead_code, clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use bytes::Bytes;
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, Secret, StorageConfig};
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::metrics::Metrics;
use gha_cache_oxide::routes::forgejo::compute_mac;
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::{
    ByteStream, FilesystemAdapter, ObjectInfo, StorageAdapter, StorageError,
};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub const SECRET: &str = "secret";
pub const REPO: &str = "testing/repo";
pub const RUN_NUMBER: &str = "1";
pub const RUN_ID: &str = "0123456789abcdef";
pub const PROXY_HOST: &str = "http://proxy.invalid:4321";
pub const VERSION: &str = "c19da02a2bd7e77277f1ac29ab45c09b7d46a4ee758284e26bb3045ad11d9d20";
pub const ARTIFACTS_PREFIX: &str = "/_apis/artifactcache/artifacts/";

struct NoJwks;

#[async_trait::async_trait]
impl JwksFetcher for NoJwks {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        Ok(Vec::new())
    }
}

pub struct Server {
    pub base: String,
    pub db: Arc<dyn Db>,
    pub storage: Arc<dyn StorageAdapter>,
    pub metrics: Arc<Metrics>,
    pub tmp: TempDir,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawns a server. `secret = None` leaves the dialect disabled.
pub async fn spawn(secret: Option<&str>) -> Server {
    spawn_inner(secret, false).await.0
}

/// Dialect enabled, storage wrapped in a [`FlakyStorage`] the test can
/// switch to failing.
pub async fn spawn_flaky() -> (Server, Arc<FlakyStorage>) {
    let (srv, flaky) = spawn_inner(Some(SECRET), true).await;
    (srv, flaky.unwrap())
}

async fn spawn_inner(secret: Option<&str>, flaky: bool) -> (Server, Option<Arc<FlakyStorage>>) {
    let tmp = TempDir::new().unwrap();
    let fs: Arc<dyn StorageAdapter> = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let flaky = flaky.then(|| FlakyStorage::new(fs.clone()));
    let storage: Arc<dyn StorageAdapter> = match &flaky {
        Some(f) => f.clone(),
        None => fs,
    };
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let config = AppConfig {
        api_base_url: base.parse().unwrap(),
        forgejo_cache_secret: secret.map(|s| Secret::new(s.to_string())),
        // Unroutable, so a request that falls through to the catch-all
        // proxy fails fast with 502 instead of reaching GitHub.
        default_actions_results_url: "http://127.0.0.1:1".parse().unwrap(),
        ..AppConfig::test_defaults(
            StorageConfig::Filesystem {
                path: tmp.path().to_path_buf(),
            },
            DbConfig::Sqlite {
                path: PathBuf::from(":memory:"),
            },
        )
    };
    let jwks = Arc::new(JwksCache::new(Arc::new(NoJwks)));
    let state = AppState::new(db.clone(), storage.clone(), jwks, config);
    let metrics = state.metrics.clone();
    let app: Router = gha_cache_oxide::build_app(state);

    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
    });

    let srv = Server {
        base,
        db,
        storage,
        metrics,
        tmp,
        shutdown: Some(tx),
    };
    (srv, flaky)
}

/// Signs requests like `act/cacheproxy`'s `Rewrite` hook.
#[derive(Clone)]
pub struct Runner {
    pub http: reqwest::Client,
    pub base: String,
    pub repo: String,
    pub write_isolation_key: String,
    /// `Some` overrides the computed MAC (for bad-MAC tests).
    pub mac_override: Option<String>,
    pub timestamp: String,
}

impl Runner {
    pub fn new(srv: &Server) -> Self {
        let ts = (chrono::Utc::now().timestamp() - 10).to_string();
        Self {
            http: reqwest::Client::new(),
            base: format!("{}/_apis/artifactcache", srv.base),
            repo: REPO.into(),
            write_isolation_key: String::new(),
            mac_override: None,
            timestamp: ts,
        }
    }

    pub fn with_key(&self, wik: &str) -> Self {
        Self {
            write_isolation_key: wik.into(),
            ..self.clone()
        }
    }

    pub fn with_repo(&self, repo: &str) -> Self {
        Self {
            repo: repo.into(),
            ..self.clone()
        }
    }

    pub fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let url = if path.starts_with("http") {
            path.to_string()
        } else {
            format!("{}{path}", self.base)
        };
        let mac = self.mac_override.clone().unwrap_or_else(|| {
            compute_mac(
                SECRET,
                &self.repo,
                RUN_NUMBER,
                &self.timestamp,
                &self.write_isolation_key,
            )
            .unwrap()
        });
        let mut b = self
            .http
            .request(method, url)
            .header("Forgejo-Cache-Repo", &self.repo)
            .header("Forgejo-Cache-RunNumber", RUN_NUMBER)
            .header("Forgejo-Cache-RunId", RUN_ID)
            .header("Forgejo-Cache-Timestamp", &self.timestamp)
            .header("Forgejo-Cache-MAC", mac)
            .header("Forgejo-Cache-Host", PROXY_HOST);
        if !self.write_isolation_key.is_empty() {
            b = b.header("Forgejo-Cache-WriteIsolationKey", &self.write_isolation_key);
        }
        b
    }

    pub async fn reserve(&self, key: &str, version: &str, size: usize) -> i64 {
        let resp = self
            .request(Method::POST, "/caches")
            .json(&json!({ "key": key, "version": version, "cacheSize": size }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        let id = body["cacheId"].as_i64().expect("numeric cacheId");
        assert_ne!(id, 0, "cacheId must be truthy for @actions/cache");
        id
    }

    pub async fn patch(
        &self,
        id: impl std::fmt::Display,
        range: &str,
        body: Vec<u8>,
    ) -> StatusCode {
        self.request(Method::PATCH, &format!("/caches/{id}"))
            .header("Content-Type", "application/octet-stream")
            .header("Content-Range", range)
            .body(body)
            .send()
            .await
            .unwrap()
            .status()
    }

    /// `size = None` sends an empty body, like act's own tests.
    pub async fn commit(&self, id: impl std::fmt::Display, size: Option<usize>) -> StatusCode {
        let b = self.request(Method::POST, &format!("/caches/{id}"));
        let b = match size {
            Some(s) => b.json(&json!({ "size": s })),
            None => b,
        };
        b.send().await.unwrap().status()
    }

    /// Returns `(status, body)` of `GET /cache`.
    pub async fn find(&self, keys: &str, version: &str) -> (StatusCode, Option<Value>) {
        let resp = self
            .request(
                Method::GET,
                &format!("/cache?keys={keys}&version={version}"),
            )
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = if status == StatusCode::OK {
            Some(resp.json().await.unwrap())
        } else {
            None
        };
        (status, body)
    }

    /// Fetches an `archiveLocation`. The location points at the runner
    /// proxy (`<host>/<runId>/_apis/...`); the proxy strips the run id,
    /// so the test does the same and talks to the server directly.
    pub async fn download(&self, archive_location: &str) -> (StatusCode, Bytes) {
        let expected_prefix = format!("{PROXY_HOST}/{RUN_ID}{ARTIFACTS_PREFIX}");
        let id = archive_location
            .strip_prefix(&expected_prefix)
            .unwrap_or_else(|| panic!("archiveLocation {archive_location:?} not via proxy"));
        let resp = self
            .request(Method::GET, &format!("/artifacts/{id}"))
            .send()
            .await
            .unwrap();
        (resp.status(), resp.bytes().await.unwrap())
    }

    /// Port of act's `uploadCacheNormally`: reserve, one chunk, commit,
    /// find, download, compare.
    pub async fn upload_normally(&self, key: &str, version: &str, content: &[u8]) {
        let id = self.reserve(key, version, content.len()).await;
        assert_eq!(
            self.patch(id, "bytes 0-99/*", content.to_vec()).await,
            StatusCode::OK
        );
        assert_eq!(self.commit(id, Some(content.len())).await, StatusCode::OK);
        let (status, body) = self.find(key, version).await;
        assert_eq!(status, StatusCode::OK);
        let body = body.unwrap();
        assert_eq!(body["result"], "hit");
        // Exact hit: the key comes back as the client spelled it.
        assert_eq!(body["cacheKey"], key);
        let (status, got) = self
            .download(body["archiveLocation"].as_str().unwrap())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(got.as_ref(), content);
    }

    /// Finds `keys` and downloads the hit; returns `(cacheKey, bytes)`.
    pub async fn find_and_download(&self, keys: &str, version: &str) -> (String, Bytes) {
        let (status, body) = self.find(keys, version).await;
        assert_eq!(status, StatusCode::OK, "expected hit for {keys}");
        let body = body.unwrap();
        let (status, bytes) = self
            .download(body["archiveLocation"].as_str().unwrap())
            .await;
        assert_eq!(status, StatusCode::OK);
        (body["cacheKey"].as_str().unwrap().to_string(), bytes)
    }
}

pub fn random_bytes(n: usize) -> Vec<u8> {
    (0..n).map(|_| rand::random::<u8>()).collect()
}

/// Updated-at timestamps are milliseconds; a short pause keeps
/// "newest first" ordering deterministic between uploads.
pub async fn tick() {
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
}

/// Filesystem adapter whose `upload_stream` / `copy` can be switched to
/// fail, to exercise storage-error paths.
pub struct FlakyStorage {
    inner: Arc<dyn StorageAdapter>,
    pub fail_upload: AtomicBool,
    pub fail_copy: AtomicBool,
}

impl FlakyStorage {
    pub fn new(inner: Arc<dyn StorageAdapter>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fail_upload: AtomicBool::new(false),
            fail_copy: AtomicBool::new(false),
        })
    }
}

fn injected() -> StorageError {
    StorageError::Io(std::io::Error::other("injected failure"))
}

#[async_trait::async_trait]
impl StorageAdapter for FlakyStorage {
    async fn upload_stream(&self, name: &str, body: ByteStream) -> Result<(), StorageError> {
        if self.fail_upload.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.upload_stream(name, body).await
    }
    async fn download_stream(&self, name: &str) -> Result<ByteStream, StorageError> {
        self.inner.download_stream(name).await
    }
    async fn delete_folder(&self, folder: &str) -> Result<(), StorageError> {
        self.inner.delete_folder(folder).await
    }
    async fn count_files_in_folder(&self, folder: &str) -> Result<u64, StorageError> {
        self.inner.count_files_in_folder(folder).await
    }
    async fn list_folder(&self, folder: &str) -> Result<Vec<ObjectInfo>, StorageError> {
        self.inner.list_folder(folder).await
    }
    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        if self.fail_copy.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.copy(from, to).await
    }
    async fn signed_url(&self, name: &str) -> Result<Option<url::Url>, StorageError> {
        self.inner.signed_url(name).await
    }
    async fn clear(&self) -> Result<(), StorageError> {
        self.inner.clear().await
    }
}
