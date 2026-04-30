//! Shared harness for `tests/twirp.rs`. Lives in a subdirectory so
//! Rust's integration-test loader doesn't compile it as its own
//! binary; the consuming `.rs` file pulls it in via `mod twirp_common`.
//!
//! Holds:
//! - a process-wide RSA key (`KEY` / `key()`),
//! - JWT-minting helpers (`mint_token`, `write_token`, `read_only_token`),
//! - app harness (`Harness`, `harness`),
//! - request builder (`post`),
//! - read-only DB count / fetch helpers (`count`, `fetch_string`),
//! - response body decoder (`body_json`).
//!
//! `#![allow(dead_code)]` is set because each test binary that imports
//! this module uses a different subset.

#![allow(dead_code, clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine;
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, LogFormat, StorageConfig};
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::{ByteStream, FilesystemAdapter, StorageAdapter, StorageError};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::{RsaPrivateKey, RsaPublicKey, traits::PublicKeyParts};
use serde_json::{Value, json};
use tempfile::TempDir;
use url::Url;

pub const ISSUER: &str = "https://token.actions.githubusercontent.com";
pub const BASE_PATH: &str = "/twirp/github.actions.results.api.v1.CacheService";

// --- Fixture: one-shot RSA key shared across all tests in the binary ----

struct KeyFixture {
    private_pem: String,
}

static KEY: OnceLock<KeyFixture> = OnceLock::new();

fn key() -> &'static KeyFixture {
    KEY.get_or_init(|| {
        let mut rng = rsa::rand_core::OsRng;
        let private = RsaPrivateKey::new(&mut rng, 2048).expect("generate RSA key");
        let private_pem = private
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .unwrap()
            .to_string();
        KeyFixture { private_pem }
    })
}

/// Stub fetcher that returns a single throwaway JWK so the `JwksCache`
/// has something to hand back even when (under skip-validation) the
/// middleware never actually consults it.
struct StaticFetcher;

#[async_trait::async_trait]
impl JwksFetcher for StaticFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        let mut rng = rsa::rand_core::OsRng;
        let private = RsaPrivateKey::new(&mut rng, 2048).expect("kg");
        let public = RsaPublicKey::from(&private);
        let n = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.n().to_bytes_be());
        let e = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.e().to_bytes_be());
        Ok(vec![JwkEntry {
            kid: "test".into(),
            n,
            e,
        }])
    }
}

pub fn mint_token(scopes_json: &Value, repo_id: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = json!({
        "iss": ISSUER,
        "iat": now,
        "exp": now + 600,
        "ac": serde_json::to_string(scopes_json).unwrap(),
        "repository_id": repo_id,
    });
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test".into());
    let key = EncodingKey::from_rsa_pem(key().private_pem.as_bytes()).unwrap();
    encode(&header, &claims, &key).unwrap()
}

pub fn write_token() -> String {
    mint_token(
        &json!([{"Scope": "refs/heads/main", "Permission": 3}]),
        "42",
    )
}

pub fn read_only_token() -> String {
    mint_token(
        &json!([{"Scope": "refs/heads/main", "Permission": 0}]),
        "42",
    )
}

// --- App harness --------------------------------------------------------

pub struct Harness {
    pub router: axum::Router,
    pub db: Arc<dyn Db>,
    /// The storage adapter the router serves. Exposed so integration
    /// tests can plant or probe files directly when the test asserts
    /// on storage state (issue #72's purge-and-retry suite reaches
    /// for this; the older direct-download tests don't).
    pub storage: Arc<dyn StorageAdapter>,
    pub tmp: TempDir,
}

/// Options controlling `harness_with`. Defaulting here keeps the plain
/// `harness()` call-site unchanged for the tests that don't care about
/// these knobs.
#[derive(Default)]
pub struct HarnessOpts {
    pub enable_direct_downloads: bool,
    /// Override the storage adapter. `None` uses a plain
    /// `FilesystemAdapter` rooted at the harness tmp dir — same as the
    /// default `harness()`.
    pub storage: Option<Arc<dyn StorageAdapter>>,
}

pub async fn harness() -> Harness {
    harness_with(HarnessOpts::default()).await
}

pub async fn harness_with(opts: HarnessOpts) -> Harness {
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let storage: Arc<dyn StorageAdapter> = opts
        .storage
        .unwrap_or_else(|| Arc::new(FilesystemAdapter::new(tmp.path()).unwrap()));
    let jwks = Arc::new(JwksCache::new(Arc::new(StaticFetcher)));
    let config = AppConfig {
        api_base_url: "http://localhost:3000".parse().unwrap(),
        port: 0,
        log_format: LogFormat::Text,
        cache_cleanup_older_than_days: 90,
        disable_cleanup_jobs: true,
        enable_direct_downloads: opts.enable_direct_downloads,
        skip_token_validation: true,
        management_api_key: None,
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
        tmp,
    }
}

/// The fixed URL a `SigningFilesystem` shim hands back from `signed_url`.
/// Deterministic so tests can assert on it exactly.
pub const SIGNED_URL: &str = "https://presigned.test/download?sig=stub";

/// Storage shim that delegates every operation to a
/// `FilesystemAdapter` but returns a fixed presigned-looking URL from
/// `signed_url`. Used by the direct-download tests to exercise the
/// "signer + flag on" branches without standing up `MinIO`.
pub struct SigningFilesystem {
    inner: FilesystemAdapter,
}

impl SigningFilesystem {
    pub fn new(root: &std::path::Path) -> Self {
        Self {
            inner: FilesystemAdapter::new(root).unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl StorageAdapter for SigningFilesystem {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        self.inner.upload_stream(object_name, body).await
    }
    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        self.inner.download_stream(object_name).await
    }
    async fn delete_folder(&self, folder_name: &str) -> Result<(), StorageError> {
        self.inner.delete_folder(folder_name).await
    }
    async fn count_files_in_folder(&self, folder_name: &str) -> Result<u64, StorageError> {
        self.inner.count_files_in_folder(folder_name).await
    }
    async fn signed_url(&self, _object_name: &str) -> Result<Option<Url>, StorageError> {
        Ok(Some(SIGNED_URL.parse().unwrap()))
    }
    async fn clear(&self) -> Result<(), StorageError> {
        self.inner.clear().await
    }
}

/// Storage shim that claims to sign URLs but fails on every call.
/// Models transient S3 signer errors so tests can pin the graceful
/// fallback-to-server-URL path in `get_cache_entry_download_url`.
pub struct FailingSigner {
    inner: FilesystemAdapter,
}

impl FailingSigner {
    pub fn new(root: &std::path::Path) -> Self {
        Self {
            inner: FilesystemAdapter::new(root).unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl StorageAdapter for FailingSigner {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        self.inner.upload_stream(object_name, body).await
    }
    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        self.inner.download_stream(object_name).await
    }
    async fn delete_folder(&self, folder_name: &str) -> Result<(), StorageError> {
        self.inner.delete_folder(folder_name).await
    }
    async fn count_files_in_folder(&self, folder_name: &str) -> Result<u64, StorageError> {
        self.inner.count_files_in_folder(folder_name).await
    }
    async fn signed_url(&self, _object_name: &str) -> Result<Option<Url>, StorageError> {
        Err(StorageError::Io(std::io::Error::other("signer offline")))
    }
    async fn clear(&self) -> Result<(), StorageError> {
        self.inner.clear().await
    }
}

// --- Request / response helpers ----------------------------------------

pub fn post(path: &str, token: Option<&str>, body: &Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    builder
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

pub async fn body_json(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (status, value)
}

/// Read-only `SELECT COUNT(*)` helper running directly against the
/// `SQLite` pool. Test harness is `SQLite`-only, so `as_sqlite_pool()`
/// always returns `Some` here.
pub async fn count(db: &dyn Db, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap()
}

pub async fn fetch_string(db: &dyn Db, sql: &str) -> String {
    sqlx::query_scalar(sql)
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap()
}

// --- Cache-entry seed helpers (shared by twirp_download.rs and friends) -

/// Seeds a cache entry + storage location and returns the entry id.
/// Plants a default part file (see [`seed_cache_entry_with_location`])
/// so the storage probe (#72) treats the entry as healthy.
pub async fn seed_cache_entry(h: &Harness, key: &str, scope: &str, updated_at: i64) -> String {
    seed_cache_entry_with_location(h, key, scope, updated_at)
        .await
        .0
}

/// Seeds a cache entry + storage location and returns
/// `(entry_id, location_id)` so tests can mark the location merged
/// after the fact.
///
/// Plants a default part file under `<folder>/parts/0` so the
/// `GetCacheEntryDownloadURL` storage probe (#72) treats the
/// just-seeded location as healthy. Tests that need a *broken*
/// location (i.e. specifically exercising the purge-and-retry path)
/// use [`seed_cache_entry_with_location_no_storage`] instead.
pub async fn seed_cache_entry_with_location(
    h: &Harness,
    key: &str,
    scope: &str,
    updated_at: i64,
) -> (String, String) {
    let (entry_id, location_id) =
        seed_cache_entry_with_location_no_storage(h, key, scope, updated_at).await;
    plant_part(h, &format!("folder-{location_id}")).await;
    (entry_id, location_id)
}

/// Variant of [`seed_cache_entry_with_location`] that does NOT plant a
/// default part file. Used by the #72 purge-and-retry tests to model
/// a `cache_entries` row whose backing storage has been wiped.
pub async fn seed_cache_entry_with_location_no_storage(
    h: &Harness,
    key: &str,
    scope: &str,
    updated_at: i64,
) -> (String, String) {
    use gha_cache_oxide::db::entities::CacheEntryCoord;
    use gha_cache_oxide::db::id::new_uuid;
    let location_id = new_uuid();
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location(&location_id, &format!("folder-{location_id}"), 1)
        .await
        .unwrap();
    let coord = CacheEntryCoord {
        key,
        version: "v1",
        scope,
        repo_id: "42",
    };
    let _ = tx
        .upsert_cache_entry(coord, &location_id, updated_at)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let entry_id = fetch_string(
        &*h.db,
        &format!(
            "SELECT id FROM cache_entries WHERE key = '{key}' AND scope = '{scope}' \
             AND version = 'v1' AND repoId = '42'"
        ),
    )
    .await;
    (entry_id, location_id)
}

/// Plants a part file under `<folder>/parts/0` via the harness's
/// own storage adapter so the #72 probe sees the same view the
/// handler will.
pub async fn plant_part(h: &Harness, folder: &str) {
    use bytes::Bytes;
    use futures::StreamExt;
    let chunk: Result<Bytes, std::io::Error> = Ok(Bytes::from_static(b"hello"));
    let stream: ByteStream = futures::stream::iter(vec![chunk]).boxed();
    h.storage
        .upload_stream(&format!("{folder}/parts/0"), stream)
        .await
        .unwrap();
}

/// Plants `<folder>/merged` via the harness's storage adapter so
/// the merged-folder probe (#72 case 1) reports non-zero.
pub async fn plant_merged(h: &Harness, folder: &str) {
    use bytes::Bytes;
    use futures::StreamExt;
    let chunk: Result<Bytes, std::io::Error> = Ok(Bytes::from_static(b"merged-bytes"));
    let stream: ByteStream = futures::stream::iter(vec![chunk]).boxed();
    h.storage
        .upload_stream(&format!("{folder}/merged"), stream)
        .await
        .unwrap();
}
