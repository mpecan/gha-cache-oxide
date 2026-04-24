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
use gha_cache_oxide::db::Db;
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::FilesystemAdapter;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::{RsaPrivateKey, RsaPublicKey, traits::PublicKeyParts};
use serde_json::{Value, json};
use tempfile::TempDir;

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
    pub db: Db,
    pub tmp: TempDir,
}

pub async fn harness() -> Harness {
    let tmp = TempDir::new().unwrap();
    let db = Db::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let storage: Arc<dyn gha_cache_oxide::storage::StorageAdapter> =
        Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(StaticFetcher)));
    let config = AppConfig {
        api_base_url: "http://localhost:3000".parse().unwrap(),
        port: 0,
        log_format: LogFormat::Text,
        cache_cleanup_older_than_days: 90,
        disable_cleanup_jobs: true,
        enable_direct_downloads: false,
        skip_token_validation: true,
        management_api_key: None,
        storage: StorageConfig::Filesystem {
            path: tmp.path().to_path_buf(),
        },
        database: DbConfig::Sqlite {
            path: PathBuf::from(":memory:"),
        },
    };
    let state = AppState::new(db.clone(), storage, jwks, config);
    let router = gha_cache_oxide::build_app(state);
    Harness { router, db, tmp }
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

/// Read-only `SELECT COUNT(*)` helper. Uses `Db::begin()` (the only
/// public path for raw queries from outside the crate) and rolls back
/// so the count itself doesn't mutate state.
pub async fn count(db: &Db, sql: &str) -> i64 {
    let mut tx = db.begin().await.unwrap();
    let n: i64 = sqlx::query_scalar(sql)
        .fetch_one(&mut **tx.sqlite_tx().expect("SQLite test harness"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    n
}

pub async fn fetch_string(db: &Db, sql: &str) -> String {
    let mut tx = db.begin().await.unwrap();
    let s: String = sqlx::query_scalar(sql)
        .fetch_one(&mut **tx.sqlite_tx().expect("SQLite test harness"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    s
}
