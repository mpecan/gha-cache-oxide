//! Server harness + request helpers for the E2E tests.
//!
//! - `spawn_server` binds `127.0.0.1:0`, configures an `AppConfig` with
//!   `api_base_url` pointing at the real bound socket, and runs
//!   `axum::serve` on a tokio task with graceful shutdown.
//! - `ServerHandle` owns the task; `Drop` signals shutdown and aborts.
//! - `post_json`, `put_bytes`, `get` are thin `reqwest` wrappers.
//! - `blockid_48` mirrors the 48-byte layout `actions/cache` uses.
//! - Token minting reuses the RSA-key fixture pattern from
//!   `tests/twirp_common/mod.rs` — the fixture is test-local because
//!   Rust integration-test binaries don't share compiled modules.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use axum::Router;
use base64::Engine;
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, StorageConfig};
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::FilesystemAdapter;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::{Client, Response};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::{RsaPrivateKey, RsaPublicKey, traits::PublicKeyParts};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::ISSUER;

// --- RSA key fixture ---------------------------------------------------

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

/// Throwaway JWKS fetcher — the JWKS cache is never consulted under
/// `skip_token_validation: true` but must still be constructable.
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

// --- Server harness ----------------------------------------------------

/// Owns a running axum server bound to a random 127.0.0.1 port. Dropping
/// the handle triggers graceful shutdown and aborts the serve task.
pub struct ServerHandle {
    pub base_url: String,
    pub http: Client,
    pub token: String,
    pub tmp: TempDir,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<std::io::Result<()>>>,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.join.take() {
            h.abort();
        }
    }
}

/// Binds `127.0.0.1:0`, resolves the random port, spawns `axum::serve`
/// with graceful shutdown wired to the returned [`ServerHandle`].
///
/// `api_base_url` is set to the real bound URL so the signed-upload and
/// signed-download URLs returned by the Twirp handlers resolve back
/// here, letting `reqwest` follow them verbatim.
pub async fn spawn_server() -> ServerHandle {
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let storage: Arc<dyn gha_cache_oxide::storage::StorageAdapter> =
        Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(StaticFetcher)));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let base_url = format!("http://{local_addr}");

    let config = AppConfig {
        api_base_url: base_url.parse().unwrap(),
        port: local_addr.port(),
        ..AppConfig::test_defaults(
            StorageConfig::Filesystem {
                path: tmp.path().to_path_buf(),
            },
            DbConfig::Sqlite {
                path: PathBuf::from(":memory:"),
            },
        )
    };
    let state = AppState::new(db, storage, jwks, config);
    let app: Router = gha_cache_oxide::build_app(state);

    let (tx, rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
    });

    wait_until_listening(&local_addr).await;

    ServerHandle {
        base_url,
        http: Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        token: write_token(),
        tmp,
        shutdown: Some(tx),
        join: Some(join),
    }
}

async fn wait_until_listening(addr: &std::net::SocketAddr) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("server failed to accept connections on {addr} within 5s");
}

// --- Request helpers ---------------------------------------------------

pub async fn post_json(srv: &ServerHandle, path: &str, body: &Value) -> Response {
    srv.http
        .post(format!("{}{path}", srv.base_url))
        .bearer_auth(&srv.token)
        .json(body)
        .send()
        .await
        .unwrap()
}

pub async fn put_bytes(srv: &ServerHandle, path: &str, body: Vec<u8>) -> Response {
    srv.http
        .put(format!("{}{path}", srv.base_url))
        .body(body)
        .send()
        .await
        .unwrap()
}

pub async fn get(srv: &ServerHandle, path: &str) -> Response {
    srv.http
        .get(format!("{}{path}", srv.base_url))
        .send()
        .await
        .unwrap()
}

/// 48-byte block id matching the `actions/cache` layout:
/// UUID (36 chars) + zero-padded decimal index (12 chars) → base64.
pub fn blockid_48(index: u64) -> String {
    let uuid = "11111111-2222-3333-4444-555555555555";
    let buf = format!("{uuid}{index:012}");
    assert_eq!(buf.len(), 48);
    base64::engine::general_purpose::STANDARD.encode(buf.as_bytes())
}
