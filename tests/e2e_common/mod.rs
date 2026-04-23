//! Shared harness for `tests/e2e.rs` and `tests/upstream_diff.rs`.
//!
//! Boots the server on a random port, mints a JWT, drives the five
//! protocol routes via `reqwest`, and compares normalized responses
//! against JSON golden files under `tests/golden/`.
//!
//! # Golden files
//!
//! - Each envelope produced by [`capture_envelope`] is normalized (the
//!   dynamic `upload_id`, cache-entry UUID and `x-ms-request-id` values
//!   are replaced by placeholders) before it hits disk or the
//!   comparator. The resulting JSON encodes the *shape* of a response,
//!   not its per-run values.
//! - Run the test binary once with `UPDATE_GOLDEN=1` to regenerate the
//!   files when a response shape intentionally changes. Without the
//!   flag, `assert_golden` diffs actual vs stored and panics on
//!   mismatch with a pretty line-oriented diff.
//!
//! # Lives here, not in `twirp_common/`
//!
//! The existing `tests/twirp_common/mod.rs` is oriented around
//! `tower::ServiceExt::oneshot` — it never binds a socket. Rust
//! integration tests each compile as their own binary, so sharing
//! across sibling `tests/*_common/mod.rs` directories is awkward;
//! duplicating the RSA-key fixture and JWT helper is confined to test
//! code and keeps each harness self-contained.

#![allow(dead_code, clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use axum::Router;
use base64::Engine;
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, LogFormat, StorageConfig};
use gha_cache_oxide::db::Db;
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

pub const ISSUER: &str = "https://token.actions.githubusercontent.com";
pub const BASE_PATH: &str = "/twirp/github.actions.results.api.v1.CacheService";

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
    let db = Db::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let storage: Arc<dyn gha_cache_oxide::storage::StorageAdapter> =
        Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(StaticFetcher)));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let base_url = format!("http://{local_addr}");

    let config = AppConfig {
        api_base_url: base_url.parse().unwrap(),
        port: local_addr.port(),
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

    // Wait for the port to accept connections. The listener is already
    // bound, so connect() returns as soon as axum polls accept() at
    // least once — usually within a few microseconds.
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

// --- Envelope + normalization -----------------------------------------

/// A JSON value that must be stripped from golden comparisons because
/// it changes per run.
#[derive(Clone)]
pub struct Dynamics {
    pub upload_id: Option<i64>,
    pub cache_entry_id: Option<String>,
}

/// Keys we consider dynamic — the upload base URL, cache-entry UUID in
/// the download URL, and the per-request `x-ms-request-id` header.
impl Dynamics {
    pub const fn none() -> Self {
        Self {
            upload_id: None,
            cache_entry_id: None,
        }
    }
}

/// Wraps the reqwest response as a JSON envelope:
/// `{status, headers: {...}, body: <parsed-json|null>}`. Only the
/// headers listed in `include_headers` are surfaced — so CI-variant
/// headers like `content-length`, `date` and `connection` don't leak
/// into goldens.
pub async fn capture_envelope(
    resp: Response,
    include_headers: &[&str],
    body_is_json: bool,
) -> (u16, Value) {
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let bytes = resp.bytes().await.unwrap();

    let mut hmap = serde_json::Map::new();
    for name in include_headers {
        if let Some(v) = headers.get(*name) {
            hmap.insert(
                (*name).to_string(),
                Value::String(v.to_str().unwrap().to_string()),
            );
        }
    }

    // Fail loudly on invalid JSON — collapsing to `null` would let a
    // broken serialization regression slip past the golden check.
    let body = if bytes.is_empty() {
        Value::Null
    } else if body_is_json {
        serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            panic!(
                "expected JSON body, got error {e}; preview: {}",
                String::from_utf8_lossy(&bytes)
            );
        })
    } else {
        json!({ "body_len": bytes.len() })
    };

    let envelope = json!({
        "status": status,
        "headers": Value::Object(hmap),
        "body": body,
    });
    (status, envelope)
}

/// Recursively rewrites dynamic values to stable placeholders so goldens
/// don't change between runs. Replacements: numeric / stringified
/// `upload_id` → `<UPLOAD_ID>`; cache-entry UUID → `<CACHE_ENTRY_ID>`;
/// `x-ms-request-id` header values → `<REQUEST_ID>`; server base URL
/// → `<BASE_URL>`. URL substrings for upload/download paths are
/// replaced wholesale so the surrounding URL stays readable.
pub fn normalize(value: Value, base_url: &str, dynamics: &Dynamics) -> Value {
    normalize_inner(value, base_url, dynamics, None)
}

fn normalize_inner(value: Value, base_url: &str, d: &Dynamics, parent_key: Option<&str>) -> Value {
    match value {
        Value::String(s) => {
            let s = normalize_string(&s, base_url, d);
            if parent_key == Some("x-ms-request-id") {
                Value::String(PH_REQUEST_ID.into())
            } else {
                Value::String(s)
            }
        }
        Value::Number(n) => {
            if let (Some(id), Some(n_val)) = (d.upload_id, n.as_i64())
                && id == n_val
            {
                Value::String(PH_UPLOAD_ID.into())
            } else {
                Value::Number(n)
            }
        }
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .map(|v| normalize_inner(v, base_url, d, None))
                .collect(),
        ),
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                let child = normalize_inner(v, base_url, d, Some(k.as_str()));
                out.insert(k, child);
            }
            Value::Object(out)
        }
        other => other,
    }
}

// Placeholders used in golden files. Kept as consts so clippy doesn't
// mistake `"{foo}"` literals for misplaced format args.
const PH_UPLOAD_ID: &str = "<UPLOAD_ID>";
const PH_CACHE_ENTRY_ID: &str = "<CACHE_ENTRY_ID>";
const PH_BASE_URL: &str = "<BASE_URL>";
const PH_REQUEST_ID: &str = "<REQUEST_ID>";

fn normalize_string(s: &str, base_url: &str, d: &Dynamics) -> String {
    let mut out = s.to_string();
    // Replace upload base URL paths first, then the raw base URL, then
    // the cache_entry id, so the more-specific replacements win.
    if let Some(id) = d.upload_id {
        let needle = format!("/devstoreaccount1/upload/{id}");
        let repl = format!("/devstoreaccount1/upload/{PH_UPLOAD_ID}");
        out = out.replace(&needle, &repl);
        let stringified = id.to_string();
        if out == stringified {
            return PH_UPLOAD_ID.to_string();
        }
    }
    if let Some(ref cid) = d.cache_entry_id {
        let needle = format!("/download/{cid}");
        let repl = format!("/download/{PH_CACHE_ENTRY_ID}");
        out = out.replace(&needle, &repl);
        if out == *cid {
            return PH_CACHE_ENTRY_ID.to_string();
        }
    }
    out = out.replace(base_url, PH_BASE_URL);
    out
}

// --- Golden comparator -------------------------------------------------

/// Returns the repo-relative path of a golden file. Kept out of the
/// public surface because callers should use [`assert_golden`].
fn golden_path(label: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(format!("{label}.json"))
}

/// Compares `actual` against the golden file at `tests/golden/<label>.json`.
/// Under `UPDATE_GOLDEN=1` writes `actual` to the file instead of
/// asserting — the canonical way to regenerate goldens.
///
/// On mismatch, panics with a line-oriented diff of
/// `serde_json::to_string_pretty(expected)` vs `actual`.
pub fn assert_golden(label: &str, actual: &Value) {
    let path = golden_path(label);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let parent = path.parent().unwrap();
        std::fs::create_dir_all(parent).unwrap();
        let pretty = serde_json::to_string_pretty(actual).unwrap();
        std::fs::write(&path, format!("{pretty}\n")).unwrap();
        return;
    }
    let expected_raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "missing golden file {} ({e}) — run once with UPDATE_GOLDEN=1 to populate",
            path.display()
        )
    });
    let expected: Value = serde_json::from_str(&expected_raw).unwrap();
    compare_to_golden_value(&expected, actual, label);
}

/// Pretty-prints a diff and panics if the two values aren't equal. Kept
/// separate from `assert_golden` so the comparator self-test can drive
/// it without touching the filesystem.
pub fn compare_to_golden_value(expected: &Value, actual: &Value, label: &str) {
    if expected == actual {
        return;
    }
    let expected_str = serde_json::to_string_pretty(expected).unwrap();
    let actual_str = serde_json::to_string_pretty(actual).unwrap();
    let diff = line_diff(&expected_str, &actual_str);
    panic!("golden mismatch for `{label}` (run with UPDATE_GOLDEN=1 to regenerate):\n{diff}");
}

/// Minimal line-oriented diff good enough to tell a human which lines
/// differ. Avoids adding a `similar`/`difference` crate for a test
/// helper that's only read on failure.
fn line_diff(a: &str, b: &str) -> String {
    let a_lines: Vec<&str> = a.lines().collect();
    let b_lines: Vec<&str> = b.lines().collect();
    let max = a_lines.len().max(b_lines.len());
    let mut out = String::new();
    for i in 0..max {
        let al = a_lines.get(i).copied().unwrap_or("");
        let bl = b_lines.get(i).copied().unwrap_or("");
        if al == bl {
            out.push_str("  ");
            out.push_str(al);
        } else {
            out.push_str("- ");
            out.push_str(al);
            out.push('\n');
            out.push_str("+ ");
            out.push_str(bl);
        }
        out.push('\n');
    }
    out
}

// --- Extraction helpers for the full-flow test -------------------------

/// Returns the `upload_id` embedded in a `signed_upload_url` under the
/// given base URL. Panics if the URL doesn't follow the expected shape —
/// that's a protocol bug and the test should fail loudly.
pub fn extract_upload_id(create_entry_body: &Value, base_url: &str) -> i64 {
    let url = create_entry_body["signed_upload_url"]
        .as_str()
        .expect("signed_upload_url missing");
    let prefix = format!("{base_url}/devstoreaccount1/upload/");
    url.strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("expected {prefix}<id>, got {url}"))
        .parse()
        .unwrap()
}

pub fn extract_cache_entry_id(download_url_body: &Value, base_url: &str) -> String {
    let url = download_url_body["signed_download_url"]
        .as_str()
        .expect("signed_download_url missing");
    let prefix = format!("{base_url}/download/");
    url.strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("expected {prefix}<id>, got {url}"))
        .to_string()
}
