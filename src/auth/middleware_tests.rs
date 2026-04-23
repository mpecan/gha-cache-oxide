//! Tests for `src/auth/middleware.rs`, split via `#[path]` to keep
//! `middleware.rs` production code under the soft line limit.
//!
//! Each test drives the middleware through `tower::ServiceExt::oneshot`
//! against a tiny probe route that echoes whether a `CacheScope` landed
//! in request extensions.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::sync::{Arc, OnceLock};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::{Router, middleware as axum_mw};
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde_json::json;
use tower::ServiceExt;

use super::{AuthError, CacheScope, GITHUB_ISSUER};
use crate::auth::{JwkEntry, JwksCache, JwksFetcher, require_github_token};
use crate::config::{AppConfig, DbConfig, LogFormat, StorageConfig};
use crate::db::Db;
use crate::state::AppState;
use crate::storage::FilesystemAdapter;

// ---------------------------------------------------------------------------
// Shared key + JWK fixture — generated once per process via OnceLock so the
// ~200ms RSA keygen happens once rather than per test.
// ---------------------------------------------------------------------------

struct KeyFixture {
    private_pem: String,
    jwk: JwkEntry,
}

static KEY_FIXTURE: OnceLock<KeyFixture> = OnceLock::new();

fn key_fixture() -> &'static KeyFixture {
    KEY_FIXTURE.get_or_init(|| {
        let mut rng = rsa::rand_core::OsRng;
        let private = RsaPrivateKey::new(&mut rng, 2048).expect("generate RSA key");
        let private_pem = private
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .unwrap()
            .to_string();
        let public = RsaPublicKey::from(&private);
        let n = base64url_noopad(&public.n().to_bytes_be());
        let e = base64url_noopad(&public.e().to_bytes_be());
        KeyFixture {
            private_pem,
            jwk: JwkEntry {
                kid: "test-kid".to_string(),
                n,
                e,
            },
        }
    })
}

fn base64url_noopad(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

// ---------------------------------------------------------------------------
// Fetcher that returns a fixed set of entries and counts calls.
// ---------------------------------------------------------------------------

struct StaticFetcher {
    entries: std::sync::Mutex<Vec<JwkEntry>>,
    calls: std::sync::atomic::AtomicUsize,
}

impl StaticFetcher {
    fn new(entries: Vec<JwkEntry>) -> Arc<Self> {
        Arc::new(Self {
            entries: std::sync::Mutex::new(entries),
            calls: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn set(&self, entries: Vec<JwkEntry>) {
        *self.entries.lock().unwrap() = entries;
    }

    fn call_count(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl JwksFetcher for StaticFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.entries.lock().unwrap().clone())
    }
}

// ---------------------------------------------------------------------------
// Token builder.
// ---------------------------------------------------------------------------

fn mint_token(kid: &str, claims: serde_json::Value, issuer: &str) -> String {
    let fixture = key_fixture();
    mint_token_with_key(kid, claims, issuer, &fixture.private_pem)
}

fn mint_token_with_key(
    kid: &str,
    mut claims: serde_json::Value,
    issuer: &str,
    private_pem: &str,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let obj = claims.as_object_mut().expect("claims is a JSON object");
    obj.entry("iss").or_insert_with(|| json!(issuer));
    obj.entry("iat").or_insert_with(|| json!(now));
    obj.entry("exp").or_insert_with(|| json!(now + 600));
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_string());
    let key = EncodingKey::from_rsa_pem(private_pem.as_bytes()).expect("load RSA key");
    encode(&header, &claims, &key).expect("sign JWT")
}

// ---------------------------------------------------------------------------
// App / state helpers.
// ---------------------------------------------------------------------------

struct TestHarness {
    router: Router,
    _tmp: tempfile::TempDir,
    fetcher: Arc<StaticFetcher>,
}

async fn build_harness(entries: Vec<JwkEntry>, skip_validation: bool) -> TestHarness {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = Db::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let storage = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let fetcher = StaticFetcher::new(entries);
    let jwks = Arc::new(JwksCache::new(fetcher.clone()));
    let config = AppConfig {
        api_base_url: "http://localhost:3000".parse().unwrap(),
        port: 0,
        log_format: LogFormat::Text,
        cache_cleanup_older_than_days: 90,
        disable_cleanup_jobs: true,
        enable_direct_downloads: false,
        skip_token_validation: skip_validation,
        management_api_key: None,
        storage: StorageConfig::Filesystem {
            path: tmp.path().to_path_buf(),
        },
        database: DbConfig::Sqlite {
            path: std::path::PathBuf::from(":memory:"),
        },
    };
    let state = AppState::new(db, storage, jwks, config);
    let router = Router::new()
        .route("/echo", get(echo_scope))
        .layer(axum_mw::from_fn_with_state(
            state.clone(),
            require_github_token,
        ))
        .with_state(state);
    TestHarness {
        router,
        _tmp: tmp,
        fetcher,
    }
}

/// Probe route: echoes the `CacheScope` attached by the middleware.
/// Uses a direct `Request<Body>` extraction to read extensions manually —
/// avoids any typing subtlety around the `Extension` extractor that
/// might conflict with our `from_fn_with_state` usage below.
async fn echo_scope(req: axum::http::Request<axum::body::Body>) -> impl IntoResponse {
    let scope = req
        .extensions()
        .get::<CacheScope>()
        .cloned()
        .expect("middleware must insert CacheScope on success");
    Json(json!({
        "repo_id": scope.repo_id,
        "scopes": scope.scopes.iter().map(|s| json!({
            "Scope": s.scope,
            "Permission": s.permission,
        })).collect::<Vec<_>>(),
    }))
}

fn get_req(path: &str, headers: &[(&'static str, String)]) -> Request<Body> {
    let mut builder = Request::builder().method(Method::GET).uri(path);
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    builder.body(Body::empty()).unwrap()
}

fn ok_claims() -> serde_json::Value {
    json!({
        "ac": serde_json::to_string(&json!([
            {"Scope": "refs/heads/main", "Permission": 3},
        ])).unwrap(),
        "repository_id": "42",
    })
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn valid_token_populates_cache_scope() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(&fixture.jwk.kid, ok_claims(), GITHUB_ISSUER);

    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["repo_id"], "42");
    assert_eq!(json["scopes"][0]["Scope"], "refs/heads/main");
    assert_eq!(json["scopes"][0]["Permission"], 3);
}

#[tokio::test]
async fn missing_authorization_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let response = harness.router.oneshot(get_req("/echo", &[])).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["statusCode"], 401);
    assert!(json["message"].as_str().unwrap().contains("missing"));
}

#[tokio::test]
async fn non_bearer_authorization_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), "Basic foo".to_string())],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn invalid_signature_returns_401() {
    let fixture = key_fixture();
    // Mint the token with a DIFFERENT private key — signature won't
    // match `fixture.jwk`.
    let mut rng = rsa::rand_core::OsRng;
    let wrong_private = RsaPrivateKey::new(&mut rng, 2048).unwrap();
    let wrong_pem = wrong_private
        .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
        .unwrap()
        .to_string();
    let token = mint_token_with_key(&fixture.jwk.kid, ok_claims(), GITHUB_ISSUER, &wrong_pem);

    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn wrong_issuer_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(&fixture.jwk.kid, ok_claims(), "https://evil.example");
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn missing_ac_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(
        &fixture.jwk.kid,
        json!({ "repository_id": "42" }),
        GITHUB_ISSUER,
    );
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["message"].as_str().unwrap().contains("cache scopes"));
}

#[tokio::test]
async fn invalid_scopes_json_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(
        &fixture.jwk.kid,
        json!({
            "ac": "not-valid-json",
            "repository_id": "42",
        }),
        GITHUB_ISSUER,
    );
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn empty_scopes_array_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(
        &fixture.jwk.kid,
        json!({
            "ac": "[]",
            "repository_id": "42",
        }),
        GITHUB_ISSUER,
    );
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["message"]
            .as_str()
            .unwrap()
            .contains("any cache scopes")
    );
}

#[tokio::test]
async fn missing_repository_id_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(
        &fixture.jwk.kid,
        json!({
            "ac": serde_json::to_string(&json!([{"Scope": "s", "Permission": 1}])).unwrap(),
        }),
        GITHUB_ISSUER,
    );
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["message"].as_str().unwrap().contains("repository id"));
}

#[tokio::test]
async fn skip_validation_decodes_without_verifying() {
    // Different kid, different private key — signature is meaningless.
    // With skip_validation the middleware must still accept the token
    // and populate the scope.
    let mut rng = rsa::rand_core::OsRng;
    let unrelated = RsaPrivateKey::new(&mut rng, 2048).unwrap();
    let unrelated_pem = unrelated
        .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
        .unwrap()
        .to_string();

    // Empty fetcher — wouldn't matter, auth bypassed.
    let harness = build_harness(vec![], true).await;
    let before = super::skip_warning_count();

    let token = mint_token_with_key("doesnt-matter", ok_claims(), GITHUB_ISSUER, &unrelated_pem);
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let after = super::skip_warning_count();
    assert!(
        after == before || after == before + 1,
        "warning counter should increment by at most one; before={before} after={after}"
    );
    // Further calls must not re-emit the warning regardless.
    let snapshot = super::skip_warning_count();
    let _ = harness; // ensure harness dropped before we mint another token
    let harness2 = build_harness(vec![], true).await;
    let token2 = mint_token_with_key("other", ok_claims(), GITHUB_ISSUER, &unrelated_pem);
    let _ = harness2
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token2}"))],
        ))
        .await
        .unwrap();
    assert_eq!(
        super::skip_warning_count(),
        snapshot,
        "warn should fire at most once per process lifetime"
    );
}

#[tokio::test]
async fn jwks_cache_refreshes_on_kid_miss() {
    let fixture = key_fixture();
    // Two kids share the same key material (cheapest way to exercise
    // the refetch path with a valid signature both times).
    let kid_a = JwkEntry {
        kid: "kid-A".to_string(),
        n: fixture.jwk.n.clone(),
        e: fixture.jwk.e.clone(),
    };
    let kid_b = JwkEntry {
        kid: "kid-B".to_string(),
        n: fixture.jwk.n.clone(),
        e: fixture.jwk.e.clone(),
    };

    let harness = build_harness(vec![kid_a.clone()], false).await;
    // First call with kid-A — cache empty, one fetch.
    let token_a = mint_token("kid-A", ok_claims(), GITHUB_ISSUER);
    let resp_a = harness
        .router
        .clone()
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token_a}"))],
        ))
        .await
        .unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);
    assert_eq!(harness.fetcher.call_count(), 1);

    // Switch the fetcher to return both kids (as GitHub would during
    // key rotation). kid-B forces a second fetch.
    harness.fetcher.set(vec![kid_a, kid_b]);

    let token_b = mint_token("kid-B", ok_claims(), GITHUB_ISSUER);
    let resp_b = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token_b}"))],
        ))
        .await
        .unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);
    assert_eq!(harness.fetcher.call_count(), 2);
}

// ---------------------------------------------------------------------------
// Extra edge cases flagged by review: time-based validation (exp/nbf),
// empty-string rep/ac, post-review remediations.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expired_token_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    // Mint with exp in the past — jsonwebtoken validates exp by default
    // and should reject.
    let mut claims = ok_claims();
    claims["exp"] = json!(1_000_000_000_u64); // Sun, 9 Sep 2001 01:46:40 GMT
    claims["iat"] = json!(999_999_000_u64);
    let token = mint_token(&fixture.jwk.kid, claims, GITHUB_ISSUER);
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn not_yet_valid_token_returns_401() {
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    // nbf far in the future — valid signature + issuer, but not yet
    // usable. jsonwebtoken enforces nbf when `validate_nbf` is on (off
    // by default — but we don't explicitly disable it; jsonwebtoken
    // 9's default enforces nbf ONLY when the claim is present).
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60 * 60 * 24 * 365; // one year ahead
    let mut claims = ok_claims();
    claims["nbf"] = json!(future);
    claims["exp"] = json!(future + 600);
    let token = mint_token(&fixture.jwk.kid, claims, GITHUB_ISSUER);
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn empty_repository_id_returns_401() {
    // Distinct from the "missing" case — `repository_id` is present
    // but empty. `build_cache_scope` rejects that explicitly.
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(
        &fixture.jwk.kid,
        json!({
            "ac": serde_json::to_string(&json!([{"Scope": "s", "Permission": 1}])).unwrap(),
            "repository_id": "",
        }),
        GITHUB_ISSUER,
    );
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["message"].as_str().unwrap().contains("repository id"));
}

#[tokio::test]
async fn empty_ac_string_returns_401_as_invalid_json() {
    // Distinct from `ac: "[]"` (empty array → EmptyScopes); `ac: ""`
    // is not valid JSON at all → InvalidScopesJson → 401.
    let fixture = key_fixture();
    let harness = build_harness(vec![fixture.jwk.clone()], false).await;
    let token = mint_token(
        &fixture.jwk.kid,
        json!({
            "ac": "",
            "repository_id": "42",
        }),
        GITHUB_ISSUER,
    );
    let response = harness
        .router
        .oneshot(get_req(
            "/echo",
            &[(header::AUTHORIZATION.as_str(), format!("Bearer {token}"))],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["message"].as_str().unwrap().contains("Invalid JSON"));
}
