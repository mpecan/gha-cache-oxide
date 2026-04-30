//! Integration tests for the catch-all fallback proxy (issue #24).
//!
//! Drives the full router via `tower::ServiceExt::oneshot`. The
//! upstream "results receiver" is mocked with `wiremock` so we can
//! assert exactly what the upstream sees.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, StorageConfig};
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::FilesystemAdapter;
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::matchers::{body_string, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct NullFetcher;
#[async_trait::async_trait]
impl JwksFetcher for NullFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        Ok(Vec::new())
    }
}

struct Harness {
    router: axum::Router,
    _tmp: TempDir,
}

async fn harness_with_upstream(upstream: &str) -> Harness {
    harness_with_overrides(upstream, 16 * 1024 * 1024).await
}

async fn harness_with_overrides(upstream: &str, max_body: usize) -> Harness {
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let storage = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(NullFetcher)));
    let config = AppConfig {
        default_actions_results_url: upstream.parse().unwrap(),
        proxy_max_request_body_bytes: max_body,
        ..AppConfig::test_defaults(
            StorageConfig::Filesystem {
                path: tmp.path().to_path_buf(),
            },
            DbConfig::Sqlite {
                path: PathBuf::from(":memory:"),
            },
        )
    };
    let router = gha_cache_oxide::build_app(AppState::new(db, storage, jwks, config));
    Harness { router, _tmp: tmp }
}

fn req(method: Method, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .unwrap()
}

async fn body_bytes(resp: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

// ---------- forwards GET preserving path -------------------------------

#[tokio::test]
async fn forwards_get_to_upstream_with_path() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/runs/123"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"forwarded": true})),
        )
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/api/v3/runs/123", Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(body["forwarded"], serde_json::Value::Bool(true));
}

// ---------- forwards request method + body -----------------------------

#[tokio::test]
async fn forwards_post_with_body() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/receiver/foo"))
        .and(body_string("hello-bench"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let resp = h
        .router
        .oneshot(req(
            Method::POST,
            "/receiver/foo",
            Body::from("hello-bench"),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

// ---------- preserves upstream status codes ----------------------------

#[tokio::test]
async fn forwards_upstream_404() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404).set_body_string("nope"))
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/anything", Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_bytes(resp).await, b"nope".to_vec());
}

// ---------- query string forwarded -------------------------------------

#[tokio::test]
async fn forwards_query_string() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/search"))
        .and(wiremock::matchers::query_param("q", "rust"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/search?q=rust", Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ---------- hop-by-hop request headers stripped ------------------------

#[tokio::test]
async fn does_not_forward_hop_by_hop_request_headers() {
    let mock = MockServer::start().await;
    // wiremock's `header_exists` would fail the mock if the header
    // arrives; we want a NEGATIVE assertion. Use a custom matcher that
    // passes only when Connection is absent.
    Mock::given(method("GET"))
        .and(path("/check-headers"))
        .and(NoConnectionHeader)
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let mut request = req(Method::GET, "/check-headers", Body::empty());
    request
        .headers_mut()
        .insert(header::CONNECTION, "close".parse().unwrap());
    let resp = h.router.oneshot(request).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "upstream must not have seen `Connection: close`",
    );
}

// ---------- response status codes mirrored ----------------------------

#[tokio::test]
async fn forwards_upstream_500() {
    // Upstream 5xx must propagate verbatim — not collapse into our own
    // 502 (which is reserved for transport failures).
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).set_body_string("svc down"))
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/anything", Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_bytes(resp).await, b"svc down".to_vec());
}

// ---------- response headers forwarded except hop-by-hop ---------------

#[tokio::test]
async fn forwards_response_headers_and_strips_hop_by_hop() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/x-test")
                .insert_header("x-custom-passthrough", "yes")
                // Hop-by-hop: must NOT propagate to the axum response.
                .insert_header("transfer-encoding", "chunked")
                .insert_header("connection", "close"),
        )
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/headers", Body::empty()))
        .await
        .unwrap();
    let headers = resp.headers().clone();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(headers.get("content-type").unwrap(), "application/x-test");
    assert_eq!(headers.get("x-custom-passthrough").unwrap(), "yes");
    assert!(
        headers.get("transfer-encoding").is_none(),
        "Transfer-Encoding must be stripped (RFC 7230 §6.1)",
    );
    assert!(
        headers.get("connection").is_none(),
        "Connection must be stripped (RFC 7230 §6.1)",
    );
}

// ---------- 502 on upstream connect failure ----------------------------

#[tokio::test]
async fn returns_502_on_connect_failure() {
    // Port 1 is reserved + closed; reqwest reports a transport error.
    let h = harness_with_upstream("http://127.0.0.1:1").await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/down", Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}

// ---------- explicit routes still win -----------------------------------

#[tokio::test]
async fn health_route_does_not_fall_through_to_proxy() {
    // Upstream is configured but should never be hit — `/health` is
    // explicit in `build_app`.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/health", Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(body, serde_json::json!({"ok": true}));
}

// ---------- 413 on oversized body --------------------------------------

#[tokio::test]
async fn rejects_oversized_request_body() {
    let mock = MockServer::start().await;
    // The proxy should never reach the upstream when the body exceeds
    // the limit — guard against false-positive PASSes by setting an
    // expected zero-call mock.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock)
        .await;

    let h = harness_with_upstream(&mock.uri()).await;
    // 16 MiB + 1 byte — one byte over the default proxy_max_request_body_bytes.
    let oversized = vec![0u8; 16 * 1024 * 1024 + 1];
    let resp = h
        .router
        .oneshot(req(Method::POST, "/big", Body::from(oversized)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn body_cap_is_configurable() {
    // Pin the AppConfig.proxy_max_request_body_bytes plumbing: a 1 KiB
    // cap rejects a 1025-byte body but lets a 1024-byte body through.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/under-cap"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock)
        .await;

    let h = harness_with_overrides(&mock.uri(), 1024).await;

    // Just-at-cap: passes through to the upstream → 200.
    let just_at_cap = vec![0u8; 1024];
    let resp = h
        .router
        .clone()
        .oneshot(req(Method::POST, "/under-cap", Body::from(just_at_cap)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // One-over-cap: 413, never reaches upstream.
    let oversized = vec![0u8; 1025];
    let resp = h
        .router
        .oneshot(req(Method::POST, "/under-cap", Body::from(oversized)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

// ---------- helpers -----------------------------------------------------

/// wiremock matcher that passes only when the incoming request has no
/// `Connection` header. Used to assert hop-by-hop stripping.
struct NoConnectionHeader;

impl wiremock::Match for NoConnectionHeader {
    fn matches(&self, request: &wiremock::Request) -> bool {
        !request
            .headers
            .keys()
            .any(|name| name.as_str().eq_ignore_ascii_case("connection"))
    }
}
