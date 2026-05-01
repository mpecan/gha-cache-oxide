//! Integration tests for the `OpenAPI` spec endpoint (issue #77 part 1).
//!
//! Three pins:
//! 1. The endpoint returns a syntactically valid `OpenAPI` 3.1 doc that
//!    references every management endpoint.
//! 2. The committed snapshot at `docs/openapi.json` matches what the
//!    server emits live (regenerate with `OPENAPI_REGENERATE=1`).
//! 3. The endpoint sits behind the same Bearer-auth wall as the rest
//!    of `/management/...` (no token → 401).

use axum::http::{Method, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use super::common::{KEY, body_json, harness, req};

const SPEC_PATH: &str = "/management/_docs/spec.json";
const SNAPSHOT_PATH: &str = "docs/openapi.json";

/// Pretty-prints `value` with 2-space indentation + a trailing newline,
/// matching the shape `serde_json::to_string_pretty` produces (used as
/// the canonical on-disk form).
fn canonical_json(value: &Value) -> String {
    let mut s = serde_json::to_string_pretty(value).unwrap();
    s.push('\n');
    s
}

#[tokio::test]
async fn spec_endpoint_returns_valid_openapi_3_1() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, SPEC_PATH, Some(KEY)))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Top-level fields per the `OpenAPI` 3.1 spec.
    assert!(
        body["openapi"].as_str().unwrap_or("").starts_with("3.1"),
        "expected openapi 3.1.x, got {:?}",
        body["openapi"]
    );
    assert!(body["info"]["title"].is_string());
    assert!(body["info"]["version"].is_string());

    // Every management endpoint is represented somewhere in `paths`.
    let paths = body["paths"].as_object().expect("paths must be an object");
    // The spec endpoint itself is intentionally undocumented — it's a
    // bootstrap path and doesn't belong in the operator-visible
    // surface.
    for required in [
        "/cache-entries",
        "/cache-entries/{id}",
        "/cache-entries/match",
        "/storage-locations",
        "/storage-locations/{id}",
        "/cleanup/trigger",
    ] {
        assert!(
            paths.contains_key(required),
            "spec is missing required path {required:?}; got keys {:?}",
            paths.keys().collect::<Vec<_>>(),
        );
    }

    // Bearer security scheme is registered.
    let bearer = &body["components"]["securitySchemes"]["bearer"];
    assert_eq!(bearer["type"], "http");
    assert_eq!(bearer["scheme"], "bearer");
}

/// Serves both as a pin against accidental drift AND as the
/// regeneration command:
/// `OPENAPI_REGENERATE=1 cargo test --test management spec_snapshot_matches_committed_file`
/// rewrites `docs/openapi.json` from the live spec.
#[tokio::test]
async fn spec_snapshot_matches_committed_file() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, SPEC_PATH, Some(KEY)))
        .await
        .unwrap();
    let (status, live) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    let live_pretty = canonical_json(&live);

    let snapshot_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(SNAPSHOT_PATH);

    if std::env::var_os("OPENAPI_REGENERATE").is_some() {
        std::fs::write(&snapshot_path, &live_pretty)
            .unwrap_or_else(|e| panic!("failed to write {SNAPSHOT_PATH}: {e}"));
        eprintln!("wrote {} ({} bytes)", SNAPSHOT_PATH, live_pretty.len());
        return;
    }

    let committed = std::fs::read_to_string(&snapshot_path).unwrap_or_else(|e| {
        panic!(
            "missing {SNAPSHOT_PATH}: {e} — run with `OPENAPI_REGENERATE=1 cargo test --test management spec_snapshot_matches_committed_file` to create it"
        );
    });

    assert_eq!(
        live_pretty, committed,
        "live spec drifted from committed snapshot at {SNAPSHOT_PATH}; \
         regenerate with `OPENAPI_REGENERATE=1 cargo test --test management spec_snapshot_matches_committed_file`",
    );
}

#[tokio::test]
async fn spec_endpoint_requires_bearer_auth() {
    let h = harness(Some(KEY)).await;
    // No Authorization header at all → 401, not 200.
    let resp = h
        .router
        .oneshot(req(Method::GET, SPEC_PATH, None))
        .await
        .unwrap();
    let (status, _body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
