//! `GET /management/cache-entries/match` — debug helper that runs
//! `match_cache_entry` from outside the Twirp flow.
//!
//! Coverage:
//! - Exact-primary match returns the seeded entry + `"exact-primary"` type.
//! - Repeated-key (`?scopes=a&scopes=b`) and bracket
//!   (`?scopes[]=a`) array notations both deserialise.
//! - Missing `primaryKey` / empty `scopes` produce 400 with a
//!   descriptive body.
//! - Routing precedence: `/match` literal beats `/{id}` parameter
//!   even when an entry's id is literally `"match"`.

use axum::http::{Method, StatusCode};
use gha_cache_oxide::db::entities::CacheEntryCoord;
use tower::ServiceExt;

use super::common::{KEY, body_json, harness, req, seed_entry};

#[tokio::test]
async fn match_endpoint_returns_match_and_type_for_exact_primary() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-match-exact",
        "fldr-match-exact",
        "entry-match-exact",
        "scope-match",
    )
    .await;

    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/match?primaryKey=k&version=v&repoId=r&scopes=scope-match",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["match"]["id"], "entry-match-exact");
    assert_eq!(body["type"], "exact-primary");
}

#[tokio::test]
async fn match_endpoint_accepts_repeated_keys_for_multi_value_params() {
    let h = harness(Some(KEY)).await;
    // Seed under scope-B; primary key is "fallback" so a restoreKey
    // match wins after the primary "missing" miss.
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location("loc-match-rk", "fldr-match-rk", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "entry-match-rk",
        CacheEntryCoord {
            key: "fallback",
            version: "v",
            scope: "scope-B",
            repo_id: "r",
        },
        0,
        "loc-match-rk",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Repeated-key form: ?scopes=...&scopes=...&restoreKeys=...
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/match?primaryKey=missing&version=v&repoId=r&scopes=scope-A&scopes=scope-B&restoreKeys=fallback",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["match"]["id"], "entry-match-rk");
    assert_eq!(body["type"], "exact-restore");
}

#[tokio::test]
async fn match_endpoint_accepts_brackets_for_multi_value_params() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-match-br",
        "fldr-match-br",
        "entry-match-br",
        "scope-match-br",
    )
    .await;

    // Bracket form: ?scopes[]=...
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/match?primaryKey=k&version=v&repoId=r&scopes[]=scope-match-br",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["match"]["id"], "entry-match-br");
}

#[tokio::test]
async fn match_endpoint_returns_404_when_no_match() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-match-miss",
        "fldr-match-miss",
        "entry-match-miss",
        "scope-match-miss",
    )
    .await;

    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/match?primaryKey=miss&version=v&repoId=r&scopes=scope-match-miss",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], "No matching cache entry");
}

#[tokio::test]
async fn match_endpoint_400_on_missing_required_params() {
    let h = harness(Some(KEY)).await;
    // No query string at all → reports the first missing required param.
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/match",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"].as_str().unwrap().contains("primaryKey"),
        "expected error to name the missing param, got {body}"
    );
}

#[tokio::test]
async fn match_endpoint_400_when_scopes_empty() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/match?primaryKey=k&version=v&repoId=r",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"].as_str().unwrap().contains("scopes"),
        "expected error to mention `scopes`, got {body}"
    );
}

/// Pins the routing-precedence invariant called out in
/// `src/routes/management/mod.rs`: the literal `/match` segment wins
/// over the `/{id}` parameter route. Even when an entry's id literally
/// equals `"match"`, `GET /cache-entries/match` reaches the match
/// endpoint, never `get_one`. Without the literal route registered,
/// this test would 200-with-the-entry; with it, it 400s on missing
/// `primaryKey` (proving the match handler ran).
#[tokio::test]
async fn match_endpoint_wins_over_get_one_routing() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-routing",
        "fldr-routing",
        "match",
        "scope-routing",
    )
    .await;

    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/match",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "GET /cache-entries/match must hit the match handler (which 400s without primaryKey), \
         not the /{{id}} handler that would 200 with the entry id=\"match\""
    );
    assert!(body["message"].as_str().unwrap().contains("primaryKey"));
}
