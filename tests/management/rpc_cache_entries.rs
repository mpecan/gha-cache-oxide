//! oRPC `_rpc` integration tests for `cacheEntries.*` procedures.

use axum::http::StatusCode;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::common::{KEY, harness, seed_entry, upload_test_file};
use super::rpc_common::{body_json, rpc_post};

// ---------- cacheEntries.findMany ---------------------------------------

#[tokio::test]
async fn find_many_happy_path_matches_upstream_output_shape() {
    let h = harness(Some(KEY)).await;
    seed_entry(&*h.db, "loc-rpc-1", "fldr-rpc-1", "entry-rpc-1", "scn-rpc").await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/findMany",
            Some(KEY),
            json!({ "scope": "scn-rpc" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Output schema upstream is `{total, items}` only — no
    // page/itemsPerPage. Pin it.
    let inner = &body["json"];
    assert_eq!(inner["total"], json!(1));
    assert!(
        inner.get("page").is_none(),
        "findMany output must not include page (matches upstream Zod schema)"
    );
    assert!(
        inner.get("itemsPerPage").is_none(),
        "findMany output must not include itemsPerPage (matches upstream Zod schema)"
    );
    let items = inner["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], json!("entry-rpc-1"));
    assert_eq!(items[0]["scope"], json!("scn-rpc"));
}

// ---------- cacheEntries.get --------------------------------------------

#[tokio::test]
async fn get_happy_path() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-rpc-2",
        "fldr-rpc-2",
        "entry-rpc-2",
        "scn-rpc-2",
    )
    .await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/get",
            Some(KEY),
            json!({ "id": "entry-rpc-2" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["json"]["id"], json!("entry-rpc-2"));
}

#[tokio::test]
async fn get_unknown_id_returns_orpc_not_found() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/get",
            Some(KEY),
            json!({ "id": "does-not-exist" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["json"]["defined"], json!(false));
    assert_eq!(body["json"]["code"], json!("NOT_FOUND"));
    assert_eq!(body["json"]["status"], json!(404));
    assert_eq!(body["json"]["message"], json!("Cache entry not found"));
}

// ---------- cacheEntries.match ------------------------------------------

#[tokio::test]
async fn match_happy_path() {
    let h = harness(Some(KEY)).await;
    // seed_entry uses key="k", version="v", repoId="r" — match on those.
    seed_entry(
        &*h.db,
        "loc-rpc-m",
        "fldr-rpc-m",
        "entry-rpc-m",
        "scn-rpc-m",
    )
    .await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/match",
            Some(KEY),
            json!({
                "primaryKey": "k",
                "version": "v",
                "repoId": "r",
                "scopes": ["scn-rpc-m"],
            }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    let inner = &body["json"];
    assert_eq!(inner["match"]["id"], json!("entry-rpc-m"));
    assert_eq!(inner["type"], json!("exact-primary"));
}

#[tokio::test]
async fn match_no_match_returns_null() {
    // Upstream's `cacheEntries.match` returns `200 OK` with body
    // `null` when no entry matches (`lib/api/cache-entries.ts:65,75`),
    // not 404. Mirror that for SDK type-safety.
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/match",
            Some(KEY),
            json!({
                "primaryKey": "nope",
                "version": "v",
                "repoId": "r",
                "scopes": ["scn-empty"],
            }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["json"], Value::Null);
}

#[tokio::test]
async fn match_accepts_scalar_scope() {
    // Upstream's Zod preprocesses scalar `scopes`/`restoreKeys` into
    // single-element arrays (`lib/api/cache-entries.ts:46-50`). Pin
    // the parser tolerance.
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-rpc-mscalar",
        "fldr-rpc-mscalar",
        "entry-rpc-mscalar",
        "scn-rpc-mscalar",
    )
    .await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/match",
            Some(KEY),
            json!({
                "primaryKey": "k",
                "version": "v",
                "repoId": "r",
                "scopes": "scn-rpc-mscalar",
                "restoreKeys": "fallback",
            }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "scalar scopes/restoreKeys must parse: {body:?}"
    );
    assert_eq!(body["json"]["match"]["id"], json!("entry-rpc-mscalar"));
}

#[tokio::test]
async fn match_empty_scopes_returns_bad_request() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/match",
            Some(KEY),
            json!({
                "primaryKey": "k",
                "version": "v",
                "repoId": "r",
                "scopes": [],
            }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["json"]["code"], json!("BAD_REQUEST"));
}

// ---------- cacheEntries.delete -----------------------------------------

#[tokio::test]
async fn delete_happy_path() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-rpc-d",
        "fldr-rpc-d",
        "entry-rpc-d",
        "scn-rpc-d",
    )
    .await;
    upload_test_file(&*h.storage, "fldr-rpc-d/blob").await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/delete",
            Some(KEY),
            json!({ "id": "entry-rpc-d" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    // Upstream returns no body; we mirror with `{"json": null}`.
    assert_eq!(body["json"], Value::Null);

    let still_there = h.db.find_cache_entry_by_id("entry-rpc-d").await.unwrap();
    assert!(still_there.is_none(), "row should be deleted");
}

#[tokio::test]
async fn delete_unknown_id_returns_not_found() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/delete",
            Some(KEY),
            json!({ "id": "does-not-exist" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["json"]["code"], json!("NOT_FOUND"));
}

// ---------- cacheEntries.deleteMany -------------------------------------

#[tokio::test]
async fn delete_many_happy_path() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-rpc-dm-1",
        "fldr-rpc-dm-1",
        "entry-rpc-dm-1",
        "scn-rpc-dm",
    )
    .await;
    seed_entry(
        &*h.db,
        "loc-rpc-dm-2",
        "fldr-rpc-dm-2",
        "entry-rpc-dm-2",
        "scn-rpc-dm",
    )
    .await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "cacheEntries/deleteMany",
            Some(KEY),
            json!({ "scope": "scn-rpc-dm" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["json"]["deleted"], json!(2));
}

#[tokio::test]
async fn delete_many_empty_filter_returns_bad_request() {
    // Deliberate divergence from upstream (which silently accepts an
    // empty filter and wipes everything). README divergence note #7.
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post("cacheEntries/deleteMany", Some(KEY), json!({})))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["json"]["code"], json!("BAD_REQUEST"));
}
