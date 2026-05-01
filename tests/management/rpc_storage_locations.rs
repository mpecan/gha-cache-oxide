//! oRPC `_rpc` integration tests for `storageLocations.*` procedures.
//!
//! Upstream exposes only `get` and `delete` — no `list` (compare
//! `lib/api/storage-locations.ts`). We mirror that exactly; a test
//! that hits `/storageLocations/list` would fail through to the
//! 404-fallback covered by `rpc_auth.rs`.

use axum::http::StatusCode;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::common::{KEY, harness, seed_entry};
use super::rpc_common::{body_json, rpc_post};

#[tokio::test]
async fn get_happy_path() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-rpc-sg",
        "fldr-rpc-sg",
        "entry-rpc-sg",
        "scn-rpc-sg",
    )
    .await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "storageLocations/get",
            Some(KEY),
            json!({ "id": "loc-rpc-sg" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["json"]["id"], json!("loc-rpc-sg"));
    assert_eq!(body["json"]["folderName"], json!("fldr-rpc-sg"));
}

#[tokio::test]
async fn get_unknown_returns_not_found() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(rpc_post(
            "storageLocations/get",
            Some(KEY),
            json!({ "id": "does-not-exist" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["json"]["code"], json!("NOT_FOUND"));
}

#[tokio::test]
async fn delete_happy_path() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-rpc-sd",
        "fldr-rpc-sd",
        "entry-rpc-sd",
        "scn-rpc-sd",
    )
    .await;

    let resp = h
        .router
        .oneshot(rpc_post(
            "storageLocations/delete",
            Some(KEY),
            json!({ "id": "loc-rpc-sd" }),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["json"], Value::Null);

    let still_there =
        h.db.find_storage_location_by_id("loc-rpc-sd")
            .await
            .unwrap();
    assert!(still_there.is_none(), "row should be deleted");
}
