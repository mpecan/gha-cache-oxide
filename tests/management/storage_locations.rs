//! `GET/DELETE /management/storage-locations[/{id}]` — list, get-one,
//! and explicit orphan removal.

use axum::http::{Method, StatusCode};
use tower::ServiceExt;

use super::common::{KEY, body_json, harness, req, upload_test_file};

#[tokio::test]
async fn list_storage_locations_returns_paginated_view() {
    let h = harness(Some(KEY)).await;
    for i in 0..3 {
        let mut tx = h.db.begin().await.unwrap();
        tx.insert_storage_location(&format!("loc-locs-{i}"), &format!("fldr-locs-{i}"), 1)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let resp = h
        .router
        .clone()
        .oneshot(req(
            Method::GET,
            "/management/storage-locations?itemsPerPage=2",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 3);
    assert_eq!(body["itemsPerPage"], 2);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    // Verify the wire shape uses camelCase (folderName not folder_name).
    let first = &body["items"][0];
    assert!(first.get("folderName").is_some());
    assert!(first.get("partCount").is_some());
}

// ---------- get one storage-location (#70) -----------------------------

#[tokio::test]
async fn get_storage_location_by_id_returns_seeded_row() {
    let h = harness(Some(KEY)).await;
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location("loc-sl-get", "fldr-sl-get", 5)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/storage-locations/loc-sl-get",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], "loc-sl-get");
    assert_eq!(body["folderName"], "fldr-sl-get");
    assert_eq!(body["partCount"], 5);
}

#[tokio::test]
async fn get_unknown_storage_location_returns_404() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/storage-locations/does-not-exist",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], "Storage location not found");
}

// ---------- delete one storage-location (#70) --------------------------

#[tokio::test]
async fn delete_storage_location_removes_row_and_folder() {
    let h = harness(Some(KEY)).await;
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location("loc-sl-del", "fldr-sl-del", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    upload_test_file(h.storage.as_ref(), "fldr-sl-del/parts/0").await;
    assert_eq!(
        h.storage
            .count_files_in_folder("fldr-sl-del/parts")
            .await
            .unwrap(),
        1,
    );

    let resp = h
        .router
        .clone()
        .oneshot(req(
            Method::DELETE,
            "/management/storage-locations/loc-sl-del",
            Some(KEY),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // DB row gone.
    assert!(
        h.db.find_storage_location_by_id("loc-sl-del")
            .await
            .unwrap()
            .is_none(),
    );
    // Folder reaped.
    assert_eq!(
        h.storage
            .count_files_in_folder("fldr-sl-del/parts")
            .await
            .unwrap(),
        0,
    );
}

#[tokio::test]
async fn delete_unknown_storage_location_returns_404() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(
            Method::DELETE,
            "/management/storage-locations/does-not-exist",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], "Storage location not found");
}
