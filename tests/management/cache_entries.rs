//! Cache-entries: list / filter / paginate, get-one, delete-by-id.
//! Match endpoint and bulk delete live in their own files because of
//! their volume of edge-case coverage.

use axum::http::{Method, StatusCode};
use gha_cache_oxide::db::Db;
use tower::ServiceExt;

use super::common::{KEY, body_json, harness, req, seed_entry, upload_test_file};

/// Seeds three entries: two under `scope-A` (different repoIds) and
/// one under `scope-B`. Used by the three list/filter/paginate tests.
async fn seed_listing_fixture(db: &dyn Db) {
    seed_entry(db, "loc-list-1", "fldr-list-1", "entry-list-1", "scope-A").await;
    seed_entry(db, "loc-list-2", "fldr-list-2", "entry-list-2", "scope-A").await;
    seed_entry(db, "loc-list-3", "fldr-list-3", "entry-list-3", "scope-B").await;
}

#[tokio::test]
async fn list_cache_entries_no_filter_returns_total_and_items() {
    let h = harness(Some(KEY)).await;
    seed_listing_fixture(&*h.db).await;

    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries?itemsPerPage=10",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 3);
    assert_eq!(body["page"], 1);
    assert_eq!(body["itemsPerPage"], 10);
    assert_eq!(body["items"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn list_cache_entries_scope_filter_narrows_results() {
    let h = harness(Some(KEY)).await;
    seed_listing_fixture(&*h.db).await;

    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries?scope=scope-A",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 2);
    let scopes: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["scope"].as_str().unwrap())
        .collect();
    assert!(scopes.iter().all(|s| *s == "scope-A"));
}

#[tokio::test]
async fn list_cache_entries_paginates_via_page_and_items_per_page() {
    let h = harness(Some(KEY)).await;
    seed_listing_fixture(&*h.db).await;

    // Page 2 of size 2 returns the trailing third row.
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries?page=2&itemsPerPage=2",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 3);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
}

// ---------- get one cache-entry (#70) ----------------------------------

#[tokio::test]
async fn get_cache_entry_by_id_returns_the_seeded_row() {
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-getone",
        "fldr-getone",
        "entry-getone",
        "scope-getone",
    )
    .await;

    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/entry-getone",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], "entry-getone");
    assert_eq!(body["scope"], "scope-getone");
    // Wire shape uses camelCase (locationId, not location_id).
    assert_eq!(body["locationId"], "loc-getone");
    assert!(body.get("repoId").is_some());
}

#[tokio::test]
async fn get_unknown_cache_entry_returns_404() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries/does-not-exist",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], "Cache entry not found");
}

// ---------- delete cache-entry by id ------------------------------------

#[tokio::test]
async fn delete_cache_entry_cascades_to_db_and_folder() {
    let h = harness(Some(KEY)).await;
    seed_entry(&*h.db, "loc-del", "fldr-del", "entry-del", "scope-del").await;
    // Plant an actual file under fldr-del so we can observe deletion.
    upload_test_file(h.storage.as_ref(), "fldr-del/parts/0").await;
    assert_eq!(
        h.storage
            .count_files_in_folder("fldr-del/parts")
            .await
            .unwrap(),
        1,
    );

    let resp = h
        .router
        .clone()
        .oneshot(req(
            Method::DELETE,
            "/management/cache-entries/entry-del",
            Some(KEY),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // DB: cache_entry + storage_location both gone (FK cascade).
    assert!(
        h.db.find_location_for_entry("entry-del")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        h.db.count_storage_locations().await.unwrap(),
        0,
        "storage_locations row must be removed",
    );
    // Filesystem: folder reaped.
    assert_eq!(
        h.storage
            .count_files_in_folder("fldr-del/parts")
            .await
            .unwrap(),
        0,
        "the folder must be removed from storage",
    );
}

#[tokio::test]
async fn delete_unknown_cache_entry_returns_404() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(
            Method::DELETE,
            "/management/cache-entries/does-not-exist",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], "Cache entry not found");
}
