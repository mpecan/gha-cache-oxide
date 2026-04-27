//! Integration tests for the management REST API (issue #19).
//!
//! Drives the full `build_app` router through `tower::ServiceExt::oneshot`
//! — same shape as `tests/health.rs`. Each test owns its own
//! `TempDir`-backed filesystem adapter so `delete_folder` assertions
//! observe the real on-disk state.
//!
//! Coverage:
//! - 501 when `MANAGEMENT_API_KEY` is unset (every route).
//! - 401 on missing / wrong `Authorization` header.
//! - List/filter/paginate `cache-entries`.
//! - Delete `cache-entries/{id}` cascades to DB row + storage folder.
//! - Delete unknown id → 404.
//! - List `storage-locations` paginates.
//! - `POST /cleanup/trigger` returns the cleanup report and actually
//!   transitions the seeded rows.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, LogFormat, Secret, StorageConfig};
use gha_cache_oxide::db::entities::{CacheEntryCoord, NewUpload};
use gha_cache_oxide::db::id::new_upload_id;
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::{ByteStream, FilesystemAdapter, StorageAdapter};
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;

const KEY: &str = "test-management-key";

struct NullFetcher;
#[async_trait::async_trait]
impl JwksFetcher for NullFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        Ok(Vec::new())
    }
}

struct Harness {
    router: axum::Router,
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    _tmp: TempDir,
}

async fn harness(management_api_key: Option<&str>) -> Harness {
    let tmp = TempDir::new().unwrap();
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let db: Arc<dyn Db> = Arc::new(db);
    let storage: Arc<dyn StorageAdapter> = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    let jwks = Arc::new(JwksCache::new(Arc::new(NullFetcher)));
    let config = AppConfig {
        api_base_url: "http://localhost:3000".parse().unwrap(),
        port: 0,
        log_format: LogFormat::Text,
        cache_cleanup_older_than_days: 90,
        disable_cleanup_jobs: true,
        enable_direct_downloads: false,
        skip_token_validation: true,
        management_api_key: management_api_key.map(|k| Secret::new(k.to_string())),
        storage: StorageConfig::Filesystem {
            path: tmp.path().to_path_buf(),
        },
        database: DbConfig::Sqlite {
            path: PathBuf::from(":memory:"),
        },
    };
    let state = AppState::new(db.clone(), storage.clone(), jwks, config);
    let router = gha_cache_oxide::build_app(state);
    Harness {
        router,
        db,
        storage,
        _tmp: tmp,
    }
}

fn req(method: Method, uri: &str, key: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(k) = key {
        b = b.header(header::AUTHORIZATION, format!("Bearer {k}"));
    }
    b.body(Body::empty()).unwrap()
}

async fn body_json(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    if bytes.is_empty() {
        return (status, Value::Null);
    }
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (status, value)
}

async fn seed_entry(db: &dyn Db, loc_id: &str, folder: &str, entry_id: &str, scope: &str) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(loc_id, folder, 1).await.unwrap();
    tx.seed_cache_entry(
        entry_id,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope,
            repo_id: "r",
        },
        0,
        loc_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

// ---------- 501 / 401 gate tests ----------------------------------------

#[tokio::test]
async fn every_route_returns_501_when_management_key_unset() {
    let h = harness(None).await;

    for (method, uri) in [
        (Method::GET, "/management/cache-entries"),
        (Method::DELETE, "/management/cache-entries/anything"),
        (Method::GET, "/management/storage-locations"),
        (Method::POST, "/management/cleanup/trigger"),
    ] {
        let resp = h
            .router
            .clone()
            .oneshot(req(method.clone(), uri, Some(KEY)))
            .await
            .unwrap();
        let (status, body) = body_json(resp).await;
        assert_eq!(
            status,
            StatusCode::NOT_IMPLEMENTED,
            "{method} {uri} must 501 when MANAGEMENT_API_KEY is unset",
        );
        assert_eq!(body["statusCode"], 501);
        assert_eq!(
            body["message"],
            "Management API not enabled - set MANAGEMENT_API_KEY"
        );
    }
}

#[tokio::test]
async fn missing_authorization_returns_401() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(Method::GET, "/management/cache-entries", None))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["statusCode"], 401);
}

#[tokio::test]
async fn wrong_key_returns_401() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(
            Method::GET,
            "/management/cache-entries",
            Some("not-the-key"),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["message"], "Invalid management API key");
}

// ---------- list cache-entries -----------------------------------------

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

// ---------- delete cache-entries ---------------------------------------

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

// ---------- list storage-locations -------------------------------------

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

// ---------- cleanup trigger --------------------------------------------

#[tokio::test]
async fn cleanup_trigger_runs_pass_and_returns_report() {
    let h = harness(Some(KEY)).await;
    // Seed a stale upload — the cleanup pass should reap it.
    let upload_id = new_upload_id();
    h.db.create_upload(NewUpload {
        id: upload_id,
        coord: CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-cleanup",
            repo_id: "r",
        },
        folder_name: "fldr-cleanup-stale",
        // createdAt = 0 → unconditionally older than the 90-day cutoff.
        created_at_ms: 0,
    })
    .await
    .unwrap();

    let resp = h
        .router
        .oneshot(req(Method::POST, "/management/cleanup/trigger", Some(KEY)))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    // Body shape — the keys the JS client / shell scripts will read.
    for k in [
        "merges_reset",
        "uploads_deleted",
        "parts_deleted",
        "entries_deleted",
        "locations_deleted",
    ] {
        assert!(body.get(k).is_some(), "expected {k} in response: {body}");
    }
    assert_eq!(body["uploads_deleted"], 1, "stale upload must be reaped");
    assert!(
        h.db.find_upload_by_id(upload_id).await.unwrap().is_none(),
        "cleanup must have deleted the stale upload row",
    );
}

// ---------- helpers -----------------------------------------------------

async fn upload_test_file(storage: &dyn StorageAdapter, name: &str) {
    use bytes::Bytes;
    use futures::StreamExt;
    let chunk: Result<Bytes, std::io::Error> = Ok(Bytes::from_static(b"hello"));
    let stream: ByteStream = futures::stream::iter(vec![chunk]).boxed();
    storage.upload_stream(name, stream).await.unwrap();
}
