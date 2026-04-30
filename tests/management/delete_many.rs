//! `DELETE /management/cache-entries` — bulk delete by query filter.
//!
//! Three contracts pinned here:
//! - Empty filter is rejected (400) — divergent from upstream which
//!   silently deletes every row.
//! - Per-filter narrowing returns the expected count and removes only
//!   the matching rows.
//! - `repoId` is honoured — divergent from upstream `deleteMany` at
//!   `lib/api/cache-entries.ts:163-168` which drops the filter on the
//!   floor.

use axum::http::{Method, StatusCode};
use gha_cache_oxide::db::entities::CacheEntryCoord;
use tower::ServiceExt;

use super::common::{KEY, body_json, harness, req, seed_entry};

#[tokio::test]
async fn delete_many_rejects_empty_filter() {
    let h = harness(Some(KEY)).await;
    let resp = h
        .router
        .oneshot(req(Method::DELETE, "/management/cache-entries", Some(KEY)))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("at least one filter"),
        "got {body}"
    );
}

#[tokio::test]
async fn delete_many_by_scope_returns_count_and_removes_only_matches() {
    let h = harness(Some(KEY)).await;
    // Two scope-A, one scope-B.
    seed_entry(&*h.db, "loc-dm-1", "fldr-dm-1", "entry-dm-1", "scope-A").await;
    seed_entry(&*h.db, "loc-dm-2", "fldr-dm-2", "entry-dm-2", "scope-A").await;
    seed_entry(&*h.db, "loc-dm-3", "fldr-dm-3", "entry-dm-3", "scope-B").await;

    let resp = h
        .router
        .clone()
        .oneshot(req(
            Method::DELETE,
            "/management/cache-entries?scope=scope-A",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], 2);

    // Scope-B row survives.
    assert!(
        h.db.find_cache_entry_by_id("entry-dm-3")
            .await
            .unwrap()
            .is_some(),
        "scope-B row must survive a scope=scope-A filter",
    );
    // Scope-A rows gone.
    assert!(
        h.db.find_cache_entry_by_id("entry-dm-1")
            .await
            .unwrap()
            .is_none()
    );
}

/// Honours `repoId` — divergent from upstream. Documents the
/// divergence as a behavioural pin: a future "fix to match upstream"
/// would make this test fail loud.
#[tokio::test]
async fn delete_many_honours_repo_id_filter() {
    let h = harness(Some(KEY)).await;
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location("loc-repo-1", "fldr-repo-1", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "entry-repo-1",
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-X",
            repo_id: "100",
        },
        0,
        "loc-repo-1",
    )
    .await
    .unwrap();
    tx.insert_storage_location("loc-repo-2", "fldr-repo-2", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "entry-repo-2",
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-X",
            repo_id: "200",
        },
        0,
        "loc-repo-2",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let resp = h
        .router
        .clone()
        .oneshot(req(
            Method::DELETE,
            "/management/cache-entries?repoId=100",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], 1, "only repoId=100 should be deleted");
    assert!(
        h.db.find_cache_entry_by_id("entry-repo-1")
            .await
            .unwrap()
            .is_none(),
    );
    assert!(
        h.db.find_cache_entry_by_id("entry-repo-2")
            .await
            .unwrap()
            .is_some(),
    );
}
