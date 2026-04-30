//! `DELETE /management/cache-entries` — bulk delete by query filter.
//!
//! Four contracts pinned here:
//! - Empty filter is rejected (400) — divergent from upstream which
//!   silently deletes every row.
//! - Per-filter narrowing returns the expected count and removes only
//!   the matching rows.
//! - `repoId` is honoured — divergent from upstream `deleteMany` at
//!   `lib/api/cache-entries.ts:163-168` which drops the filter on the
//!   floor.
//! - The post-delete on-demand `cleanup:storage-locations` sweep (#71)
//!   reaps the orphan `storage_locations` rows / folders bulk delete
//!   leaves behind, mirroring upstream's `event.waitUntil(runTask(...))`.

use std::time::Duration;

use axum::http::{Method, StatusCode};
use gha_cache_oxide::db::entities::CacheEntryCoord;
use tower::ServiceExt;

use super::common::{
    KEY, body_json, harness, harness_with_cleanup_enabled, poll_until, req, seed_entry,
    upload_test_file,
};

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

/// Bulk delete (DELETE `cache_entries` rows directly) leaves the
/// `storage_locations` rows and their folders dangling — there's no FK
/// in that direction. The on-demand sweep spawned at the end of the
/// `delete_many` handler (#71) must reap them within a short window so
/// operators don't have to wait for the next hourly cleanup tick.
///
/// Polls for up to 2 s — the sweep on an in-memory `SQLite` + temp-dir
/// filesystem typically completes in <50 ms; 2 s is generous headroom
/// for slow CI runners.
#[tokio::test]
async fn delete_many_triggers_orphan_sweep() {
    let h = harness_with_cleanup_enabled(Some(KEY)).await;
    seed_orphan_fixture(&h).await;

    let resp = h
        .router
        .clone()
        .oneshot(req(
            Method::DELETE,
            "/management/cache-entries?scope=scope-orphan",
            Some(KEY),
        ))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], 2);

    assert_orphan_reaped(&h, "loc-orphan-1", "fldr-orphan-1").await;
    assert_orphan_reaped(&h, "loc-orphan-2", "fldr-orphan-2").await;
}

/// `DISABLE_CLEANUP_JOBS=true` short-circuits the on-demand sweep —
/// matches upstream's `runTask('cleanup:storage-locations')` flow,
/// where the task body itself respects the disable flag (`tasks/cleanup/storage-locations.ts:13`).
/// Pin: bulk-delete with cleanup disabled leaves the orphan
/// `storage_locations` row in place. The hourly scheduler is also
/// disabled in that mode, so this is the operator's explicit "off"
/// switch — not a bug.
#[tokio::test]
async fn delete_many_skips_sweep_when_cleanup_disabled() {
    // `harness` (the default) uses `disable_cleanup_jobs: true`.
    let h = harness(Some(KEY)).await;
    seed_entry(
        &*h.db,
        "loc-disabled",
        "fldr-disabled",
        "entry-disabled",
        "scope-disabled",
    )
    .await;

    let resp = h
        .router
        .clone()
        .oneshot(req(
            Method::DELETE,
            "/management/cache-entries?scope=scope-disabled",
            Some(KEY),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Long enough for any spawn to have run if the gate were missing.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        h.db.find_storage_location_by_id("loc-disabled")
            .await
            .unwrap()
            .is_some(),
        "with DISABLE_CLEANUP_JOBS=true the orphan must NOT be swept",
    );
}

/// Two `cache_entries` under `scope-orphan`, each with its own
/// `storage_location` and a real file on disk so the test can
/// observe the folder reaping end-to-end.
async fn seed_orphan_fixture(h: &super::common::Harness) {
    seed_entry(
        &*h.db,
        "loc-orphan-1",
        "fldr-orphan-1",
        "entry-orphan-1",
        "scope-orphan",
    )
    .await;
    seed_entry(
        &*h.db,
        "loc-orphan-2",
        "fldr-orphan-2",
        "entry-orphan-2",
        "scope-orphan",
    )
    .await;
    upload_test_file(h.storage.as_ref(), "fldr-orphan-1/parts/0").await;
    upload_test_file(h.storage.as_ref(), "fldr-orphan-2/parts/0").await;
}

/// Polls for the `storage_location` row to disappear, then asserts
/// the underlying parts folder is also reaped.
async fn assert_orphan_reaped(h: &super::common::Harness, loc_id: &str, folder: &str) {
    let label = format!("{loc_id} storage_location reaped");
    poll_until(Duration::from_secs(2), &label, || async {
        h.db.find_storage_location_by_id(loc_id)
            .await
            .unwrap()
            .is_none()
    })
    .await;
    assert_eq!(
        h.storage
            .count_files_in_folder(&format!("{folder}/parts"))
            .await
            .unwrap(),
        0,
    );
}
