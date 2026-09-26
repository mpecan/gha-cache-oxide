//! Forgejo v1 dialect: storage and DB failure paths, and interaction
//! with the background cleanup tasks.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod forgejo_common;

use std::sync::atomic::Ordering;

use gha_cache_oxide::tasks::cleanup::EntryRetention;

use forgejo_common::{Runner, VERSION, random_bytes, spawn, spawn_flaky};
use reqwest::{Method, StatusCode};
use serde_json::Value;

const RETENTION: EntryRetention = EntryRetention {
    older_than_days: 90,
    unused_older_than_days: None,
};

async fn setup() -> (forgejo_common::Server, Runner) {
    let srv = spawn(Some(forgejo_common::SECRET)).await;
    let runner = Runner::new(&srv);
    (srv, runner)
}

// ---- failure paths -----------------------------------------------------

#[tokio::test]
async fn storage_failure_during_upload_is_500_and_retryable() {
    let (srv, flaky) = spawn_flaky().await;
    let r = Runner::new(&srv);

    let content = random_bytes(100);
    let id = r.reserve("flaky_upload", VERSION, 100).await;
    flaky.fail_upload.store(true, Ordering::SeqCst);
    assert_eq!(
        r.patch(id, "bytes 0-99/*", content.clone()).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(srv.metrics.forgejo.upload_errors.get(), 1);

    flaky.fail_upload.store(false, Ordering::SeqCst);
    assert_eq!(
        r.patch(id, "bytes 0-99/*", content.clone()).await,
        StatusCode::OK
    );
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::OK);
    let (_, bytes) = r.find_and_download("flaky_upload", VERSION).await;
    assert_eq!(bytes.as_ref(), content.as_slice());
}

#[tokio::test]
async fn storage_failure_during_commit_keeps_the_upload_for_a_retry() {
    let (srv, flaky) = spawn_flaky().await;
    let r = Runner::new(&srv);

    let content = random_bytes(200);
    let id = r.reserve("flaky_commit", VERSION, 200).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", content[..100].to_vec()).await,
        StatusCode::OK
    );
    assert_eq!(
        r.patch(id, "bytes 100-199/*", content[100..].to_vec())
            .await,
        StatusCode::OK
    );

    flaky.fail_copy.store(true, Ordering::SeqCst);
    assert_eq!(
        r.commit(id, Some(200)).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(srv.metrics.forgejo.commit_errors.get(), 1);
    assert!(srv.db.find_upload_by_id(id).await.unwrap().is_some());

    flaky.fail_copy.store(false, Ordering::SeqCst);
    assert_eq!(r.commit(id, Some(200)).await, StatusCode::OK);
    let (_, bytes) = r.find_and_download("flaky_commit", VERSION).await;
    assert_eq!(bytes.as_ref(), content.as_slice());
}

/// Port of act's `TestHandlerAPIFatalErrors`: DB failures are 500 with
/// act's error body (and, unlike act, do not kill the process).
#[tokio::test]
async fn db_failure_is_500_with_act_error_body() {
    let (srv, r) = setup().await;
    let id = r.reserve("db_fail", VERSION, 10).await;
    srv.db.as_sqlite_pool().unwrap().close().await;

    let routes = [
        (Method::GET, "/cache?keys=k&version=v".to_string()),
        (Method::POST, "/caches".to_string()),
        (Method::PATCH, format!("/caches/{id}")),
        (Method::POST, format!("/caches/{id}")),
        (Method::GET, "/artifacts/some-id".to_string()),
    ];
    for (method, path) in routes {
        let resp = r
            .request(method.clone(), &path)
            .header("Content-Range", "bytes 0-9/*")
            .body(r#"{"key":"k","version":"v"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{method} {path}"
        );
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body, serde_json::json!({ "error": "internal error" }));
    }
}

// ---- cleanup interaction ----------------------------------------------

#[tokio::test]
async fn cleanup_reaps_abandoned_chunked_upload() {
    let (srv, r) = setup().await;
    let id = r.reserve("abandoned", VERSION, 100).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", vec![0; 100]).await,
        StatusCode::OK
    );
    let folder = srv
        .db
        .find_upload_by_id(id)
        .await
        .unwrap()
        .unwrap()
        .folder_name;
    assert_eq!(srv.storage.list_folder(&folder).await.unwrap().len(), 1);

    let later = chrono::Utc::now().timestamp_millis() + 10 * 60_000;
    let report = gha_cache_oxide::tasks::cleanup::run_all(
        srv.db.as_ref(),
        srv.storage.as_ref(),
        later,
        RETENTION,
    )
    .await;
    assert_eq!(report.uploads_deleted, 1);
    assert!(srv.db.find_upload_by_id(id).await.unwrap().is_none());
    assert!(srv.storage.list_folder(&folder).await.unwrap().is_empty());
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::NOT_FOUND);
}

/// The upload is older than the staleness cutoff; only the touch from a
/// recent chunk keeps it alive. (Without the touch this is exactly
/// `cleanup_reaps_abandoned_chunked_upload`.)
#[tokio::test]
async fn cleanup_keeps_upload_that_is_receiving_chunks() {
    let (srv, r) = setup().await;
    let id = r.reserve("active", VERSION, 100).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", vec![0; 100]).await,
        StatusCode::OK
    );
    let now = chrono::Utc::now().timestamp_millis();
    // Cleanup at now+90 s → cutoff now+30 s: createdAt (now) is stale,
    // but a chunk touched at now+60 s is inside the window.
    assert!(srv.db.touch_upload(id, now + 60_000).await.unwrap());
    let report = gha_cache_oxide::tasks::cleanup::run_all(
        srv.db.as_ref(),
        srv.storage.as_ref(),
        now + 90_000,
        RETENTION,
    )
    .await;
    assert_eq!(report.uploads_deleted, 0);
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::OK);
}

/// Regression: every v1 commit is merged in the background, which sets
/// `mergeStartedAt`. The never-downloaded expiry must still reap such
/// entries (it once excluded any row with `mergeStartedAt` set, so no
/// Forgejo entry could ever expire).
#[tokio::test]
async fn unused_expiry_reaps_background_merged_v1_entries() {
    let (srv, r) = setup().await;
    let id = r.reserve("never_restored", VERSION, 100).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", vec![7; 100]).await,
        StatusCode::OK
    );
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while srv.metrics.merges.completed.get() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "background merge never finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let eight_days_later = chrono::Utc::now().timestamp_millis() + 8 * 86_400_000;
    let retention = EntryRetention {
        older_than_days: 30,
        unused_older_than_days: Some(7),
    };
    let report = gha_cache_oxide::tasks::cleanup::run_all(
        srv.db.as_ref(),
        srv.storage.as_ref(),
        eight_days_later,
        retention,
    )
    .await;
    assert_eq!(report.entries_deleted, 1);
    assert_eq!(
        r.find("never_restored", VERSION).await.0,
        StatusCode::NO_CONTENT
    );
}
