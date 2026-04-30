//! Regression test for issue #51 — the loser-wait path closes the
//! parts-deletion race that previously affected `stream_parts_response`.
//!
//! Kept separate from `tests/blob.rs` because that file is at the
//! 700-line hard limit. Same precedent as `tests/blob_recovery.rs`.
//!
//! The test seeds an artificial in-flight-merge state via the `Db`
//! trait + filesystem primitives, wraps the storage adapter to slow
//! every `download_stream` call by 200ms, then races a parts deletion
//! against the loser's read. With the fix in `routes/blob.rs`, the
//! loser polls `mergedAt`, sees it land, and serves the merged blob.
//! Without the fix, the loser's lazy `parts/<i>` fetch fails after
//! the deletion lands.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::too_many_lines
)]

mod twirp_common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use bytes::Bytes;
use gha_cache_oxide::db::entities::CacheEntryCoord;
use gha_cache_oxide::db::id::now_ms;
use gha_cache_oxide::storage::{ByteStream, FilesystemAdapter, StorageAdapter, StorageError};
use serde_json::json;
use tower::ServiceExt;

use twirp_common::{
    BASE_PATH, Harness, HarnessOpts, body_json, harness, harness_with, post, write_token,
};

/// Wrapper adapter: injects a fixed sleep before every
/// `download_stream` call. Used to widen the race window so the
/// parts-deletion event can land mid-stream deterministically.
struct SlowReadsAdapter {
    inner: Arc<dyn StorageAdapter>,
    delay: Duration,
}

#[async_trait]
impl StorageAdapter for SlowReadsAdapter {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        self.inner.upload_stream(object_name, body).await
    }

    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        tokio::time::sleep(self.delay).await;
        self.inner.download_stream(object_name).await
    }

    async fn delete_folder(&self, folder_name: &str) -> Result<(), StorageError> {
        self.inner.delete_folder(folder_name).await
    }

    async fn count_files_in_folder(&self, folder_name: &str) -> Result<u64, StorageError> {
        self.inner.count_files_in_folder(folder_name).await
    }

    async fn signed_url(&self, object_name: &str) -> Result<Option<url::Url>, StorageError> {
        self.inner.signed_url(object_name).await
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.inner.clear().await
    }
}

fn get_download(entry_id: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/download/{entry_id}"))
        .body(Body::empty())
        .unwrap()
}

async fn collect_body(resp: axum::response::Response) -> Vec<u8> {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    bytes.to_vec()
}

async fn write_object(adapter: &dyn StorageAdapter, name: &str, bytes: &[u8]) {
    use bytes::Bytes;
    use futures::StreamExt;
    let chunks: Vec<Result<Bytes, std::io::Error>> = vec![Ok(Bytes::copy_from_slice(bytes))];
    let stream = futures::stream::iter(chunks).boxed();
    adapter.upload_stream(name, stream).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loser_serves_merged_blob_when_parts_deletion_races_in_flight() {
    // Build the harness with a SlowReadsAdapter wrapping the
    // filesystem. The 200ms per-read delay is wide enough that a
    // side-task scheduled at 50ms can finish (delete parts + set
    // mergedAt) before the simulated loser's *second* part fetch
    // would have completed.
    //
    // The TempDir is intentionally leaked: the wrapper adapter
    // outlives this scope (it's cloned into the AppState via
    // harness_with), and dropping the TempDir mid-test would tear
    // down the storage backing. The harness's own internal tmp dir
    // is unrelated and unused for storage in this test.
    let tmp = tempfile::TempDir::new().unwrap();
    let storage_path = tmp.path().to_path_buf();
    std::mem::forget(tmp);
    let inner: Arc<dyn StorageAdapter> = Arc::new(FilesystemAdapter::new(&storage_path).unwrap());
    let slow: Arc<dyn StorageAdapter> = Arc::new(SlowReadsAdapter {
        inner: inner.clone(),
        delay: Duration::from_millis(200),
    });
    let h: Harness = harness_with(HarnessOpts {
        enable_direct_downloads: false,
        storage: Some(slow.clone()),
    })
    .await;

    // Seed a location + cache_entries row pointing at it. Folder name
    // is what the handler will fetch.
    let location_id = "loc-race-51";
    let folder = "folder-race-51";
    let entry_id = "entry-race-51";
    let part_count: i64 = 3;
    let mut tx = h.db.begin().await.unwrap();
    tx.insert_storage_location(location_id, folder, part_count)
        .await
        .unwrap();
    tx.seed_cache_entry(
        entry_id,
        CacheEntryCoord {
            key: "race-51",
            version: "v1",
            scope: "race",
            repo_id: "r",
        },
        0,
        location_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Write the parts on disk through the inner adapter (no slow
    // wrapping for the seeding). Three parts so the loser would do
    // multiple lazy fetches under the buggy code.
    write_object(&*inner, &format!("{folder}/parts/0"), b"alpha-").await;
    write_object(&*inner, &format!("{folder}/parts/1"), b"bravo-").await;
    write_object(&*inner, &format!("{folder}/parts/2"), b"charlie").await;

    // Pre-stage the merged blob (simulating a winner whose upload has
    // completed and just needs the DB flip). The loser-wait path
    // serves this once `mergedAt` lands.
    let merged = b"alpha-bravo-charlie";
    write_object(&*inner, &format!("{folder}/merged"), merged).await;

    // Mark the location as "merge in flight" — `mergeStartedAt` set,
    // `mergedAt` not yet.
    assert!(
        h.db.try_mark_merge_started(location_id, now_ms())
            .await
            .unwrap(),
        "freshly-seeded location should win the CAS"
    );

    // Side-task: after 50ms, simulate the winner finishing its
    // finalize_merge — delete parts on disk and set `mergedAt`. This
    // is the event the loser's parts/N fetch would race with.
    let inner_for_task = inner.clone();
    let folder_for_task = folder.to_string();
    let db_for_task = h.db.clone();
    let location_id_for_task = location_id.to_string();
    let kicker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        inner_for_task
            .delete_folder(&format!("{folder_for_task}/parts"))
            .await
            .unwrap();
        db_for_task
            .mark_merged(&location_id_for_task, 999)
            .await
            .unwrap();
    });

    // Trigger the download. Buggy code lazy-fetches parts; the slow
    // wrapper means the SECOND fetch happens after parts are deleted,
    // and `to_bytes` panics on the resulting object-not-found.
    // Fixed code waits for `mergedAt`, then serves the merged blob.
    let resp = h
        .router
        .clone()
        .oneshot(get_download(entry_id))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "loser must get 200");
    let body = collect_body(resp).await;
    assert_eq!(body, merged, "loser must serve the merged-blob bytes");

    kicker.await.unwrap();
}

// ---------------------------------------------------------------------------
// Twirp-driven race tests (moved from `tests/blob.rs` for issue #51).
//
// Both tests exercise the loser-wait path via the public Twirp + blob
// routes, with no slow-adapter injection. Helpers (blockid_48,
// put_upload, etc.) are duplicated from blob.rs intentionally — the
// surface is bounded and a shared module would couple two otherwise-
// independent test binaries.
// ---------------------------------------------------------------------------

fn blockid_48(index: u64) -> String {
    let uuid = "11111111-2222-3333-4444-555555555555";
    let buf = format!("{uuid}{index:012}");
    assert_eq!(buf.len(), 48);
    base64::engine::general_purpose::STANDARD.encode(buf.as_bytes())
}

fn put_upload(id: i64, query: &str, body: Body) -> Request<Body> {
    let uri = if query.is_empty() {
        format!("/devstoreaccount1/upload/{id}")
    } else {
        format!("/devstoreaccount1/upload/{id}?{query}")
    };
    Request::builder()
        .method("PUT")
        .uri(uri)
        .body(body)
        .unwrap()
}

async fn upload_id_from_create_entry(h: &Harness, token: &str) -> i64 {
    let req = post(
        &format!("{BASE_PATH}/CreateCacheEntry"),
        Some(token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let (status, body) = body_json(h.router.clone().oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let url = body["signed_upload_url"].as_str().unwrap().to_string();
    url.strip_prefix("http://localhost:3000/devstoreaccount1/upload/")
        .unwrap()
        .parse()
        .unwrap()
}

async fn finalize_and_get_cache_entry_id(h: &Harness, token: &str) -> String {
    let req = post(
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        Some(token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let (status, _) = body_json(h.router.clone().oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let (status, body) = body_json(h.router.clone().oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let url = body["signed_download_url"].as_str().unwrap().to_string();
    url.strip_prefix("http://localhost:3000/download/")
        .unwrap()
        .to_string()
}

/// Polls `partsDeletedAt IS NOT NULL` so callers asserting "parts are
/// gone from disk" don't race the FS delete; see `tests/blob.rs`'s
/// twin helper for the rationale.
async fn wait_for_merge_complete(h: &Harness, entry_id: &str) {
    let pool = h.db.as_sqlite_pool().expect("SQLite test harness");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let parts_deleted_at: Option<i64> = sqlx::query_scalar(
            "SELECT partsDeletedAt FROM storage_locations \
             WHERE id = (SELECT locationId FROM cache_entries WHERE id = ?)",
        )
        .bind(entry_id)
        .fetch_one(pool)
        .await
        .unwrap();
        if parts_deleted_at.is_some() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "lazy merge did not finalise within 2s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn two_concurrent_first_downloads_both_serve_correct_bytes_with_one_merge() {
    // Two concurrent first-downloads — both serve correct bytes, exactly
    // one merge runs. Loser hits the wait path (issue #51); previously
    // raced parts deletion mid-stream.
    let h = harness().await;
    let token = write_token();

    let upload_id = upload_id_from_create_entry(&h, &token).await;
    let payload = b"concurrent-download-payload";
    let req = put_upload(
        upload_id,
        &format!("comp=block&blockid={}", blockid_48(0)),
        Body::from(Bytes::copy_from_slice(payload)),
    );
    assert_eq!(
        h.router.clone().oneshot(req).await.unwrap().status(),
        StatusCode::CREATED,
    );
    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    let a = {
        let router = h.router.clone();
        let eid = entry_id.clone();
        tokio::spawn(async move { router.oneshot(get_download(&eid)).await.unwrap() })
    };
    let b = {
        let router = h.router.clone();
        let eid = entry_id.clone();
        tokio::spawn(async move { router.oneshot(get_download(&eid)).await.unwrap() })
    };
    let (ra, rb) = tokio::join!(a, b);
    let resp_a = ra.unwrap();
    let resp_b = rb.unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);
    assert_eq!(resp_b.status(), StatusCode::OK);
    let bytes_a = collect_body(resp_a).await;
    let bytes_b = collect_body(resp_b).await;
    assert_eq!(bytes_a, payload);
    assert_eq!(bytes_b, payload);

    wait_for_merge_complete(&h, &entry_id).await;
    let merged_path = h.tmp.path().join(upload_id.to_string()).join("merged");
    assert_eq!(tokio::fs::read(&merged_path).await.unwrap(), payload);
}

#[tokio::test]
async fn interrupted_merge_leaves_flags_reset_so_next_download_retries() {
    // Two phases:
    //   Phase 1 — wait path (issue #51): set `mergeStartedAt` directly,
    //   spawn a fake merger that flips `mergedAt` after 50ms, confirm the
    //   download serves the pre-staged merged blob.
    //   Phase 2 — reset retry: clear both flags + remove the staged blob,
    //   confirm the next download re-claims the CAS and re-merges parts.
    let h = harness().await;
    let token = write_token();

    let upload_id = upload_id_from_create_entry(&h, &token).await;
    let payload = b"interrupt-test";
    let req = put_upload(
        upload_id,
        &format!("comp=block&blockid={}", blockid_48(0)),
        Body::from(Bytes::copy_from_slice(payload)),
    );
    h.router.clone().oneshot(req).await.unwrap();
    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    let pool = h.db.as_sqlite_pool().expect("SQLite test harness");
    let location_id: String =
        sqlx::query_scalar("SELECT locationId FROM cache_entries WHERE id = ?")
            .bind(&entry_id)
            .fetch_one(pool)
            .await
            .unwrap();

    // Phase 1.
    let merged_path = h.tmp.path().join(upload_id.to_string()).join("merged");
    tokio::fs::write(&merged_path, payload).await.unwrap();
    sqlx::query("UPDATE storage_locations SET mergeStartedAt = ? WHERE id = ?")
        .bind(now_ms())
        .bind(&location_id)
        .execute(pool)
        .await
        .unwrap();

    let pool_clone = pool.clone();
    let location_id_clone = location_id.clone();
    let kicker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        sqlx::query("UPDATE storage_locations SET mergedAt = ? WHERE id = ?")
            .bind(now_ms())
            .bind(&location_id_clone)
            .execute(&pool_clone)
            .await
            .unwrap();
    });

    let resp = h
        .router
        .clone()
        .oneshot(get_download(&entry_id))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(collect_body(resp).await, payload);
    kicker.await.unwrap();

    // Phase 2.
    let _ = tokio::fs::remove_file(&merged_path).await;
    sqlx::query("UPDATE storage_locations SET mergeStartedAt = NULL, mergedAt = NULL WHERE id = ?")
        .bind(&location_id)
        .execute(pool)
        .await
        .unwrap();

    let resp = h
        .router
        .clone()
        .oneshot(get_download(&entry_id))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(collect_body(resp).await, payload);

    wait_for_merge_complete(&h, &entry_id).await;
    assert_eq!(tokio::fs::read(&merged_path).await.unwrap(), payload);
}
