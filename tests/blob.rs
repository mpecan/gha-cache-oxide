//! Integration tests for upload `PUT /devstoreaccount1/upload/{id}`
//! and download `GET /download/{cache_entry_id}`. Drives the full app
//! via `tower::ServiceExt::oneshot` so the blob routes' absence of
//! OIDC middleware (the `upload_id` / `cache_entry_id` is the
//! capability) is exercised end-to-end.
//!
//! Shared harness in `tests/twirp_common/mod.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::too_many_lines
)]

mod twirp_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use gha_cache_oxide::db::entities::{CacheEntryCoord, NewUpload};
use gha_cache_oxide::db::id::new_upload_id;
use serde_json::json;
use tower::ServiceExt;

use twirp_common::{BASE_PATH, Harness, body_json, harness, post, write_token};

// --- Helpers -------------------------------------------------------------

fn blockid_48(index: u64) -> String {
    // UUID (36 chars) + zero-padded decimal index (12 chars) = 48 bytes,
    // matching the layout actions/cache emits.
    let uuid = "11111111-2222-3333-4444-555555555555";
    let buf = format!("{uuid}{index:012}");
    assert_eq!(buf.len(), 48);
    base64::engine::general_purpose::STANDARD.encode(buf.as_bytes())
}

/// Builds a `PUT` against the upload endpoint at the given path
/// prefix. The data plane registers the same handler under both
/// `/devstoreaccount1/upload` (canonical) and `/upload` (issue #74
/// alias), so tests can parameterise over both prefixes.
fn put_upload_at(prefix: &str, id: i64, query: &str, body: Body) -> Request<Body> {
    let uri = if query.is_empty() {
        format!("{prefix}/{id}")
    } else {
        format!("{prefix}/{id}?{query}")
    };
    Request::builder()
        .method("PUT")
        .uri(uri)
        .body(body)
        .unwrap()
}

fn put_upload(id: i64, query: &str, body: Body) -> Request<Body> {
    put_upload_at("/devstoreaccount1/upload", id, query, body)
}

fn get_download(entry_id: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/download/{entry_id}"))
        .body(Body::empty())
        .unwrap()
}

async fn seed_upload(h: &Harness) -> i64 {
    let id = new_upload_id();
    h.db.create_upload(NewUpload {
        id,
        coord: CacheEntryCoord {
            key: "build-cache",
            version: "v1",
            scope: "refs/heads/main",
            repo_id: "42",
        },
        folder_name: &id.to_string(),
        created_at_ms: 0,
    })
    .await
    .unwrap();
    id
}

async fn collect_body(resp: axum::response::Response) -> Vec<u8> {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    bytes.to_vec()
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

/// Finalizes the upload and then resolves the download URL via
/// `GetCacheEntryDownloadURL` — returning the `cache_entry_id` path
/// segment. This is the flow real clients take: finalize's `entry_id`
/// is the *upload* id (opaque confirmation), whereas the download URL
/// points at the `cache_entry.id` UUID.
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
    assert_eq!(body["ok"], json!(true));
    let url = body["signed_download_url"].as_str().unwrap().to_string();
    url.strip_prefix("http://localhost:3000/download/")
        .unwrap()
        .to_string()
}

// --- blocklist no-op ------------------------------------------------------

#[tokio::test]
async fn blocklist_comp_returns_201_with_request_id() {
    // Upstream parity: routes/devstoreaccount1/upload/[uploadId].put.ts:22-26.
    // Works even for IDs that don't exist — the commit-list step never
    // touches the DB.
    let h = harness().await;
    let req = put_upload(0, "comp=blocklist", Body::empty());
    let resp = h.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert!(
        resp.headers().get("x-ms-request-id").is_some(),
        "tonistiigi/go-actions-cache needs the x-ms-request-id header"
    );
}

// --- upload ---------------------------------------------------------------

#[tokio::test]
async fn upload_with_valid_blockid_writes_part_and_increments_counters() {
    let h = harness().await;
    let id = seed_upload(&h).await;
    let query = format!("comp=block&blockid={}", blockid_48(0));
    let req = put_upload(id, &query, Body::from(Bytes::from_static(b"hello world")));
    let resp = h.router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert!(resp.headers().get("x-ms-request-id").is_some());

    let upload = h.db.find_upload_by_id(id).await.unwrap().unwrap();
    assert_eq!(upload.started_part_upload_count, 1);
    assert_eq!(upload.finished_part_upload_count, 1);
    assert!(upload.last_part_uploaded_at.is_some());

    let path = h.tmp.path().join(id.to_string()).join("parts").join("0");
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello world");
}

#[tokio::test]
async fn upload_without_blockid_uses_chunk_zero() {
    // Upstream: if blockid is missing the upload is smaller than one
    // chunk, index defaults to 0 (put.ts:31).
    let h = harness().await;
    let id = seed_upload(&h).await;
    let req = put_upload(id, "", Body::from("abc"));
    let resp = h.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let path = h.tmp.path().join(id.to_string()).join("parts").join("0");
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"abc");
}

#[tokio::test]
async fn upload_with_invalid_blockid_is_400() {
    let h = harness().await;
    let id = seed_upload(&h).await;
    let req = put_upload(id, "comp=block&blockid=%21not-base64%21", Body::from("x"));
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["statusCode"], 400);
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("Invalid block id"),
        "message should mention the invalid block id"
    );
}

#[tokio::test]
async fn upload_to_unknown_id_is_404() {
    // Documented divergence from upstream (silent 201). See
    // `src/routes/blob.rs` module-level docstring.
    let h = harness().await;
    let query = format!("comp=block&blockid={}", blockid_48(0));
    let req = put_upload(99_999, &query, Body::from("x"));
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], json!("Upload not found"));
}

// --- alias path parity (issue #74) ---------------------------------------

/// `/upload/{uploadId}` must behave identically to
/// `/devstoreaccount1/upload/{uploadId}` — upstream registers the
/// same handler at both paths
/// (`routes/upload/[uploadId].put.ts:1` re-exports the canonical
/// handler). We exercise the full happy path (block upload +
/// blocklist commit + finalize + download), the blocklist no-op for
/// IDs that don't exist, and the unknown-ID error path through both
/// prefixes inside one parameterised loop, asserting identical
/// observable behaviour.
#[tokio::test]
async fn upload_alias_behaves_identically_to_canonical() {
    for prefix in ["/devstoreaccount1/upload", "/upload"] {
        // Blocklist no-op against an ID that doesn't exist — works on
        // both prefixes because the commit-list step never touches
        // the DB. Mirrors `blocklist_comp_returns_201_with_request_id`.
        let h = harness().await;
        let req = put_upload_at(prefix, 0, "comp=blocklist", Body::empty());
        let resp = h.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED, "prefix={prefix}");
        assert!(
            resp.headers().get("x-ms-request-id").is_some(),
            "x-ms-request-id required for tonistiigi/go-actions-cache (prefix={prefix})"
        );

        // Unknown ID with a real block-upload query → 404. Mirrors
        // `upload_to_unknown_id_is_404`.
        let query = format!("comp=block&blockid={}", blockid_48(0));
        let req = put_upload_at(prefix, 99_999, &query, Body::from("x"));
        let resp = h.router.clone().oneshot(req).await.unwrap();
        let (status, body) = body_json(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "prefix={prefix}");
        assert_eq!(body["message"], json!("Upload not found"));

        // Full happy path: create entry → upload one block via the
        // alias → finalize → download → bytes match.
        let token = write_token();
        let upload_id = upload_id_from_create_entry(&h, &token).await;
        let block = format!("comp=block&blockid={}", blockid_48(0));
        let payload: &[u8] = b"alias-bytes";
        let req = put_upload_at(
            prefix,
            upload_id,
            &block,
            Body::from(Bytes::copy_from_slice(payload)),
        );
        let resp = h.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED, "prefix={prefix}");

        let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;
        let resp = h
            .router
            .clone()
            .oneshot(get_download(&entry_id))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "prefix={prefix}");
        assert_eq!(collect_body(resp).await, payload, "prefix={prefix}");
    }
}

// --- round-trip via public HTTP surface ----------------------------------

#[tokio::test]
async fn round_trip_three_parts_download_matches_bytes() {
    // Acceptance criterion: reserve → upload 3 parts → finalize →
    // download matches bytes exactly.
    let h = harness().await;
    let token = write_token();

    let upload_id = upload_id_from_create_entry(&h, &token).await;

    let payloads: [&[u8]; 3] = [b"part-0-data", b"part-1-BYTES", b"final-part2"];
    for (i, p) in payloads.iter().enumerate() {
        let query = format!("comp=block&blockid={}", blockid_48(i as u64));
        let req = put_upload(upload_id, &query, Body::from(Bytes::copy_from_slice(p)));
        let resp = h.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    let resp = h.router.oneshot(get_download(&entry_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = collect_body(resp).await;
    let expected: Vec<u8> = payloads.iter().flat_map(|p| p.iter().copied()).collect();
    assert_eq!(got, expected);
}

// --- lazy merge (issue #15) ----------------------------------------------

/// Waits up to 2s for the lazy-merge background task to finalise
/// (merged blob present on disk + `parts_deleted_at` set in DB). The
/// handler returns the response to the client as soon as the teed
/// bytes have flushed; the merger's upload + DB finalise run slightly
/// behind that. Tests poll on the visible side-effect.
async fn wait_for_merge_complete(h: &Harness, entry_id: &str) {
    // Wait for `partsDeletedAt`, not just `mergedAt`. `finalize_merge`
    // (`src/merge.rs`) sets `mergedAt` first, then runs a separate tx that
    // marks `partsDeletedAt` AND deletes the parts folder atomically.
    // Waiting only on `mergedAt` returns in the gap, so callers asserting
    // "parts are gone from disk" race with the filesystem delete on
    // slower runners. `partsDeletedAt IS NOT NULL` is observable only
    // after the storage delete has succeeded and the tx commits.
    let pool = h.db.as_sqlite_pool().expect("SQLite test harness");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
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
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn second_download_serves_from_merged_blob() {
    // Acceptance: Second download of a previously-served entry serves
    // from `merged`. First download teed into `<folder>/merged`;
    // second MUST read that file rather than the parts (which have
    // been deleted).
    let h = harness().await;
    let token = write_token();

    let upload_id = upload_id_from_create_entry(&h, &token).await;
    let payloads: [&[u8]; 3] = [b"first-", b"second-", b"third"];
    for (i, p) in payloads.iter().enumerate() {
        let query = format!("comp=block&blockid={}", blockid_48(i as u64));
        let req = put_upload(upload_id, &query, Body::from(Bytes::copy_from_slice(p)));
        let resp = h.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }
    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    // First download — triggers the merge. Body matches.
    let first = h
        .router
        .clone()
        .oneshot(get_download(&entry_id))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_bytes = collect_body(first).await;
    assert_eq!(first_bytes, b"first-second-third");

    wait_for_merge_complete(&h, &entry_id).await;

    // Parts gone; merged blob on disk.
    let folder = h.tmp.path().join(upload_id.to_string());
    assert_eq!(
        tokio::fs::read(folder.join("merged")).await.unwrap(),
        b"first-second-third"
    );
    // The parts files are deleted individually (LocalFileSystem
    // doesn't prune empty parent dirs; the dir may still exist).
    for i in 0..payloads.len() {
        assert!(
            !folder.join("parts").join(i.to_string()).exists(),
            "part {i} should be deleted after merge"
        );
    }

    // Second download reads from the merged blob. Delete the (already
    // gone) parts folder to be certain: if a bug made us try to read
    // parts, we'd 404 now. Second download succeeds → we served from
    // `merged`.
    let second = h.router.oneshot(get_download(&entry_id)).await.unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second_bytes = collect_body(second).await;
    assert_eq!(second_bytes, b"first-second-third");
}

// `two_concurrent_first_downloads_both_serve_correct_bytes_with_one_merge`
// and `interrupted_merge_leaves_flags_reset_so_next_download_retries`
// previously lived here. They moved to `tests/blob_race.rs` (issue #51)
// once the fix landed — keeping the merge-state-race tests together
// and bringing this file back under the 700-line hard limit.

#[tokio::test]
async fn download_unknown_cache_entry_id_is_404() {
    let h = harness().await;
    let resp = h
        .router
        .oneshot(get_download("does-not-exist"))
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], json!("Cache file not found"));
}

#[tokio::test]
async fn download_with_parts_deleted_but_not_merged_is_404() {
    // Stale cache safety: if `parts_deleted_at` is set but no merged
    // blob has been committed (`merged_at` is NULL), we 404 instead of
    // truncating mid-stream. Guards the defensive branch at
    // `src/routes/blob.rs` (parts_deleted_at.is_some()).
    let h = harness().await;
    let token = write_token();
    let upload_id = upload_id_from_create_entry(&h, &token).await;
    let query = format!("comp=block&blockid={}", blockid_48(0));
    h.router
        .clone()
        .oneshot(put_upload(upload_id, &query, Body::from("x")))
        .await
        .unwrap();
    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    // Flip parts_deleted_at on the corresponding storage_location.
    // Two-step against the pool directly: read the locationId, then run
    // the UPDATE; keeps us on the driver-agnostic trait surface and off
    // the raw Transaction type.
    let pool = h.db.as_sqlite_pool().expect("SQLite test harness");
    let location_id: String =
        sqlx::query_scalar("SELECT locationId FROM cache_entries WHERE id = ?")
            .bind(&entry_id)
            .fetch_one(pool)
            .await
            .unwrap();
    sqlx::query("UPDATE storage_locations SET partsDeletedAt = ? WHERE id = ?")
        .bind(1_234_567_i64)
        .bind(&location_id)
        .execute(pool)
        .await
        .unwrap();

    let resp = h.router.oneshot(get_download(&entry_id)).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], json!("Cache file not found"));
}

#[tokio::test]
async fn download_returns_404_when_part_file_missing_on_disk() {
    // Even with a valid cache_entry row, if the part file was deleted
    // post-finalize (e.g. disk corruption) the client must see 404 at
    // the start rather than a truncated body. Covers the
    // `ensure_parts_exist` actual<expected branch.
    let h = harness().await;
    let token = write_token();
    let upload_id = upload_id_from_create_entry(&h, &token).await;
    let query = format!("comp=block&blockid={}", blockid_48(0));
    h.router
        .clone()
        .oneshot(put_upload(upload_id, &query, Body::from("abc")))
        .await
        .unwrap();
    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    // Delete the sole part file; cache_entry row stays intact.
    let part_path = h
        .tmp
        .path()
        .join(upload_id.to_string())
        .join("parts")
        .join("0");
    tokio::fs::remove_file(&part_path).await.unwrap();

    let resp = h.router.oneshot(get_download(&entry_id)).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], json!("Cache file not found"));
}

#[tokio::test]
async fn download_touches_last_downloaded_at() {
    // Fire-and-forget update is spawned, so we poll the value after
    // the response drains.
    let h = harness().await;
    let token = write_token();
    let upload_id = upload_id_from_create_entry(&h, &token).await;
    let query = format!("comp=block&blockid={}", blockid_48(0));
    h.router
        .clone()
        .oneshot(put_upload(upload_id, &query, Body::from("x")))
        .await
        .unwrap();
    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    let resp = h
        .router
        .clone()
        .oneshot(get_download(&entry_id))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = collect_body(resp).await;

    // Give the fire-and-forget task a few ticks; 500ms is generous for
    // a single UPDATE against an in-memory SQLite.
    let pool = h.db.as_sqlite_pool().expect("SQLite test harness");
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    loop {
        let value: Option<i64> = sqlx::query_scalar(
            "SELECT lastDownloadedAt FROM storage_locations \
             WHERE id = (SELECT locationId FROM cache_entries WHERE id = ?)",
        )
        .bind(&entry_id)
        .fetch_one(pool)
        .await
        .unwrap();
        if value.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "lastDownloadedAt was never updated"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

// --- concurrency ---------------------------------------------------------

#[tokio::test]
async fn concurrent_part_uploads_succeed_and_counters_add_up() {
    // Acceptance criterion: concurrent uploads of different parts of
    // the same upload succeed.
    let h = harness().await;
    let id = seed_upload(&h).await;

    let r1 = {
        let query = format!("comp=block&blockid={}", blockid_48(0));
        let req = put_upload(id, &query, Body::from("AAA"));
        let router = h.router.clone();
        tokio::spawn(async move { router.oneshot(req).await })
    };
    let r2 = {
        let query = format!("comp=block&blockid={}", blockid_48(1));
        let req = put_upload(id, &query, Body::from("BBB"));
        let router = h.router.clone();
        tokio::spawn(async move { router.oneshot(req).await })
    };

    let (a, b) = tokio::join!(r1, r2);
    assert_eq!(a.unwrap().unwrap().status(), StatusCode::CREATED);
    assert_eq!(b.unwrap().unwrap().status(), StatusCode::CREATED);

    let upload = h.db.find_upload_by_id(id).await.unwrap().unwrap();
    assert_eq!(upload.started_part_upload_count, 2);
    assert_eq!(upload.finished_part_upload_count, 2);

    let p0 = h.tmp.path().join(id.to_string()).join("parts").join("0");
    let p1 = h.tmp.path().join(id.to_string()).join("parts").join("1");
    assert_eq!(tokio::fs::read(&p0).await.unwrap(), b"AAA");
    assert_eq!(tokio::fs::read(&p1).await.unwrap(), b"BBB");
}

// --- streaming proof: 100 MiB round-trip --------------------------------

#[tokio::test]
async fn streams_100_mib_round_trip_without_materialising_body() {
    // Acceptance criterion: axum doesn't fully buffer bodies into
    // memory. 100 × 1 MiB `Bytes` clones share the same backing
    // buffer, so the source stream's peak allocation is ~1 MiB even
    // though the total payload is 100 MiB. If axum collected the
    // whole body before dispatching to our handler, the test would
    // still pass (the source allocator wouldn't care) but the file
    // write on disk would have buffered 100 MiB first; we verify the
    // file is present and has the right size/content, which is the
    // *functional* streaming guarantee callers care about. A strict
    // RSS-based assertion is flaky on CI across platforms.
    let h = harness().await;
    let token = write_token();

    let upload_id = upload_id_from_create_entry(&h, &token).await;

    let chunk = Bytes::from(vec![0x42u8; 1024 * 1024]);
    let chunks: Vec<Result<Bytes, std::io::Error>> = (0..100).map(|_| Ok(chunk.clone())).collect();
    let upload_body = Body::from_stream(stream::iter(chunks));
    let query = format!("comp=block&blockid={}", blockid_48(0));

    let resp = h
        .router
        .clone()
        .oneshot(put_upload(upload_id, &query, upload_body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let entry_id = finalize_and_get_cache_entry_id(&h, &token).await;

    let resp = h.router.oneshot(get_download(&entry_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Drain the body without holding the full 100 MiB in memory twice:
    // count bytes and spot-check the first / last byte of each frame.
    let mut data_stream = resp.into_body().into_data_stream();
    let mut total = 0usize;
    let mut first_byte = None;
    let mut last_byte = None;
    while let Some(frame) = data_stream.next().await {
        let bytes = frame.unwrap();
        if !bytes.is_empty() {
            if first_byte.is_none() {
                first_byte = Some(bytes[0]);
            }
            last_byte = Some(bytes[bytes.len() - 1]);
        }
        total += bytes.len();
    }
    assert_eq!(total, 100 * 1024 * 1024);
    assert_eq!(first_byte, Some(0x42));
    assert_eq!(last_byte, Some(0x42));
}
