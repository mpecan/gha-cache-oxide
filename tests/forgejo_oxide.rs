//! Oxide-specific behaviour of the Forgejo v1 dialect — everything
//! `tests/forgejo.rs` (the port of act's `handler_test.go`) does not
//! cover: parallel / out-of-order / late chunks, commit races,
//! repository isolation, matching order, metrics, and the
//! disabled-by-default mount. Failure paths and cleanup interaction
//! live in `tests/forgejo_failures.rs`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod forgejo_common;

use forgejo_common::{Runner, VERSION, random_bytes, spawn, tick};
use reqwest::{Method, StatusCode};
use serde_json::Value;

async fn setup() -> (forgejo_common::Server, Runner) {
    let srv = spawn(Some(forgejo_common::SECRET)).await;
    let runner = Runner::new(&srv);
    (srv, runner)
}

async fn folder_of(srv: &forgejo_common::Server, r: &Runner, key: &str) -> String {
    let (_, body) = r.find(key, VERSION).await;
    let location = body.unwrap()["archiveLocation"]
        .as_str()
        .unwrap()
        .to_string();
    let entry_id = location.rsplit('/').next().unwrap().to_string();
    srv.db
        .find_location_for_entry(&entry_id)
        .await
        .unwrap()
        .unwrap()
        .folder_name
}

// ---- chunks ------------------------------------------------------------

/// `actions/cache` uploads chunks in parallel; they can land in any
/// order. Chunks here are uploaded concurrently, last-first.
#[tokio::test]
async fn out_of_order_parallel_chunks_reassemble() {
    let (srv, r) = setup().await;
    let chunk = 1024 * 1024 + 7;
    let content = random_bytes(chunk * 4 + 123);
    let id = r.reserve("out_of_order", VERSION, content.len()).await;

    let uploads = content.chunks(chunk).enumerate().rev().map(|(i, part)| {
        let start = i * chunk;
        let range = format!("bytes {start}-{}/*", start + part.len() - 1);
        let r = r.clone();
        let part = part.to_vec();
        async move { r.patch(id, &range, part).await }
    });
    for status in futures::future::join_all(uploads).await {
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(r.commit(id, Some(content.len())).await, StatusCode::OK);

    let (_, bytes) = r.find_and_download("out_of_order", VERSION).await;
    assert_eq!(bytes.len(), content.len());
    assert!(
        bytes.as_ref() == content.as_slice(),
        "reassembled bytes differ"
    );
    // Second download serves the lazily merged blob; must be identical.
    let (_, again) = r.find_and_download("out_of_order", VERSION).await;
    assert!(again.as_ref() == content.as_slice());

    let m = &srv.metrics.forgejo;
    let t = m.sources.totals();
    assert_eq!(t.upload_bytes.get(), content.len() as u64);
    assert_eq!(t.download_bytes.get(), 2 * content.len() as u64);
    assert_eq!(t.commits.get(), 1);
}

#[tokio::test]
async fn retried_chunk_overwrites_instead_of_duplicating() {
    let (_srv, r) = setup().await;
    let content = random_bytes(200);
    let id = r.reserve("retried_chunk", VERSION, 200).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", vec![9; 100]).await,
        StatusCode::OK
    );
    assert_eq!(
        r.patch(id, "bytes 0-99/*", content[..100].to_vec()).await,
        StatusCode::OK
    );
    assert_eq!(
        r.patch(id, "bytes 100-199/*", content[100..].to_vec())
            .await,
        StatusCode::OK
    );
    assert_eq!(r.commit(id, Some(200)).await, StatusCode::OK);
    let (_, bytes) = r.find_and_download("retried_chunk", VERSION).await;
    assert_eq!(bytes.as_ref(), content.as_slice());
}

#[tokio::test]
async fn commit_with_gap_between_chunks_is_rejected() {
    let (srv, r) = setup().await;
    let id = r.reserve("gap", VERSION, 300).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", vec![1; 100]).await,
        StatusCode::OK
    );
    assert_eq!(
        r.patch(id, "bytes 200-299/*", vec![3; 100]).await,
        StatusCode::OK
    );
    let folder = srv
        .db
        .find_upload_by_id(id)
        .await
        .unwrap()
        .unwrap()
        .folder_name;
    assert_eq!(r.commit(id, None).await, StatusCode::BAD_REQUEST);
    assert_eq!(r.find("gap", VERSION).await.0, StatusCode::NO_CONTENT);
    assert!(srv.db.find_upload_by_id(id).await.unwrap().is_none());
    assert!(srv.storage.list_folder(&folder).await.unwrap().is_empty());
}

#[tokio::test]
async fn commit_without_chunks_is_rejected() {
    let (_srv, r) = setup().await;
    let id = r.reserve("no_chunks", VERSION, 0).await;
    assert_eq!(r.commit(id, None).await, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn repositories_are_isolated() {
    let (_srv, r) = setup().await;
    r.upload_normally("shared_key", VERSION, &[5; 32]).await;
    let other = r.with_repo("someone/else");

    assert_eq!(
        other.find("shared_key", VERSION).await.0,
        StatusCode::NO_CONTENT
    );
    let (_, body) = r.find("shared_key", VERSION).await;
    let location = body.unwrap()["archiveLocation"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(other.download(&location).await.0, StatusCode::NOT_FOUND);

    let id = r.reserve("pending", VERSION, 10).await;
    assert_eq!(
        other.patch(id, "bytes 0-9/*", vec![0; 10]).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(other.commit(id, None).await, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn version_must_match() {
    let (_srv, r) = setup().await;
    r.upload_normally("versioned", VERSION, &[1; 10]).await;
    assert_eq!(
        r.find("versioned", "other-version").await.0,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn empty_keys_do_not_match_everything() {
    let (_srv, r) = setup().await;
    r.upload_normally("anything", VERSION, &[1; 10]).await;
    assert_eq!(r.find("", VERSION).await.0, StatusCode::NO_CONTENT);
    assert_eq!(r.find(",", VERSION).await.0, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn find_purges_entry_whose_blob_vanished() {
    let (srv, r) = setup().await;
    r.upload_normally("vanished", VERSION, &[1; 10]).await;
    // The download in `upload_normally` started a background lazy merge;
    // let it land first, or it can write `merged` after the clear below.
    let (_, body) = r.find("vanished", VERSION).await;
    let location = body.unwrap()["archiveLocation"]
        .as_str()
        .unwrap()
        .to_string();
    let entry_id = location.rsplit('/').next().unwrap().to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let loc = srv
            .db
            .find_location_for_entry(&entry_id)
            .await
            .unwrap()
            .unwrap();
        if loc.parts_deleted_at.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "lazy merge never finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    srv.storage.clear().await.unwrap();
    assert_eq!(r.find("vanished", VERSION).await.0, StatusCode::NO_CONTENT);
    assert!(
        srv.db
            .find_cache_entry_by_id(&entry_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn archive_location_points_back_through_the_proxy() {
    let (_srv, r) = setup().await;
    r.upload_normally("location", VERSION, &[1; 10]).await;
    let (_, body) = r.find("location", VERSION).await;
    let location = body.unwrap()["archiveLocation"]
        .as_str()
        .unwrap()
        .to_string();
    let prefix = format!(
        "{}/{}/_apis/artifactcache/artifacts/",
        forgejo_common::PROXY_HOST,
        forgejo_common::RUN_ID
    );
    assert!(location.starts_with(&prefix), "{location}");
}

#[tokio::test]
async fn lookups_are_counted_and_exported() {
    let (srv, r) = setup().await;
    r.upload_normally("counted", VERSION, &[1; 10]).await; // 1 hit
    r.find("counted-miss", VERSION).await;
    let text = reqwest::get(format!("{}/metrics", srv.base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let repo = forgejo_common::REPO;
    assert!(text.contains(&format!(
        "gha_cache_oxide_forgejo_cache_lookups_total{{result=\"hit\",repo=\"{repo}\",key_prefix=\"counted\"}} 1\n"
    )));
    assert!(text.contains(&format!(
        "gha_cache_oxide_forgejo_cache_lookups_total{{result=\"miss\",repo=\"{repo}\",key_prefix=\"counted-miss\"}} 1\n"
    )));
    assert!(text.contains(&format!(
        "gha_cache_oxide_forgejo_commits_total{{repo=\"{repo}\",key_prefix=\"counted\"}} 1\n"
    )));
}

/// Without `FORGEJO_CACHE_SECRET` nothing is mounted: the request falls
/// through to the catch-all proxy (unroutable in the harness → 502).
#[tokio::test]
async fn dialect_is_disabled_without_secret() {
    let srv = spawn(None).await;
    let r = Runner::new(&srv);
    let resp = r
        .request(Method::GET, "/cache?keys=k&version=v")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}

// ---- commit races (review: commit must claim the upload) -------------

/// A client or proxy retrying `POST caches/:id` while the first commit
/// is still copying: exactly one commit wins and the entry survives.
#[tokio::test]
async fn concurrent_duplicate_commits_keep_the_entry() {
    let (_srv, r) = setup().await;
    for round in 0..10 {
        let key = format!("dup_commit_{round}");
        let content = random_bytes(3 * 1000 + 17);
        let id = r.reserve(&key, VERSION, content.len()).await;
        for (i, part) in content.chunks(1000).enumerate() {
            let start = i * 1000;
            let range = format!("bytes {start}-{}/*", start + part.len() - 1);
            assert_eq!(r.patch(id, &range, part.to_vec()).await, StatusCode::OK);
        }
        let mut statuses: [StatusCode; 2] = tokio::join!(
            r.commit(id, Some(content.len())),
            r.commit(id, Some(content.len()))
        )
        .into();
        statuses.sort();
        assert_eq!(
            statuses,
            [StatusCode::OK, StatusCode::NOT_FOUND],
            "round {round}"
        );
        let (_, bytes) = r.find_and_download(&key, VERSION).await;
        assert!(
            bytes.as_ref() == content.as_slice(),
            "round {round}: entry damaged"
        );
    }
}

/// A chunk that is still streaming when the commit lands must not leave
/// an orphaned object behind, and must not report success.
#[tokio::test]
async fn chunk_in_flight_during_commit_is_dropped_with_404() {
    let (srv, r) = setup().await;
    let first = random_bytes(100);
    let id = r.reserve("late_chunk", VERSION, 100).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", first.clone()).await,
        StatusCode::OK
    );

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(1);
    let late = r
        .request(Method::PATCH, &format!("/caches/{id}"))
        .header("Content-Range", "bytes 100-199/*")
        .body(reqwest::Body::wrap_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .send();
    let late = tokio::spawn(late);
    tx.send(Ok(vec![1; 10])).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // The in-flight chunk is not visible yet, so this commits 100 bytes.
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::OK);
    tx.send(Ok(vec![2; 90])).await.unwrap();
    drop(tx);
    assert_eq!(late.await.unwrap().unwrap().status(), StatusCode::NOT_FOUND);

    let folder = folder_of(&srv, &r, "late_chunk").await;
    let chunks = srv
        .storage
        .list_folder(&format!("{folder}/chunks"))
        .await
        .unwrap();
    assert!(chunks.is_empty(), "late chunk leaked: {chunks:?}");
    let (_, bytes) = r.find_and_download("late_chunk", VERSION).await;
    assert_eq!(bytes.as_ref(), first.as_slice());
}

#[tokio::test]
async fn recommitting_a_key_replaces_the_entry_and_its_blobs() {
    let (srv, r) = setup().await;
    r.upload_normally("recommit", VERSION, &[1; 50]).await;
    let old_folder = folder_of(&srv, &r, "recommit").await;
    tick().await;
    r.upload_normally("recommit", VERSION, &[2; 70]).await;

    let (_, bytes) = r.find_and_download("recommit", VERSION).await;
    assert_eq!(bytes.as_ref(), [2; 70].as_slice());
    assert_eq!(srv.db.count_storage_locations().await.unwrap(), 1);
    assert!(
        srv.storage
            .list_folder(&old_folder)
            .await
            .unwrap()
            .is_empty()
    );
}

// ---- matching ---------------------------------------------------------

/// Prefix matches return the most recently committed entry, not the
/// lexicographically first.
#[tokio::test]
async fn prefix_match_is_newest_first() {
    let (_srv, r) = setup().await;
    r.upload_normally("nf_z", VERSION, &[1; 10]).await;
    tick().await;
    r.upload_normally("nf_a", VERSION, &[2; 10]).await;
    let (key, _) = r.find_and_download("nf_", VERSION).await;
    assert_eq!(key, "nf_a");

    tick().await;
    r.upload_normally("nf_z", VERSION, &[3; 10]).await;
    let (key, _) = r.find_and_download("nf_", VERSION).await;
    assert_eq!(key, "nf_z");
}

#[tokio::test]
async fn reserve_accepts_json_without_content_type() {
    let (_srv, r) = setup().await;
    let resp = r
        .request(Method::POST, "/caches")
        .body(format!(r#"{{"key":"no_ct","version":"{VERSION}"}}"#))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn unknown_sub_path_is_404_not_proxied() {
    let (_srv, r) = setup().await;
    let resp = r.request(Method::GET, "/nope").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "not found");
}

#[tokio::test]
async fn metrics_are_not_mounted_without_the_dialect() {
    let srv = spawn(None).await;
    let resp = reqwest::get(format!("{}/metrics", srv.base)).await.unwrap();
    assert_ne!(resp.status(), StatusCode::OK);
}

/// Regression (trial, setup-node `cache: pnpm`): the restored key must
/// equal the primary key byte-for-byte, or `actions/setup-node`
/// (`primaryKey === matchedKey`) re-uploads the whole cache on a hit.
#[tokio::test]
async fn exact_hit_returns_the_requested_key_case() {
    let (_srv, r) = setup().await;
    let key = "node-cache-Linux-x64-pnpm-AbC123";
    r.upload_normally(key, VERSION, &[1; 10]).await;

    let (_, body) = r.find(key, VERSION).await;
    assert_eq!(body.unwrap()["cacheKey"], key);

    // A restore key that matches exactly is echoed too.
    let (_, body) = r
        .find(&format!("node-cache-Linux-x64-pnpm-nomatch,{key}"), VERSION)
        .await;
    assert_eq!(body.unwrap()["cacheKey"], key);

    // A prefix hit has no requested spelling to echo: stored key.
    let (_, body) = r.find("node-cache-Linux-x64-pnpm-", VERSION).await;
    assert_eq!(body.unwrap()["cacheKey"], key.to_lowercase());
}

/// Commit starts a background merge, so the entry is merged before
/// anyone downloads it (first restore = one read of `merged`).
#[tokio::test]
async fn commit_merges_in_the_background_before_first_download() {
    let (srv, r) = setup().await;
    let content = random_bytes(3 * 1000 + 5);
    let id = r.reserve("bg_merge", VERSION, content.len()).await;
    for (i, part) in content.chunks(1000).enumerate() {
        let start = i * 1000;
        let range = format!("bytes {start}-{}/*", start + part.len() - 1);
        assert_eq!(r.patch(id, &range, part.to_vec()).await, StatusCode::OK);
    }
    assert_eq!(r.commit(id, Some(content.len())).await, StatusCode::OK);

    // No download yet: poll the location until the merge lands.
    let (_, body) = r.find("bg_merge", VERSION).await;
    let location = body.unwrap()["archiveLocation"]
        .as_str()
        .unwrap()
        .to_string();
    let entry_id = location.rsplit('/').next().unwrap().to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let merged = loop {
        let loc = srv
            .db
            .find_location_for_entry(&entry_id)
            .await
            .unwrap()
            .unwrap();
        // `completed` is bumped just after finalise commits; wait for both.
        if loc.parts_deleted_at.is_some() && srv.metrics.merges.completed.get() == 1 {
            break loc;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background merge never finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert!(merged.merged_at.is_some());
    assert_eq!(srv.metrics.merges.completed.get(), 1);

    let (_, bytes) = r.download(&location).await;
    assert!(bytes.as_ref() == content.as_slice(), "merged blob differs");
    let text = reqwest::get(format!("{}/metrics", srv.base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(text.contains("gha_cache_oxide_merges_total{result=\"ok\"} 1\n"));
    assert!(text.contains("gha_cache_oxide_merge_duration_seconds_count 1\n"));
}

/// Lookups, bytes and commits are attributed to the MAC-validated repo
/// and the key's tool prefix, so misses can be traced to a workflow.
#[tokio::test]
async fn metrics_are_attributed_per_repo_and_key_prefix() {
    let (srv, r) = setup().await;
    let images = r.with_repo("mpecan/images");
    let blob = "buildkit-blob-1-sha256:0123456789abcdef";
    images.upload_normally(blob, VERSION, &[1; 40]).await;
    assert_eq!(
        images.find("index-buildkit-1-89abcdef", VERSION).await.0,
        StatusCode::NO_CONTENT
    );
    r.upload_normally("v0-rust-build-Linux-x64-abc", VERSION, &[2; 25])
        .await;

    let s = &srv.metrics.forgejo.sources;
    let bk = s.get("mpecan/images", blob);
    assert_eq!((bk.hits.get(), bk.misses.get()), (1, 0));
    assert_eq!(bk.upload_bytes.get(), 40);
    assert_eq!(bk.download_bytes.get(), 40);
    assert_eq!(bk.commits.get(), 1);
    assert_eq!(s.get("mpecan/images", "index-buildkit-x").misses.get(), 1);
    let rust = s.get(forgejo_common::REPO, "v0-rust-anything");
    assert_eq!((rust.hits.get(), rust.upload_bytes.get()), (1, 25));
    // Nothing leaks across repos.
    assert_eq!(s.get(forgejo_common::REPO, blob).hits.get(), 0);

    let text = reqwest::get(format!("{}/metrics", srv.base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(text.contains(
        "gha_cache_oxide_forgejo_download_bytes_total{repo=\"mpecan/images\",key_prefix=\"buildkit-blob\"} 40\n"
    ));
}
