//! `GetCacheEntryDownloadURL` purge-and-retry on missing storage (#72).
//!
//! Split out of `tests/twirp_download.rs` to keep that file under the
//! 500-line soft limit. Shares the harness in `tests/twirp_common/mod.rs`.
//!
//! These four tests pin the contract:
//! - parts folder empty → purge, retry, return restore-key URL
//! - merged blob gone with `ENABLE_DIRECT_DOWNLOADS=true` → same shape
//! - no candidate has storage → `{ok:false}` (not a stale URL)
//! - retry cap honoured (3 probes max per request)

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod twirp_common;

use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;

use gha_cache_oxide::storage::StorageAdapter;
use twirp_common::{
    BASE_PATH, HarnessOpts, SIGNED_URL, SigningFilesystem, body_json, harness, harness_with,
    plant_merged, post, seed_cache_entry_with_location, seed_cache_entry_with_location_no_storage,
    write_token,
};

/// Spins up a `signing_harness` equivalent — direct-download enabled
/// against a `SigningFilesystem` shim. Local copy because
/// `tests/twirp_download.rs` is a separate test binary; sharing
/// requires moving to `twirp_common`.
async fn signing_harness(enable_flag: bool) -> twirp_common::Harness {
    let root = TempDir::new().unwrap();
    let storage: Arc<dyn StorageAdapter> = Arc::new(SigningFilesystem::new(root.path()));
    // Leak the root so the underlying tmpdir outlives the Harness.
    let _ = Box::leak(Box::new(root));
    harness_with(HarnessOpts {
        enable_direct_downloads: enable_flag,
        storage: Some(storage),
    })
    .await
}

/// Not-yet-merged path: the parts folder is empty for the
/// exact-primary match, but a restore-key match has parts on disk.
/// The handler must purge the broken row and return the URL pointing
/// at the restore-key entry's id.
#[tokio::test]
async fn download_url_purges_when_parts_folder_empty_then_returns_restore() {
    let h = harness().await;
    let token = write_token();

    // Exact-primary match: NOT merged, parts folder empty.
    let (broken_entry_id, _broken_location_id) =
        seed_cache_entry_with_location_no_storage(&h, "primary-k", "refs/heads/main", 1_000).await;

    // Restore-key match (same scope): NOT merged, parts folder populated.
    let (good_entry_id, good_location_id) =
        seed_cache_entry_with_location(&h, "fallback-k", "refs/heads/main", 500).await;
    let _ = good_location_id;

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({
            "key":"primary-k",
            "version":"v1",
            "restore_keys":["fallback-k"],
        }),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(
        body["signed_download_url"],
        json!(format!("http://localhost:3000/download/{good_entry_id}")),
        "URL must point at the restore-key entry, not the broken primary",
    );
    assert_eq!(body["matched_key"], json!("fallback-k"));

    // Broken row is gone; the surviving one is still here.
    assert!(
        h.db.find_cache_entry_by_id(&broken_entry_id)
            .await
            .unwrap()
            .is_none(),
        "broken cache_entries row must be purged",
    );
    assert!(
        h.db.find_cache_entry_by_id(&good_entry_id)
            .await
            .unwrap()
            .is_some(),
    );
}

/// Merged + `ENABLE_DIRECT_DOWNLOADS=true`: the location's folder is
/// empty (the merged blob is gone). Probe must catch it, purge, and
/// re-match. The fallback entry's location is also merged with a real
/// `merged` file on disk.
#[tokio::test]
async fn download_url_purges_when_merged_blob_missing_under_direct_downloads() {
    let h = signing_harness(true).await;
    let token = write_token();

    // Broken merged: row marked merged, NO files on disk (mimics the
    // post-`cleanup:parts` state where parts were reaped and the
    // merged blob has been independently lost).
    let (broken_entry_id, broken_location_id) =
        seed_cache_entry_with_location_no_storage(&h, "primary-k", "refs/heads/main", 1_000).await;
    h.db.mark_merged(&broken_location_id, 2_000).await.unwrap();

    // Good merged: row marked merged, `<folder>/merged` planted.
    let (good_entry_id, good_location_id) =
        seed_cache_entry_with_location_no_storage(&h, "fallback-k", "refs/heads/main", 500).await;
    h.db.mark_merged(&good_location_id, 2_000).await.unwrap();
    plant_merged(&h, &format!("folder-{good_location_id}")).await;

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({
            "key":"primary-k",
            "version":"v1",
            "restore_keys":["fallback-k"],
        }),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    // Both entries' merged URL would resolve to SIGNED_URL (the shim
    // returns the same constant for any path), so we can't distinguish
    // via the URL string. The DB assertions are the load-bearing pin.
    assert_eq!(body["signed_download_url"], json!(SIGNED_URL));
    assert_eq!(body["matched_key"], json!("fallback-k"));

    assert!(
        h.db.find_cache_entry_by_id(&broken_entry_id)
            .await
            .unwrap()
            .is_none(),
        "broken cache_entries row must be purged",
    );
    assert!(
        h.db.find_cache_entry_by_id(&good_entry_id)
            .await
            .unwrap()
            .is_some(),
    );
}

/// All candidates have empty storage. After probing every match (capped
/// by the retry budget), the handler returns `{ok:false}` rather than
/// a stale URL.
#[tokio::test]
async fn download_url_returns_ok_false_when_no_candidate_has_storage() {
    let h = harness().await;
    let token = write_token();

    let (entry_a, _loc_a) =
        seed_cache_entry_with_location_no_storage(&h, "primary-k", "refs/heads/main", 1_000).await;
    let (entry_b, _loc_b) =
        seed_cache_entry_with_location_no_storage(&h, "fallback-k", "refs/heads/main", 500).await;
    // No part files planted — both folders are empty.

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({
            "key":"primary-k",
            "version":"v1",
            "restore_keys":["fallback-k"],
        }),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(false));
    // No download URL field on ok:false responses.
    assert!(body.get("signed_download_url").is_none());

    // Both entries got purged en route to the ok:false.
    assert!(
        h.db.find_cache_entry_by_id(&entry_a)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        h.db.find_cache_entry_by_id(&entry_b)
            .await
            .unwrap()
            .is_none()
    );
}

/// The handler caps storage probes at 3 per request. With four broken
/// candidates lined up, only three are probed/purged before the
/// handler bails with `{ok:false}` — the fourth entry survives.
#[tokio::test]
async fn download_url_caps_at_three_probes() {
    let h = harness().await;
    let token = write_token();

    // All four entries use the no_storage seeder so every probe sees
    // an empty parts folder and the loop hits the cap before exhausting.
    let (entry_primary, _) =
        seed_cache_entry_with_location_no_storage(&h, "primary-k", "refs/heads/main", 4_000).await;
    let (entry_restore_1, _) =
        seed_cache_entry_with_location_no_storage(&h, "rk-1", "refs/heads/main", 3_000).await;
    let (entry_restore_2, _) =
        seed_cache_entry_with_location_no_storage(&h, "rk-2", "refs/heads/main", 2_000).await;
    let (entry_restore_3, _) =
        seed_cache_entry_with_location_no_storage(&h, "rk-3", "refs/heads/main", 1_000).await;

    let req = post(
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        Some(&token),
        &json!({
            "key":"primary-k",
            "version":"v1",
            "restore_keys":["rk-1","rk-2","rk-3"],
        }),
    );
    let resp = h.router.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(false));

    // Three were purged (the cap), one survived.
    let surviving = [
        h.db.find_cache_entry_by_id(&entry_primary)
            .await
            .unwrap()
            .is_some(),
        h.db.find_cache_entry_by_id(&entry_restore_1)
            .await
            .unwrap()
            .is_some(),
        h.db.find_cache_entry_by_id(&entry_restore_2)
            .await
            .unwrap()
            .is_some(),
        h.db.find_cache_entry_by_id(&entry_restore_3)
            .await
            .unwrap()
            .is_some(),
    ];
    let surviving_count = surviving.iter().filter(|b| **b).count();
    assert_eq!(
        surviving_count, 1,
        "exactly one entry must survive the 3-probe cap; got {surviving:?}",
    );
}
