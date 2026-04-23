//! End-to-end conformance harness (#10).
//!
//! Boots the Rust server on a random port, drives the five protocol
//! routes via `reqwest`, and pins each response against a golden JSON
//! file under `tests/golden/`. Shared fixtures live in
//! `tests/e2e_common/mod.rs`.
//!
//! # Regenerating goldens
//!
//! ```sh
//! UPDATE_GOLDEN=1 cargo test --test e2e -- --nocapture
//! ```
//!
//! Then eyeball `tests/golden/*.json` and commit them. Subsequent runs
//! without `UPDATE_GOLDEN` diff actual vs stored and panic with a
//! line-oriented diff on mismatch.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod e2e_common;

use serde_json::{Value, json};

use e2e_common::{
    BASE_PATH, Dynamics, ServerHandle, assert_golden, blockid_48, capture_envelope,
    compare_to_golden_value, extract_cache_entry_id, extract_upload_id, get, normalize, post_json,
    put_bytes, spawn_server,
};

/// The payload uploaded and downloaded in the round-trip test. Chosen
/// to be short, ASCII-only, and deterministic so the golden's `body_len`
/// is stable across platforms.
const PAYLOAD: &[u8] = b"hello, world - golden bytes 0\n";
const KEY: &str = "e2e-key";
const VERSION: &str = "e2e-version";

#[tokio::test]
async fn full_round_trip_matches_goldens() {
    let srv = spawn_server().await;
    let mut d = Dynamics::none();

    // 1. CreateCacheEntry → captures upload_id for the rest of the run.
    d.upload_id = Some(step_create_cache_entry(&srv).await);

    // 2. PUT upload (single part).
    step_upload_part(&srv, d.upload_id.unwrap(), &d).await;

    // 3. FinalizeCacheEntryUpload (asserts entry_id == upload_id).
    step_finalize(&srv, d.upload_id.unwrap(), &d).await;

    // 4. GetCacheEntryDownloadURL → captures cache_entry_id.
    d.cache_entry_id = Some(step_get_download_url(&srv, &d).await);

    // 5. GET /download/{id} — asserts bytes match PAYLOAD exactly.
    step_download(&srv, d.cache_entry_id.as_ref().unwrap(), &d).await;
}

async fn step_create_cache_entry(srv: &ServerHandle) -> i64 {
    let resp = post_json(
        srv,
        &format!("{BASE_PATH}/CreateCacheEntry"),
        &json!({"key": KEY, "version": VERSION}),
    )
    .await;
    let (status, envelope) = capture_envelope(resp, &["content-type"], true).await;
    assert_eq!(status, 200, "CreateCacheEntry must return 200");
    let upload_id = extract_upload_id(&envelope["body"], &srv.base_url);
    let mut d = Dynamics::none();
    d.upload_id = Some(upload_id);
    assert_golden(
        "create_cache_entry",
        &normalize(envelope, &srv.base_url, &d),
    );
    upload_id
}

async fn step_upload_part(srv: &ServerHandle, upload_id: i64, d: &Dynamics) {
    let path = format!(
        "/devstoreaccount1/upload/{upload_id}?comp=block&blockid={}",
        blockid_48(0)
    );
    let resp = put_bytes(srv, &path, PAYLOAD.to_vec()).await;
    let (status, envelope) = capture_envelope(resp, &["x-ms-request-id"], false).await;
    assert_eq!(status, 201, "PUT upload must return 201");
    assert_golden(
        "upload_part_blocklist",
        &normalize(envelope, &srv.base_url, d),
    );
}

async fn step_finalize(srv: &ServerHandle, upload_id: i64, d: &Dynamics) {
    let resp = post_json(
        srv,
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        &json!({"key": KEY, "version": VERSION}),
    )
    .await;
    let (status, envelope) = capture_envelope(resp, &["content-type"], true).await;
    assert_eq!(status, 200, "Finalize must return 200");
    let entry_id_str = envelope["body"]["entry_id"].as_str().unwrap().to_string();
    assert_eq!(
        entry_id_str,
        upload_id.to_string(),
        "entry_id must be the upload id stringified"
    );
    assert_golden(
        "finalize_cache_entry_upload",
        &normalize(envelope, &srv.base_url, d),
    );
}

async fn step_get_download_url(srv: &ServerHandle, d: &Dynamics) -> String {
    let resp = post_json(
        srv,
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        &json!({"key": KEY, "version": VERSION}),
    )
    .await;
    let (status, envelope) = capture_envelope(resp, &["content-type"], true).await;
    assert_eq!(status, 200, "GetCacheEntryDownloadURL must return 200");
    let cache_entry_id = extract_cache_entry_id(&envelope["body"], &srv.base_url);
    let mut d_full = d.clone();
    d_full.cache_entry_id = Some(cache_entry_id.clone());
    assert_golden(
        "get_cache_entry_download_url",
        &normalize(envelope, &srv.base_url, &d_full),
    );
    cache_entry_id
}

async fn step_download(srv: &ServerHandle, cache_entry_id: &str, d: &Dynamics) {
    let resp = get(srv, &format!("/download/{cache_entry_id}")).await;
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string());
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        body.as_ref(),
        PAYLOAD,
        "downloaded bytes must equal uploaded payload"
    );
    let envelope: Value = json!({
        "status": status,
        "headers": { "content-type": content_type },
        "body": { "body_len": body.len() },
    });
    assert_golden("download", &normalize(envelope, &srv.base_url, d));
}

/// Acceptance criterion: dropping a response field must cause the test
/// to fail with a meaningful diff. Drives the comparator directly
/// without touching disk so it works before goldens exist.
#[test]
fn dropping_a_field_fails_with_meaningful_diff() {
    let expected = json!({
        "status": 200,
        "headers": {"content-type": "application/json"},
        "body": {
            "ok": true,
            "signed_upload_url": "placeholder/devstoreaccount1/upload/0",
        }
    });
    let mut actual = expected.clone();
    actual["body"]
        .as_object_mut()
        .unwrap()
        .remove("signed_upload_url");

    let err = std::panic::catch_unwind(|| {
        compare_to_golden_value(&expected, &actual, "synthetic_drop_field");
    })
    .expect_err("comparator must panic on mismatch");

    let msg: String = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&'static str>().map(|s| (*s).to_string()))
        .expect("panic payload should be a string");
    assert!(
        msg.contains("signed_upload_url"),
        "diff should mention the dropped field, got: {msg}"
    );
    assert!(
        msg.contains("- ") && msg.contains("+ "),
        "diff should include +/- markers, got: {msg}"
    );
    assert!(
        msg.contains("golden mismatch"),
        "diff preface should mention golden mismatch, got: {msg}"
    );
}

/// Sanity check: the comparator passes silently when values match. Pins
/// the "no panic means success" branch of `compare_to_golden_value`.
#[test]
fn identical_values_compare_equal() {
    let v = json!({ "ok": true, "n": 3, "xs": [1, 2, 3] });
    compare_to_golden_value(&v, &v, "synthetic_equal");
}
