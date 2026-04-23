//! `#[ignore]`d variant of the E2E harness that replays the same five
//! requests against a running upstream TypeScript server and diffs
//! normalized responses against our own. Manual verification only —
//! CI inclusion is explicitly out of scope (#10's Out-of-Scope).
//!
//! # Running
//!
//! Start the upstream server locally with `SKIP_TOKEN_VALIDATION=true`
//! (so the synthetic JWT is accepted) and filesystem + `SQLite` storage:
//!
//! ```sh
//! UPSTREAM_URL=http://localhost:3000 \
//!   cargo test --test upstream_diff -- --ignored --nocapture
//! ```
//!
//! If `UPSTREAM_URL` is unset the test early-exits with a skip note —
//! `#[ignore]` already keeps it out of the default run, but we double
//! up so passing `--ignored` alone without the env var doesn't look
//! like a silent success.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod e2e_common;

use reqwest::{Client, Response};
use serde_json::{Value, json};

use e2e_common::{
    BASE_PATH, Dynamics, blockid_48, capture_envelope, compare_to_golden_value, normalize,
    spawn_server, write_token,
};

const PAYLOAD: &[u8] = b"hello, world - upstream diff\n";

#[tokio::test]
#[ignore = "requires UPSTREAM_URL and an upstream server running SKIP_TOKEN_VALIDATION=true"]
async fn responses_match_upstream_byte_for_byte() {
    let Some(upstream_url) = env_or_skip() else {
        return;
    };

    let ours_srv = spawn_server().await;
    let ours = Target::borrow(&ours_srv.base_url, &ours_srv.http, &ours_srv.token);
    let upstream_http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let upstream_token = write_token();
    let theirs = Target::borrow(&upstream_url, &upstream_http, &upstream_token);

    // Unique (key, version) per run so upstream's DB doesn't return
    // a pre-existing entry.
    let key = format!("upstream-diff-{}", std::process::id());
    let body = json!({"key": &key, "version": "upstream-diff-v1"});

    diff_post(
        "create_cache_entry",
        &ours,
        &theirs,
        &format!("{BASE_PATH}/CreateCacheEntry"),
        &body,
    )
    .await;
    diff_upload(&ours, &theirs, &body).await;
    diff_post(
        "finalize_cache_entry_upload",
        &ours,
        &theirs,
        &format!("{BASE_PATH}/FinalizeCacheEntryUpload"),
        &body,
    )
    .await;
    diff_post(
        "get_cache_entry_download_url",
        &ours,
        &theirs,
        &format!("{BASE_PATH}/GetCacheEntryDownloadURL"),
        &body,
    )
    .await;
    diff_download(&ours, &theirs, &body).await;
}

fn env_or_skip() -> Option<String> {
    let Ok(u) = std::env::var("UPSTREAM_URL") else {
        println!(
            "SKIP: UPSTREAM_URL is not set; nothing to diff against. \
             Start upstream with SKIP_TOKEN_VALIDATION=true and set UPSTREAM_URL."
        );
        return None;
    };
    Some(u.trim_end_matches('/').to_string())
}

// --- Target: a server the diff driver can talk to ----------------------

struct Target<'a> {
    base_url: &'a str,
    http: &'a Client,
    token: &'a str,
}

impl<'a> Target<'a> {
    const fn borrow(base_url: &'a str, http: &'a Client, token: &'a str) -> Self {
        Self {
            base_url,
            http,
            token,
        }
    }
}

// --- Low-level request helpers ----------------------------------------

async fn post_json(t: &Target<'_>, path: &str, body: &Value) -> Response {
    t.http
        .post(format!("{}{path}", t.base_url))
        .bearer_auth(t.token)
        .json(body)
        .send()
        .await
        .unwrap()
}

async fn put_bytes(t: &Target<'_>, path: &str, body: Vec<u8>) -> Response {
    t.http
        .put(format!("{}{path}", t.base_url))
        .body(body)
        .send()
        .await
        .unwrap()
}

async fn get(t: &Target<'_>, path: &str) -> Response {
    t.http
        .get(format!("{}{path}", t.base_url))
        .send()
        .await
        .unwrap()
}

// --- Diff driver -------------------------------------------------------

/// POSTs `body` to `path` on both targets, normalizes responses, and
/// asserts the normalized payloads are equal. Headers (beyond
/// `content-type`) are intentionally NOT diffed — h3 and axum legitimately
/// emit different casings / ordering / `content-length` framing.
async fn diff_post(label: &str, ours: &Target<'_>, theirs: &Target<'_>, path: &str, body: &Value) {
    let our_resp = post_json(ours, path, body).await;
    let their_resp = post_json(theirs, path, body).await;

    let (our_status, our_env) = capture_envelope(our_resp, &["content-type"], true).await;
    let (their_status, their_env) = capture_envelope(their_resp, &["content-type"], true).await;

    assert_eq!(
        our_status, their_status,
        "status mismatch for {label}: ours={our_status}, theirs={their_status}"
    );

    let ours_norm = normalize_envelope(&our_env, ours.base_url);
    let theirs_norm = normalize_envelope(&their_env, theirs.base_url);
    compare_to_golden_value(&theirs_norm, &ours_norm, label);
}

/// Drives PUT upload against both targets. Upload IDs differ per server,
/// so each is resolved via `CreateCacheEntry` (upstream may dedupe
/// in-flight uploads — we use the same body twice but the upload id
/// persists between `CreateCacheEntry` calls). Only status + body
/// emptiness are diffed; `x-ms-request-id` differs by design.
async fn diff_upload(ours: &Target<'_>, theirs: &Target<'_>, body: &Value) {
    let our_id = resolve_upload_id(ours, body).await;
    let their_id = resolve_upload_id(theirs, body).await;
    let query = format!("comp=block&blockid={}", blockid_48(0));

    let our_resp = put_bytes(
        ours,
        &format!("/devstoreaccount1/upload/{our_id}?{query}"),
        PAYLOAD.to_vec(),
    )
    .await;
    let their_resp = put_bytes(
        theirs,
        &format!("/devstoreaccount1/upload/{their_id}?{query}"),
        PAYLOAD.to_vec(),
    )
    .await;
    assert_eq!(our_resp.status(), their_resp.status());
    let our_empty = our_resp.bytes().await.unwrap().is_empty();
    let their_empty = their_resp.bytes().await.unwrap().is_empty();
    assert_eq!(our_empty, their_empty, "upload body emptiness diverges");
}

/// GETs `/download/{id}` on both and asserts bytes match `PAYLOAD`.
/// Headers aren't diffed — transfer-encoding / content-length differ
/// between Node's h3 and axum.
async fn diff_download(ours: &Target<'_>, theirs: &Target<'_>, body: &Value) {
    let our_cid = resolve_cache_entry_id(ours, body).await;
    let their_cid = resolve_cache_entry_id(theirs, body).await;

    let our_resp = get(ours, &format!("/download/{our_cid}")).await;
    let their_resp = get(theirs, &format!("/download/{their_cid}")).await;
    assert_eq!(our_resp.status(), 200);
    assert_eq!(their_resp.status(), 200);
    let ours_bytes = our_resp.bytes().await.unwrap();
    let their_bytes = their_resp.bytes().await.unwrap();
    assert_eq!(ours_bytes.as_ref(), PAYLOAD);
    assert_eq!(their_bytes.as_ref(), PAYLOAD);
}

// --- Body extraction helpers ------------------------------------------

async fn resolve_upload_id(t: &Target<'_>, body: &Value) -> i64 {
    let resp = post_json(t, &format!("{BASE_PATH}/CreateCacheEntry"), body).await;
    let json: Value = resp.json().await.unwrap();
    let url = json["signed_upload_url"].as_str().unwrap();
    let prefix = format!("{}/devstoreaccount1/upload/", t.base_url);
    url.strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("unexpected upload url: {url}"))
        .parse()
        .unwrap()
}

async fn resolve_cache_entry_id(t: &Target<'_>, body: &Value) -> String {
    let resp = post_json(t, &format!("{BASE_PATH}/GetCacheEntryDownloadURL"), body).await;
    let json: Value = resp.json().await.unwrap();
    let url = json["signed_download_url"].as_str().unwrap();
    let prefix = format!("{}/download/", t.base_url);
    url.strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("unexpected download url: {url}"))
        .to_string()
}

/// Normalizes the given envelope against its own target's dynamics.
/// Pulls `upload_id` / `cache_entry_id` out of the body before normalizing
/// so equal-shape responses from two servers with different ids
/// collapse to the same placeholder-ed JSON.
fn normalize_envelope(env: &Value, base_url: &str) -> Value {
    let mut d = Dynamics::none();
    if let Some(body) = env.get("body") {
        d.upload_id = opt_upload_id(body, base_url);
        d.cache_entry_id = opt_cache_entry_id(body, base_url);
    }
    normalize(env.clone(), base_url, &d)
}

fn opt_upload_id(body: &Value, base_url: &str) -> Option<i64> {
    let url = body.get("signed_upload_url")?.as_str()?;
    let prefix = format!("{base_url}/devstoreaccount1/upload/");
    url.strip_prefix(&prefix)?.parse().ok()
}

fn opt_cache_entry_id(body: &Value, base_url: &str) -> Option<String> {
    let url = body.get("signed_download_url")?.as_str()?;
    let prefix = format!("{base_url}/download/");
    Some(url.strip_prefix(&prefix)?.to_string())
}
