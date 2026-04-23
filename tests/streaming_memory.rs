//! Bounded-memory proof for the upload streaming path.
//!
//! Issue #9's acceptance criteria require the handler not to buffer the
//! full request body in memory. We prove that with a `GlobalAlloc`
//! wrapper that tracks both currently-live bytes and the peak value
//! that counter reaches, then measure the peak delta across a 100 MiB
//! upload: if axum materialised the body before calling our handler
//! (or if `object_store::BufWriter` collected everything), the peak
//! would jump by ≥ 100 MiB; when the body actually streams the peak
//! stays bounded by the flush/chunk buffer size (~10 MiB).
//!
//! We track **peak live bytes**, not cumulative bytes allocated — the
//! latter is the body size even under perfect streaming because each
//! chunk's alloc+dealloc both count. Only a simultaneously-held 100
//! MiB buffer shows up as a peak spike.
//!
//! The allocator wrapper is process-wide, so this test lives in its
//! own binary — no parallel tests mean the counter belongs to this
//! test alone. The simpler 100 MiB round-trip (no memory assertion)
//! stays in `tests/blob.rs` so it runs on every `cargo test` even if
//! someone later weakens or disables the allocator wrapper.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod twirp_common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use bytes::Bytes;
use futures::stream;
use serde_json::json;
use tower::ServiceExt;

use twirp_common::{BASE_PATH, body_json, harness, post, write_token};

// --- Peak-tracking global allocator --------------------------------------

struct CountingAlloc;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarding to the system allocator with the same layout.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            // Raise the peak if we just exceeded it. Relaxed ordering
            // is fine — we only care about the max value observed.
            let mut peak = PEAK_BYTES.load(Ordering::Relaxed);
            while live > peak {
                match PEAK_BYTES.compare_exchange_weak(
                    peak,
                    live,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(observed) => peak = observed,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: forwarding to the system allocator with the same layout
        // that was passed to `alloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// Resets `PEAK_BYTES` to the current live count so subsequent
/// allocations can be measured against a fresh baseline.
fn reset_peak_to_current() {
    PEAK_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
}

fn peak_bytes() -> usize {
    PEAK_BYTES.load(Ordering::Relaxed)
}

// --- Helpers (kept local so this binary has no dependency on tests/blob.rs) -

fn blockid_48(index: u64) -> String {
    let uuid = "11111111-2222-3333-4444-555555555555";
    let buf = format!("{uuid}{index:012}");
    assert_eq!(buf.len(), 48);
    base64::engine::general_purpose::STANDARD.encode(buf.as_bytes())
}

// --- Test ----------------------------------------------------------------

#[tokio::test]
async fn upload_of_100_mib_does_not_buffer_the_body_in_memory() {
    let h = harness().await;
    let token = write_token();

    // Reserve an upload slot so the PUT has a valid target.
    let req = post(
        &format!("{BASE_PATH}/CreateCacheEntry"),
        Some(&token),
        &json!({"key":"build-cache","version":"v1"}),
    );
    let (_, body) = body_json(h.router.clone().oneshot(req).await.unwrap()).await;
    let url = body["signed_upload_url"].as_str().unwrap().to_string();
    let upload_id: i64 = url
        .strip_prefix("http://localhost:3000/devstoreaccount1/upload/")
        .unwrap()
        .parse()
        .unwrap();

    // 100 × 1 MiB frames. `Bytes::clone` shares the underlying Arc, so
    // the source side only allocates 1 MiB + small overheads regardless
    // of the frame count — meaning any large allocation growth we
    // observe has to come from the server-side handler pipeline.
    let upload_size: usize = 100 * 1024 * 1024;
    let chunk = Bytes::from(vec![0x42u8; 1024 * 1024]);
    let frames: Vec<Result<Bytes, std::io::Error>> = (0..100).map(|_| Ok(chunk.clone())).collect();
    let upload_body = Body::from_stream(stream::iter(frames));

    let query = format!("comp=block&blockid={}", blockid_48(0));
    let put_req = Request::builder()
        .method("PUT")
        .uri(format!("/devstoreaccount1/upload/{upload_id}?{query}"))
        .body(upload_body)
        .unwrap();

    // Snapshot live bytes and reset the peak immediately before the
    // upload so harness setup cost (SQLite migrations, router build,
    // JWT mint, CreateCacheEntry round-trip) stays out of the peak.
    let baseline_live = live_bytes();
    reset_peak_to_current();

    let resp = h.router.clone().oneshot(put_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let peak_delta = peak_bytes().saturating_sub(baseline_live);
    // If any layer (axum, `object_store::BufWriter`, our handler)
    // holds the whole body simultaneously, `peak_delta` jumps to ≥
    // 100 MiB. Streaming keeps it bounded by BufWriter's 10 MiB flush
    // threshold plus transient per-chunk buffers and tokio/tracing
    // overhead. Threshold tuned loose to survive background task
    // churn without false-failing; a real buffering regression blows
    // through it by ~4×.
    let limit = upload_size / 4;
    assert!(
        peak_delta < limit,
        "upload appears to buffer: peak live memory grew {peak_delta} bytes during a \
         {upload_size}-byte upload (limit: {limit} bytes)"
    );

    // Sanity: the part actually made it to disk.
    let part_path = h
        .tmp
        .path()
        .join(upload_id.to_string())
        .join("parts")
        .join("0");
    let meta = tokio::fs::metadata(&part_path).await.unwrap();
    assert_eq!(meta.len(), upload_size as u64);
}
