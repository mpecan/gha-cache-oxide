//! GCS-adapter-specific behaviour that falls outside the driver-agnostic
//! conformance suite — everything in here is driver-specific contract,
//! not a trait-level invariant.
//!
//! All tests in this binary require a live GCS-compatible endpoint
//! (`fake-gcs-server` in CI, optionally a real bucket in a parity sweep).
//! They are `#[ignore]`'d so a default `cargo test` never touches the
//! network.
//!
//! ```sh
//! GCS_TEST_ENDPOINT=http://localhost:4443 \
//! GCS_TEST_SA_KEY=/tmp/sa.json \
//!   cargo test --test gcs_adapter -- --ignored
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc
)]

use std::path::PathBuf;

use gha_cache_oxide::storage::{GcsAdapter, GcsConfig, StorageError};
use url::Url;

/// Acceptance criterion #1 on issue #68: "boots successfully with valid
/// creds; surfaces a clear startup error otherwise". Mirrors the parallel
/// S3 scenario at `tests/s3_adapter.rs:39-78`. We point the adapter at a
/// guaranteed-nonexistent bucket and assert:
///
/// 1. `GcsAdapter::new` returns `Err`, not a panic.
/// 2. The error is the [`StorageError::BucketUnavailable`] variant,
///    carrying the bucket name we passed in so operators don't have to
///    grep the underlying `object_store` error.
#[tokio::test]
#[ignore = "requires GCS_TEST_ENDPOINT + a running fake-gcs-server; `cargo test --test gcs_adapter -- --ignored`"]
async fn gcs_missing_bucket_produces_clear_error() {
    let endpoint = std::env::var("GCS_TEST_ENDPOINT").expect(
        "GCS_TEST_ENDPOINT must be set; start fake-gcs-server and re-run `cargo test -- --ignored`",
    );
    let sa_key = std::env::var("GCS_TEST_SA_KEY").ok().map(PathBuf::from);

    // Randomised bucket name so a real bucket in the test setup can't
    // accidentally shadow this — the assertion is that construction
    // fails loud, not that it succeeds.
    let bucket = format!("gha-cache-missing-{}", uuid::Uuid::new_v4());

    let err = GcsAdapter::new(GcsConfig {
        bucket: bucket.clone(),
        service_account_key: sa_key,
        endpoint: Some(Url::parse(&endpoint).unwrap()),
        key_prefix: None,
    })
    .await
    .unwrap_err();

    // Variant-match only — same rationale as the S3 sibling test:
    // different fake-/real-GCS implementations phrase 404s differently,
    // and the variant itself plus the bucket field is what operators
    // actually need.
    match err {
        StorageError::BucketUnavailable { bucket: b, .. } => {
            assert_eq!(b, bucket, "error should carry the bucket name we passed");
        }
        other => panic!("expected StorageError::BucketUnavailable, got {other:?}"),
    }
}
