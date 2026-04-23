//! Driver-agnostic conformance suite for [`StorageAdapter`].
//!
//! Extracts the trait-level contract from the filesystem-specific tests so
//! every future driver (S3, GCS, Azure) plugs in by supplying a `setup`
//! function. Each scenario lives in [`scenarios`] as a `pub async fn`, and
//! the [`storage_conformance_cases!`] macro expands to one `#[tokio::test]`
//! per scenario inside a named sub-module — failures identify the exact
//! scenario (`filesystem::round_trip_small_payload`) rather than a single
//! opaque "conformance" test.
//!
//! # Shape
//!
//! * [`Harness`] — bundles the adapter with capability flags (currently
//!   `signs_urls`) so scenarios branch on what the backend can do rather
//!   than what it is.
//! * [`SetupResult`] — tuple returned by each driver's setup function:
//!   `(adapter, RAII guard, signs_urls)`. The guard is `Box<dyn Send>` so
//!   the macro works uniformly — filesystem passes a `TempDir`, S3 will
//!   pass a bucket-scoped clean-up handle.
//! * [`run_conformance_suite`] — programmatic sequential runner for
//!   one-shot uses (future parity sweep against a live bucket); clears
//!   between scenarios to mimic fresh-setup semantics.
//!
//! # Adding a scenario
//!
//! One place, three lines: add a `pub async fn` under [`scenarios`], add
//! the matching line to the macro body, and add the matching line to
//! [`run_conformance_suite`]. No driver file is touched.

// Scenarios panic on failure by design — that IS the test assertion path.
// Silencing the pedantic `missing_panics_doc` keeps them free of boilerplate
// `# Panics` sections that would only repeat the obvious.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::too_long_first_doc_paragraph
)]

use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, stream};
use gha_cache_oxide::storage::{ByteStream, FilesystemAdapter, StorageAdapter, StorageError};
use tempfile::TempDir;

// ------------------------------------------------------------------------
// Harness + setup plumbing
// ------------------------------------------------------------------------

/// Bundles the adapter with capability flags the scenarios branch on.
pub struct Harness {
    pub adapter: Arc<dyn StorageAdapter>,
    /// `true` for backends that issue presigned download URLs (S3, GCS);
    /// `false` for filesystem. `signed_url_matches_capability` asserts
    /// `Some(_)` vs `None` accordingly.
    pub signs_urls: bool,
}

/// Tuple returned by a driver setup function: adapter, RAII guard
/// (erased), `signs_urls` capability flag. The erased guard keeps the
/// macro driver-agnostic — `Box<dyn Send>` holds a `TempDir` today and
/// whatever S3 needs tomorrow without changing the macro body.
pub type SetupResult = (Arc<dyn StorageAdapter>, Box<dyn Send>, bool);

async fn filesystem_setup() -> SetupResult {
    let tmp = TempDir::new().unwrap();
    let adapter: Arc<dyn StorageAdapter> = Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
    (adapter, Box::new(tmp), false)
}

// ------------------------------------------------------------------------
// Shared byte-stream helpers
// ------------------------------------------------------------------------

fn bytes_stream(data: Vec<u8>) -> ByteStream {
    stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(data))]).boxed()
}

async fn collect(mut s: ByteStream) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = s.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

// ------------------------------------------------------------------------
// Scenarios — each tests one trait-level contract.
// ------------------------------------------------------------------------

pub mod scenarios {
    use super::{Harness, StorageError, bytes_stream, collect};

    pub async fn round_trip_zero_bytes(h: &Harness) {
        h.adapter
            .upload_stream("empty.bin", bytes_stream(vec![]))
            .await
            .unwrap();
        let got = collect(h.adapter.download_stream("empty.bin").await.unwrap()).await;
        assert!(got.is_empty(), "zero-byte round-trip yielded {got:?}");
    }

    pub async fn round_trip_small_payload(h: &Harness) {
        let payload = b"hello, world".to_vec();
        h.adapter
            .upload_stream("folder/file.bin", bytes_stream(payload.clone()))
            .await
            .unwrap();
        let got = collect(h.adapter.download_stream("folder/file.bin").await.unwrap()).await;
        assert_eq!(got, payload);
    }

    /// 12 MiB — beats `object_store::BufWriter`'s 10 MiB default flush
    /// threshold with margin, so the writer must flush mid-stream and
    /// reassemble on read. Non-uniform payload so chunk reordering would
    /// surface as a byte-level mismatch rather than a false pass.
    pub async fn round_trip_crosses_buffer_flush(h: &Harness) {
        let payload: Vec<u8> = (0..12 * 1024 * 1024)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        h.adapter
            .upload_stream("big.bin", bytes_stream(payload.clone()))
            .await
            .unwrap();
        let got = collect(h.adapter.download_stream("big.bin").await.unwrap()).await;
        assert_eq!(got.len(), payload.len());
        assert_eq!(got, payload, "byte-exactness across buffer flush");
    }

    pub async fn download_missing_returns_object_not_found(h: &Harness) {
        match h.adapter.download_stream("does/not/exist").await {
            Err(StorageError::ObjectNotFound(ref s)) if s == "does/not/exist" => {}
            Err(other) => panic!("expected ObjectNotFound, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    pub async fn upload_rejects_directory_traversal(h: &Harness) {
        let err = h
            .adapter
            .upload_stream("../etc/passwd", bytes_stream(b"bad".to_vec()))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::InvalidObjectName { .. }),
            "expected InvalidObjectName, got {err:?}"
        );
    }

    pub async fn download_rejects_absolute_path(h: &Harness) {
        match h.adapter.download_stream("/etc/passwd").await {
            Err(StorageError::InvalidObjectName { .. }) => {}
            Err(other) => panic!("expected InvalidObjectName, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    pub async fn count_files_in_missing_folder_returns_zero(h: &Harness) {
        assert_eq!(
            h.adapter
                .count_files_in_folder("no-such-dir")
                .await
                .unwrap(),
            0
        );
    }

    pub async fn count_files_counts_uploaded_files(h: &Harness) {
        for i in 0u8..3 {
            h.adapter
                .upload_stream(&format!("parts/{i}"), bytes_stream(vec![i]))
                .await
                .unwrap();
        }
        h.adapter
            .upload_stream("other/z", bytes_stream(vec![0]))
            .await
            .unwrap();

        assert_eq!(h.adapter.count_files_in_folder("parts").await.unwrap(), 3);
        assert_eq!(h.adapter.count_files_in_folder("other").await.unwrap(), 1);
    }

    pub async fn delete_folder_removes_all_children(h: &Harness) {
        for i in 0u8..3 {
            h.adapter
                .upload_stream(&format!("target/{i}"), bytes_stream(vec![i]))
                .await
                .unwrap();
        }
        h.adapter
            .upload_stream("sibling/x", bytes_stream(vec![0]))
            .await
            .unwrap();

        h.adapter.delete_folder("target").await.unwrap();

        assert_eq!(h.adapter.count_files_in_folder("target").await.unwrap(), 0);
        assert_eq!(
            h.adapter.count_files_in_folder("sibling").await.unwrap(),
            1,
            "delete_folder(\"target\") must not touch 'sibling'"
        );
    }

    pub async fn delete_missing_folder_is_noop(h: &Harness) {
        h.adapter.delete_folder("never-created").await.unwrap();
    }

    pub async fn delete_folder_is_recursive(h: &Harness) {
        h.adapter
            .upload_stream("root/a", bytes_stream(vec![1]))
            .await
            .unwrap();
        h.adapter
            .upload_stream("root/nested/b", bytes_stream(vec![2]))
            .await
            .unwrap();
        h.adapter
            .upload_stream("root/nested/deep/c", bytes_stream(vec![3]))
            .await
            .unwrap();

        h.adapter.delete_folder("root").await.unwrap();

        assert_eq!(h.adapter.count_files_in_folder("root").await.unwrap(), 0);
    }

    pub async fn clear_removes_everything(h: &Harness) {
        h.adapter
            .upload_stream("a/1", bytes_stream(vec![1]))
            .await
            .unwrap();
        h.adapter
            .upload_stream("b/2", bytes_stream(vec![2]))
            .await
            .unwrap();

        h.adapter.clear().await.unwrap();

        assert_eq!(h.adapter.count_files_in_folder("a").await.unwrap(), 0);
        assert_eq!(h.adapter.count_files_in_folder("b").await.unwrap(), 0);
    }

    pub async fn upload_overwrites_existing_object(h: &Harness) {
        h.adapter
            .upload_stream("obj", bytes_stream(b"first".to_vec()))
            .await
            .unwrap();
        h.adapter
            .upload_stream("obj", bytes_stream(b"second".to_vec()))
            .await
            .unwrap();
        let got = collect(h.adapter.download_stream("obj").await.unwrap()).await;
        assert_eq!(&got, b"second");
    }

    /// Pins the upstream invariant that `parts` and `parts-foo` are
    /// disjoint top-level folders — a future backend whose `list` is
    /// byte-prefix matching (rather than segment-aware) would silently
    /// leak siblings here.
    pub async fn prefix_matching_is_segment_aware(h: &Harness) {
        h.adapter
            .upload_stream("parts/a", bytes_stream(vec![1]))
            .await
            .unwrap();
        h.adapter
            .upload_stream("parts/b", bytes_stream(vec![2]))
            .await
            .unwrap();
        h.adapter
            .upload_stream("parts-foo/x", bytes_stream(vec![3]))
            .await
            .unwrap();

        assert_eq!(
            h.adapter.count_files_in_folder("parts").await.unwrap(),
            2,
            "count should match only 'parts', not 'parts-foo'"
        );

        h.adapter.delete_folder("parts").await.unwrap();
        assert_eq!(h.adapter.count_files_in_folder("parts").await.unwrap(), 0);
        assert_eq!(
            h.adapter.count_files_in_folder("parts-foo").await.unwrap(),
            1,
            "delete_folder(\"parts\") must not touch 'parts-foo'"
        );
    }

    /// `signed_url` must be `None` for backends that cannot sign (the
    /// server proxies the download) and `Some(_)` for backends that can
    /// (the client goes direct). `Harness::signs_urls` says which.
    pub async fn signed_url_matches_capability(h: &Harness) {
        h.adapter
            .upload_stream("obj", bytes_stream(b"content".to_vec()))
            .await
            .unwrap();
        let got = h.adapter.signed_url("obj").await.unwrap();
        if h.signs_urls {
            assert!(
                got.is_some(),
                "signs_urls=true backend must return Some(url)"
            );
        } else {
            assert!(
                got.is_none(),
                "signs_urls=false backend must return None, got {got:?}"
            );
        }
    }

    pub async fn signed_url_validates_object_name(h: &Harness) {
        match h.adapter.signed_url("../evil").await {
            Err(StorageError::InvalidObjectName { .. }) => {}
            Err(other) => panic!("expected InvalidObjectName, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    /// Each trait method routes its name through `validate_object_name`
    /// before touching the backend; empty names must round-trip the
    /// dedicated `InvalidObjectName { reason: "empty" }` variant. Pins
    /// the invariant at the trait boundary so a future driver can't
    /// short-circuit validation.
    ///
    /// `download_stream` is matched (not `unwrap_err`'d) because
    /// `ByteStream` does not implement `Debug`.
    pub async fn rejects_empty_object_name(h: &Harness) {
        fn assert_empty(err: &StorageError, call: &str) {
            match err {
                StorageError::InvalidObjectName {
                    reason: "empty", ..
                } => {}
                other => panic!("{call}: expected InvalidObjectName(empty), got {other:?}"),
            }
        }

        let upload_err = h
            .adapter
            .upload_stream("", bytes_stream(b"x".to_vec()))
            .await
            .unwrap_err();
        assert_empty(&upload_err, "upload_stream");

        match h.adapter.download_stream("").await {
            Err(e) => assert_empty(&e, "download_stream"),
            Ok(_) => panic!("download_stream(\"\") should error"),
        }

        assert_empty(
            &h.adapter.delete_folder("").await.unwrap_err(),
            "delete_folder",
        );
        assert_empty(
            &h.adapter.count_files_in_folder("").await.unwrap_err(),
            "count_files_in_folder",
        );
        assert_empty(&h.adapter.signed_url("").await.unwrap_err(), "signed_url");
    }
}

// ------------------------------------------------------------------------
// Programmatic runner
// ------------------------------------------------------------------------

/// Sequential runner — exercises every scenario against the same adapter,
/// clearing between scenarios to mimic fresh-setup semantics. Useful for
/// one-shot sweeps (e.g. a future CI job that boots a real bucket once
/// and runs the whole suite before tearing it down).
///
/// # Panics
/// Any failing scenario panics with the scenario-level assertion — the
/// function intentionally does not catch, so the caller sees the same
/// error it would from `cargo test`.
pub async fn run_conformance_suite(adapter: Arc<dyn StorageAdapter>, signs_urls: bool) {
    let h = Harness {
        adapter,
        signs_urls,
    };

    macro_rules! run {
        ($name:ident) => {{
            h.adapter.clear().await.unwrap();
            scenarios::$name(&h).await;
        }};
    }

    run!(round_trip_zero_bytes);
    run!(round_trip_small_payload);
    run!(round_trip_crosses_buffer_flush);
    run!(download_missing_returns_object_not_found);
    run!(upload_rejects_directory_traversal);
    run!(download_rejects_absolute_path);
    run!(count_files_in_missing_folder_returns_zero);
    run!(count_files_counts_uploaded_files);
    run!(delete_folder_removes_all_children);
    run!(delete_missing_folder_is_noop);
    run!(delete_folder_is_recursive);
    run!(clear_removes_everything);
    run!(upload_overwrites_existing_object);
    run!(prefix_matching_is_segment_aware);
    run!(signed_url_matches_capability);
    run!(signed_url_validates_object_name);
    run!(rejects_empty_object_name);
}

// ------------------------------------------------------------------------
// storage_conformance_cases! — generates one named #[tokio::test] per
// scenario inside a sub-module. Invoking twice (e.g. `filesystem` + `s3`)
// keeps both test sets live without collision.
//
// The scenario list is passed as a trailing token sequence so the same
// macro expansion drives every driver; the repetition group (`$(...)+`)
// avoids nesting a helper macro, which would lose access to `$setup`.
//
// Note for future drivers: setup is infallible here — filesystem only
// needs `TempDir::new()`. An S3 setup that needs live creds should
// panic on failure under `#[ignore]` (run with `cargo test -- --ignored`,
// per CLAUDE.md), matching the pattern the constitution already uses
// for tests that require external services.
// ------------------------------------------------------------------------

macro_rules! storage_conformance_cases {
    ($mod_name:ident, $setup:ident, $($scenario:ident),+ $(,)?) => {
        mod $mod_name {
            use super::{Harness, scenarios, $setup};
            $(
                #[tokio::test]
                async fn $scenario() {
                    let (adapter, _guard, signs_urls) = $setup().await;
                    let h = Harness { adapter, signs_urls };
                    scenarios::$scenario(&h).await;
                }
            )+
        }
    };
}

storage_conformance_cases!(
    filesystem,
    filesystem_setup,
    round_trip_zero_bytes,
    round_trip_small_payload,
    round_trip_crosses_buffer_flush,
    download_missing_returns_object_not_found,
    upload_rejects_directory_traversal,
    download_rejects_absolute_path,
    count_files_in_missing_folder_returns_zero,
    count_files_counts_uploaded_files,
    delete_folder_removes_all_children,
    delete_missing_folder_is_noop,
    delete_folder_is_recursive,
    clear_removes_everything,
    upload_overwrites_existing_object,
    prefix_matching_is_segment_aware,
    signed_url_matches_capability,
    signed_url_validates_object_name,
    rejects_empty_object_name,
);

// ------------------------------------------------------------------------
// Smoke test the programmatic runner alongside the macro-generated ones
// so both entry points are exercised on every `cargo test`.
// ------------------------------------------------------------------------

#[tokio::test]
async fn runner_executes_full_suite_against_filesystem() {
    let (adapter, _guard, signs_urls) = filesystem_setup().await;
    run_conformance_suite(adapter, signs_urls).await;
}
