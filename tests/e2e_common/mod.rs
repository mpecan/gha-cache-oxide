//! Shared harness for `tests/e2e.rs` and `tests/upstream_diff.rs`.
//!
//! Split into three focused submodules:
//! - [`harness`] — server boot (random port, graceful shutdown),
//!   `reqwest` request helpers, JWT minting, `blockid_48`.
//! - [`normalize`] — envelope capture, placeholder substitution for
//!   dynamic values (`upload_id`, cache-entry UUID, `x-ms-request-id`,
//!   base URL), and small URL-parsing extractors.
//! - [`golden`] — `assert_golden` + `write_or_compare_golden` driver
//!   with `UPDATE_GOLDEN=1` regen support, and `compare_to_golden_value`.
//!
//! # Golden files
//!
//! Run the test binary once with `UPDATE_GOLDEN=1` to regenerate
//! `tests/golden/*.json` when a response shape intentionally changes.
//! Without the flag, `assert_golden` diffs actual vs stored and panics
//! on mismatch with a pretty line-oriented diff.
//!
//! # Why not share with `twirp_common/`
//!
//! The sibling `tests/twirp_common/mod.rs` is built around
//! `tower::ServiceExt::oneshot` — it never binds a socket, and Rust
//! integration tests each compile as their own binary, so sharing
//! across `tests/*_common/mod.rs` directories is awkward. Duplicating
//! the RSA-key fixture + JWT helpers is confined to test code and
//! keeps each harness self-contained.

// `unused_imports` on the re-exports is expected: each integration
// test binary (tests/e2e.rs vs tests/upstream_diff.rs) uses a different
// subset of the facade, and Rust checks lints per-binary.
#![allow(
    dead_code,
    unused_imports,
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used
)]

pub mod golden;
pub mod harness;
pub mod normalize;

pub use golden::{assert_golden, compare_to_golden_value, write_or_compare_golden};
pub use harness::{
    ServerHandle, blockid_48, get, mint_token, post_json, put_bytes, spawn_server, write_token,
};
pub use normalize::{
    Dynamics, capture_envelope, extract_cache_entry_id, extract_upload_id, normalize,
};

pub const ISSUER: &str = "https://token.actions.githubusercontent.com";
pub const BASE_PATH: &str = "/twirp/github.actions.results.api.v1.CacheService";
