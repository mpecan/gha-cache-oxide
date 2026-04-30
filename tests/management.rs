//! Integration tests for the management REST API (issues #19, #70).
//!
//! Drives the full `build_app` router through `tower::ServiceExt::oneshot`.
//! Test cases are split across topical submodules under
//! `tests/management/` so each file stays under the 700-line hard
//! limit; the `common` submodule provides the shared `Harness`,
//! request builder, and seed helpers.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

#[path = "management/common.rs"]
mod common;

#[path = "management/auth_gate.rs"]
mod auth_gate;
#[path = "management/cache_entries.rs"]
mod cache_entries;
#[path = "management/cleanup.rs"]
mod cleanup;
#[path = "management/delete_many.rs"]
mod delete_many;
#[path = "management/match_endpoint.rs"]
mod match_endpoint;
#[path = "management/storage_locations.rs"]
mod storage_locations;
