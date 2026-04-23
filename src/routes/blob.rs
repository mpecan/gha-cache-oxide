//! Azure-blob-shaped upload (`PUT`) and direct download (`GET`) routes.
//!
//! These routes are served at the root (not under `/twirp/...`) and
//! intentionally skip the OIDC middleware: the `upload_id` in the upload
//! URL and the `cache_entry_id` in the download URL are capability tokens
//! minted by `CreateCacheEntry` / `GetCacheEntryDownloadURL` (both auth'd).
//!
//! Upstream references:
//! - `routes/devstoreaccount1/upload/[uploadId].put.ts`
//! - `routes/download/[cacheEntryId].ts`
//! - `lib/storage.ts#uploadPart` and `lib/storage.ts#download`
//!
//! # Deviations from upstream
//!
//! - Upstream `uploadPart` silently returns (leaving the handler to emit
//!   201) when the `uploads` row does not exist (`lib/storage.ts:84`).
//!   We return **404** — the `upload_id` was just minted by
//!   `CreateCacheEntry`, so a miss is unrecoverable and a silent success
//!   would violate the constitution's "never silently fall back or
//!   swallow errors" rule.
//! - Lazy merge (upstream's background part-concatenation task that
//!   writes a `/merged` blob during `download()`) is explicitly out of
//!   scope (issue #9 "Out of Scope: Lazy merge"). We stream parts
//!   0..`part_count` back-to-back on every download. The `merged_at` /
//!   `parts_deleted_at` columns are still observed: if a future change
//!   introduces merging we serve the merged blob, and if parts have
//!   been deleted post-merge we 404 rather than truncating mid-stream.

use std::io;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use base64::Engine;
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::db::id::now_ms;
use crate::state::AppState;
use crate::storage::{ByteStream, StorageAdapter, StorageError};

/// Builds the unauthenticated blob sub-router.
///
/// The caller is expected to `.merge(...)` this into the main router so
/// the routes live at the app root (`/devstoreaccount1/...`,
/// `/download/...`) — that's what `CreateCacheEntry` and
/// `GetCacheEntryDownloadURL` advertise in their `signed_*_url` fields.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/devstoreaccount1/upload/{upload_id}", put(upload_part))
        .route("/download/{cache_entry_id}", get(download))
}

#[derive(Debug, Deserialize)]
struct UploadQuery {
    #[serde(default)]
    comp: Option<String>,
    #[serde(default)]
    blockid: Option<String>,
}

async fn upload_part(
    State(state): State<AppState>,
    Path(upload_id): Path<i64>,
    Query(query): Query<UploadQuery>,
    body: Body,
) -> Response {
    // `?comp=blocklist` is the Azure-protocol "commit" step — upstream
    // treats it as a no-op (commit happens in FinalizeCacheEntryUpload)
    // and returns 201 with a request-id header for
    // tonistiigi/go-actions-cache compatibility.
    if query.comp.as_deref() == Some("blocklist") {
        return blob_accepted();
    }

    let chunk_index = match chunk_index_from_query(&query) {
        Ok(i) => i,
        Err(resp) => return resp,
    };

    let upload = match state.db.find_upload_by_id(upload_id).await {
        Ok(Some(u)) => u,
        Ok(None) => return not_found("Upload not found"),
        Err(e) => return internal_error(&e.to_string()),
    };

    // Increment started → stream body to storage → increment finished
    // mirrors `lib/storage.ts#uploadPart` (lines 86-106). The two
    // counters diverge briefly while the body is in flight; that's what
    // `complete_upload` checks on finalize (started == finished).
    if let Err(e) = state.db.increment_upload_started(upload_id).await {
        return internal_error(&e.to_string());
    }

    let object_name = format!("{}/parts/{}", upload.folder_name, chunk_index);
    let body_stream: ByteStream = body.into_data_stream().map_err(io::Error::other).boxed();
    if let Err(e) = state.storage.upload_stream(&object_name, body_stream).await {
        return storage_error_to_response(&e);
    }

    if let Err(e) = state
        .db
        .increment_upload_finished(upload_id, now_ms())
        .await
    {
        return internal_error(&e.to_string());
    }

    blob_accepted()
}

/// Resolves the chunk index from the query string, matching upstream
/// `routes/devstoreaccount1/upload/[uploadId].put.ts:29-36`: a missing
/// or empty `blockid` means "upload smaller than one chunk" (index 0),
/// anything present must decode to a valid index or we 400.
///
/// `Response` is large enough (~128 B) to trip `result_large_err`; the
/// caller consumes this in one place so the size is irrelevant —
/// boxing would trade a cheap move for a heap alloc.
#[allow(clippy::result_large_err)]
fn chunk_index_from_query(query: &UploadQuery) -> Result<u64, Response> {
    match query.blockid.as_deref() {
        None | Some("") => Ok(0),
        Some(b64) => {
            parse_chunk_index(b64).ok_or_else(|| bad_request(&format!("Invalid block id: {b64}")))
        }
    }
}

async fn download(State(state): State<AppState>, Path(cache_entry_id): Path<String>) -> Response {
    let location = match state.db.find_location_for_entry(&cache_entry_id).await {
        Ok(Some(l)) => l,
        Ok(None) => return not_found("Cache file not found"),
        Err(e) => return internal_error(&e.to_string()),
    };

    spawn_touch_last_downloaded(&state, &location.id);

    // Merged blob (only ever populated once #10+ introduces lazy merge)
    // takes precedence; parity with upstream `lib/storage.ts:227-228`.
    if location.merged_at.is_some() {
        let name = format!("{}/merged", location.folder_name);
        return match state.storage.download_stream(&name).await {
            Ok(stream) => octet_stream_response(stream),
            Err(StorageError::ObjectNotFound(_)) => not_found("Cache file not found"),
            Err(e) => internal_error(&e.to_string()),
        };
    }

    // Parts deleted but no merged blob: the cache is stale. Upstream
    // would race the merge here; we don't implement lazy merge so we
    // 404 instead of leaking a partial download.
    if location.parts_deleted_at.is_some() {
        return not_found("Cache file not found");
    }

    if let Err(resp) = ensure_parts_exist(
        state.storage.as_ref(),
        &location.folder_name,
        location.part_count,
    )
    .await
    {
        return resp;
    }
    stream_parts_response(
        state.storage.clone(),
        location.folder_name,
        location.part_count,
    )
}

/// Best-effort `UPDATE storage_locations SET lastDownloadedAt = ?`.
/// Mirrors upstream's fire-and-forget `void this.db.updateTable...`
/// (`lib/storage.ts:218-224`): a failure here is observability noise,
/// not a functional bug.
fn spawn_touch_last_downloaded(state: &AppState, location_id: &str) {
    let db = state.db.clone();
    let id = location_id.to_string();
    tokio::spawn(async move {
        if let Err(e) = db.touch_location_downloaded(&id, now_ms()).await {
            tracing::warn!(error = %e, location_id = id, "failed to touch lastDownloadedAt");
        }
    });
}

/// Verifies the parts folder contains at least `expected_part_count`
/// files. Mirrors upstream `ensurePartsExist` (lib/storage.ts:295-299).
/// A stale / incomplete cache returns 404 up-front rather than
/// truncating mid-stream.
#[allow(clippy::result_large_err)]
async fn ensure_parts_exist(
    adapter: &dyn StorageAdapter,
    folder_name: &str,
    expected_part_count: i64,
) -> Result<(), Response> {
    let parts_folder = format!("{folder_name}/parts");
    let actual = adapter
        .count_files_in_folder(&parts_folder)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;
    let expected = u64::try_from(expected_part_count).unwrap_or(0);
    if actual < expected {
        Err(not_found("Cache file not found"))
    } else {
        Ok(())
    }
}

/// Builds a streaming response that concatenates parts
/// `{folder_name}/parts/{0..part_count}`. Each part is fetched lazily
/// via `try_flatten`, so only one part's bytes are in flight at a
/// time — the body flows through chunk-by-chunk.
fn stream_parts_response(
    storage: Arc<dyn StorageAdapter>,
    folder_name: String,
    part_count: i64,
) -> Response {
    let indices = 0..u64::try_from(part_count).unwrap_or(0);
    let stream = futures::stream::iter(indices)
        .then(move |i| {
            let storage = storage.clone();
            let folder = folder_name.clone();
            async move {
                storage
                    .download_stream(&format!("{folder}/parts/{i}"))
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .try_flatten();
    octet_stream_response(stream.boxed())
}

fn octet_stream_response(stream: ByteStream) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
        .into_response()
}

/// 201 Created with `x-ms-request-id` — the response shape expected by
/// both `actions/cache` and `tonistiigi/go-actions-cache`. Upstream
/// `routes/devstoreaccount1/upload/[uploadId].put.ts:22-26,49-51`.
fn blob_accepted() -> Response {
    let request_id = Uuid::new_v4().to_string();
    (
        StatusCode::CREATED,
        [(
            header::HeaderName::from_static("x-ms-request-id"),
            request_id,
        )],
    )
        .into_response()
}

fn storage_error_to_response(e: &StorageError) -> Response {
    tracing::error!(error = %e, "storage adapter error in blob route");
    match e {
        StorageError::ObjectNotFound(_) => not_found("Cache file not found"),
        StorageError::InvalidObjectName { .. } => bad_request(&e.to_string()),
        StorageError::Backend(_) | StorageError::Io(_) => internal_error(&e.to_string()),
    }
}

/// Parses an Azure-blob blockid (base64 of a fixed-length buffer) into a
/// chunk index. Mirrors upstream `getChunkIndexFromBlockId` in
/// `routes/devstoreaccount1/upload/[uploadId].put.ts:54-70`.
///
/// - **64-byte buffer (docker buildx):** big-endian `u32` at offset 16.
/// - **48-byte buffer (actions/cache):** UTF-8; first 36 bytes are a
///   UUID, the remainder is a (zero-padded) decimal index.
///
/// Any other length, malformed base64, or a non-numeric tail yields
/// `None` — the caller responds 400 per upstream parity.
fn parse_chunk_index(b64: &str) -> Option<u64> {
    let decoded = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    match decoded.len() {
        64 => {
            let bytes: [u8; 4] = decoded.get(16..20)?.try_into().ok()?;
            Some(u64::from(u32::from_be_bytes(bytes)))
        }
        48 => {
            let s = std::str::from_utf8(&decoded).ok()?;
            // `trim` tolerates whitespace padding — upstream's
            // `Number.parseInt` does the same implicitly (stops at the
            // first non-digit), so zero-padded and space-padded tails
            // both resolve to the same index.
            s.get(36..)?.trim().parse::<u64>().ok()
        }
        _ => None,
    }
}

// --- JSON error helpers, mirroring routes::twirp `error_response` -------

fn error_response(status: StatusCode, message: &str) -> Response {
    let body = axum::Json(json!({
        "statusCode": status.as_u16(),
        "message": message,
    }));
    (status, body).into_response()
}

fn bad_request(msg: &str) -> Response {
    error_response(StatusCode::BAD_REQUEST, msg)
}

fn not_found(msg: &str) -> Response {
    error_response(StatusCode::NOT_FOUND, msg)
}

fn internal_error(msg: &str) -> Response {
    tracing::error!(message = msg, "blob route internal error");
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn blockid_48(index: u64) -> String {
        // UUID (36 chars) + zero-padded decimal index (12 chars) = 48 bytes.
        // Matches the layout actions/cache emits.
        let uuid = "11111111-2222-3333-4444-555555555555";
        let buf = format!("{uuid}{index:012}");
        assert_eq!(buf.len(), 48);
        base64::engine::general_purpose::STANDARD.encode(buf.as_bytes())
    }

    fn blockid_64(index: u32) -> String {
        // 64-byte buffer with a big-endian u32 at offset 16;
        // matches the layout docker buildx emits.
        let mut buf = [0u8; 64];
        buf[16..20].copy_from_slice(&index.to_be_bytes());
        base64::engine::general_purpose::STANDARD.encode(buf)
    }

    #[test]
    fn parse_48_byte_blockid_returns_decimal_index() {
        assert_eq!(parse_chunk_index(&blockid_48(0)), Some(0));
        assert_eq!(parse_chunk_index(&blockid_48(1)), Some(1));
        assert_eq!(parse_chunk_index(&blockid_48(42)), Some(42));
        assert_eq!(
            parse_chunk_index(&blockid_48(999_999_999_999)),
            Some(999_999_999_999)
        );
    }

    #[test]
    fn parse_64_byte_blockid_returns_big_endian_uint32() {
        assert_eq!(parse_chunk_index(&blockid_64(0)), Some(0));
        assert_eq!(parse_chunk_index(&blockid_64(1)), Some(1));
        assert_eq!(
            parse_chunk_index(&blockid_64(u32::MAX)),
            Some(u64::from(u32::MAX))
        );
    }

    #[test]
    fn parse_rejects_invalid_base64() {
        assert!(parse_chunk_index("%%% not base64 %%%").is_none());
    }

    #[test]
    fn parse_rejects_wrong_length() {
        // 32 bytes — decodes cleanly but is neither 48 nor 64.
        let b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        assert!(parse_chunk_index(&b64).is_none());
    }

    #[test]
    fn parse_rejects_non_numeric_tail() {
        let uuid = "11111111-2222-3333-4444-555555555555";
        let buf = format!("{uuid}not-a-num---");
        assert_eq!(buf.len(), 48);
        let b64 = base64::engine::general_purpose::STANDARD.encode(buf.as_bytes());
        assert!(parse_chunk_index(&b64).is_none());
    }

    #[test]
    fn parse_tolerates_whitespace_padding() {
        // Upstream's Number.parseInt stops at the first non-digit, so
        // space-padded tails resolve to the same index as zero-padded.
        let uuid = "11111111-2222-3333-4444-555555555555";
        let buf = format!("{uuid}5           ");
        assert_eq!(buf.len(), 48);
        let b64 = base64::engine::general_purpose::STANDARD.encode(buf.as_bytes());
        assert_eq!(parse_chunk_index(&b64), Some(5));
    }
}
