//! The six v1 handlers. Each ports the same-named function in act's
//! `artifactcache/handler.go`; see the module docs in `mod.rs` for the
//! model mapping and the deliberate deviations.

use std::io;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::json;

use super::auth::ForgejoRun;
use super::{BASE_PATH, json_empty, json_error};
use crate::cache::chunked::{ChunkedCommitError, chunk_object_name, complete_chunked_upload};
use crate::cache::{MAX_STORAGE_PROBES, probe_storage_for_entry, purge_broken_entry};
use crate::db::entities::{CacheEntry, CacheEntryCoord, MatchRequest, NewUpload, Upload};
use crate::db::id::{new_upload_id, new_uuid, now_ms};
use crate::metrics::Metrics;
use crate::routes::blob;
use crate::state::AppState;
use crate::storage::ByteStream;

#[derive(Debug, Deserialize)]
pub(super) struct FindQuery {
    #[serde(default)]
    keys: String,
    #[serde(default)]
    version: String,
}

/// act's `Request`: missing fields decode to zero values in Go, so
/// they default here too.
#[derive(Debug, Deserialize)]
pub(super) struct ReserveBody {
    #[serde(default)]
    key: String,
    #[serde(default)]
    version: String,
}

#[derive(Debug, Deserialize)]
struct CommitBody {
    #[serde(default)]
    size: Option<i64>,
}

// -- GET /cache ------------------------------------------------------------

/// Looks up `keys` (comma-separated, lowercased) at `version`: first in
/// the caller's write-isolation scope, then — if that scope is
/// non-empty — in the shared `""` scope. Each scope walk is exact then
/// prefix, per key in order, newest first.
pub(super) async fn find(
    State(state): State<AppState>,
    Extension(run): Extension<ForgejoRun>,
    Query(q): Query<FindQuery>,
) -> Response {
    let keys: Vec<String> = q
        .keys
        .split(',')
        .filter(|k| !k.is_empty())
        .map(str::to_lowercase)
        .collect();
    let Some((primary, restore)) = keys.split_first() else {
        return miss(&state);
    };
    let restore: Vec<&str> = restore.iter().map(String::as_str).collect();

    let mut scopes = vec![run.write_isolation_key.as_str()];
    if !run.write_isolation_key.is_empty() {
        scopes.push("");
    }
    for scope in scopes {
        let req = MatchRequest {
            primary_key: primary,
            restore_keys: &restore,
            version: &q.version,
            scopes: &[scope],
            repo_id: &run.repo_id,
        };
        match match_with_healthy_storage(&state, req).await {
            Ok(Some(entry)) => return hit(&state, &run, &entry),
            Ok(None) => {}
            Err(resp) => return resp,
        }
    }
    miss(&state)
}

fn hit(state: &AppState, run: &ForgejoRun, entry: &CacheEntry) -> Response {
    state.metrics.forgejo.find_hits.inc();
    let archive_location = format!(
        "{}/{}{BASE_PATH}/artifacts/{}",
        run.proxy_host, run.run_id, entry.id
    );
    Json(json!({
        "result": "hit",
        "archiveLocation": archive_location,
        "cacheKey": entry.key,
    }))
    .into_response()
}

fn miss(state: &AppState) -> Response {
    state.metrics.forgejo.find_misses.inc();
    StatusCode::NO_CONTENT.into_response()
}

/// `match_cache_entry` plus act's "blob gone → drop the entry" check
/// (`handler.go#find`). Like the v2 download path (#72) it then retries
/// the next-best candidate instead of reporting a miss straight away,
/// capped at [`MAX_STORAGE_PROBES`].
async fn match_with_healthy_storage(
    state: &AppState,
    req: MatchRequest<'_>,
) -> Result<Option<CacheEntry>, Response> {
    for _ in 0..MAX_STORAGE_PROBES {
        let entry = match state.db.match_cache_entry(req).await {
            Ok(Some(m)) => m.entry,
            Ok(None) => return Ok(None),
            Err(e) => return Err(internal(&e)),
        };
        // act's `exist()` stats the blob for every hit, merged or not.
        // `probe_storage_for_entry`'s "direct downloads" mode is the one
        // that also checks merged locations, so ask for it.
        match probe_storage_for_entry(&*state.db, state.storage.as_ref(), true, &entry).await {
            Ok(true) => return Ok(Some(entry)),
            Ok(false) => {
                tracing::info!(entry_id = %entry.id, "forgejo find: storage gone; purging entry");
                purge_broken_entry(&*state.db, &entry)
                    .await
                    .map_err(|e| internal(&e))?;
            }
            Err(e) => return Err(internal(&e)),
        }
    }
    Ok(None)
}

// -- POST /caches ----------------------------------------------------------

/// Reserves an upload. The body is decoded regardless of
/// `Content-Type`, like act's `json.NewDecoder`.
pub(super) async fn reserve(
    State(state): State<AppState>,
    Extension(run): Extension<ForgejoRun>,
    body: Bytes,
) -> Response {
    let body: ReserveBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let key = body.key.to_lowercase();
    let id = nonzero_upload_id();
    // A UUID rather than the id: ids are drawn from 10^10 values, and a
    // collision with a live entry's folder would let chunks land in (and
    // commit copy over) that entry.
    let folder = new_uuid();
    let upload = NewUpload {
        id,
        coord: CacheEntryCoord {
            key: &key,
            version: &body.version,
            scope: &run.write_isolation_key,
            repo_id: &run.repo_id,
        },
        folder_name: &folder,
        created_at_ms: now_ms(),
    };
    match state.db.create_upload(upload).await {
        Ok(u) => Json(json!({ "cacheId": u.id })).into_response(),
        Err(e) => internal(&e),
    }
}

/// `@actions/cache` treats a falsy `cacheId` as a failed reserve, so 0
/// (a 1-in-10^10 draw from `new_upload_id`) is never handed out.
fn nonzero_upload_id() -> i64 {
    loop {
        let id = new_upload_id();
        if id != 0 {
            return id;
        }
    }
}

// -- PATCH /caches/:id -----------------------------------------------------

/// Streams one `Content-Range` chunk to its own object. Chunks may
/// arrive in parallel and in any order; nothing is buffered.
pub(super) async fn upload(
    State(state): State<AppState>,
    Extension(run): Extension<ForgejoRun>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let upload = match load_upload(&state, &run, &id).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    let range = headers
        .get("content-range")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let Some((start, _end)) = parse_content_range(range) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            &format!("cache parseContentRange({range}): invalid"),
        );
    };

    let f = &state.metrics.forgejo;
    // Touch before streaming so `cleanup:uploads` (60 s staleness)
    // never reaps an upload whose first chunk is still in flight.
    match state.db.touch_upload(upload.id, now_ms()).await {
        Ok(true) => {}
        Ok(false) => return not_reserved(upload.id),
        Err(e) => {
            f.upload_errors.inc();
            return internal(&e);
        }
    }
    let object = chunk_object_name(&upload.folder_name, start);
    let stream = counted(body, Arc::clone(&state.metrics), |m, n| {
        m.forgejo.upload_bytes.add(n);
    });
    if let Err(e) = state.storage.upload_stream(&object, stream).await {
        f.upload_errors.inc();
        return internal(&e);
    }
    match state.db.touch_upload(upload.id, now_ms()).await {
        Ok(true) => json_empty(),
        Ok(false) => late_chunk(&state, &upload).await,
        Err(e) => {
            f.upload_errors.inc();
            internal(&e)
        }
    }
}

/// The upload was committed or reaped while this chunk streamed. Its
/// `chunks/` folder is garbage either way (a commit already copied what
/// it needed into `parts/`), so drop it rather than leak an object no
/// row points at, and tell the client the reservation is gone.
async fn late_chunk(state: &AppState, upload: &Upload) -> Response {
    let chunks = format!("{}/chunks", upload.folder_name);
    if let Err(e) = state.storage.delete_folder(&chunks).await {
        tracing::warn!(error = %e, folder = chunks, "failed to drop late chunk");
    }
    not_reserved(upload.id)
}

/// Parses act's supported `Content-Range` shape, `bytes <start>-<end>/*`
/// (the total may be anything, and is ignored).
pub(super) fn parse_content_range(s: &str) -> Option<(u64, u64)> {
    let s = s.strip_prefix("bytes ").unwrap_or(s);
    let range = s.split_once('/').map_or(s, |(r, _)| r);
    let (a, b) = range.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

// -- POST /caches/:id ------------------------------------------------------

/// Commits the upload.
///
/// The declared `size` is read from the commit body, where every v1
/// client sends it (`@actions/cache` since the v1 toolkit; old clients
/// such as `actions/cache@v2` send it *only* there). act instead checks
/// reserve's `cacheSize`, and skips the check when that is 0 — i.e. for
/// exactly those old clients. A missing or negative commit size skips
/// the check here; see the deviation list in `mod.rs`.
pub(super) async fn commit(
    State(state): State<AppState>,
    Extension(run): Extension<ForgejoRun>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let upload = match load_upload(&state, &run, &id).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    let expected_size = if body.is_empty() {
        None
    } else {
        match serde_json::from_slice::<CommitBody>(&body) {
            Ok(b) => b.size.and_then(|s| u64::try_from(s).ok()),
            Err(e) => return json_error(StatusCode::BAD_REQUEST, &e.to_string()),
        }
    };

    let f = &state.metrics.forgejo;
    let result = complete_chunked_upload(
        &*state.db,
        state.storage.as_ref(),
        &upload,
        expected_size,
        now_ms(),
    )
    .await;
    match result {
        Ok(size) => {
            f.commits.inc();
            tracing::info!(
                upload_id = upload.id,
                key = upload.key,
                size,
                "forgejo cache committed"
            );
            json_empty()
        }
        Err(ChunkedCommitError::UploadGone) => not_reserved(upload.id),
        Err(e) if e.is_client_error() => {
            f.commit_errors.inc();
            tracing::warn!(upload_id = upload.id, error = %e, "forgejo commit rejected");
            json_error(
                StatusCode::BAD_REQUEST,
                &format!("commit({}): {e}", upload.id),
            )
        }
        Err(e) => {
            f.commit_errors.inc();
            internal(&e)
        }
    }
}

/// act's `readCache` + write-isolation check for `caches/:id`.
/// A malformed id is 400, an unknown (or foreign-repo) id 404, and an
/// isolation-key mismatch 403.
async fn load_upload(state: &AppState, run: &ForgejoRun, id: &str) -> Result<Upload, Response> {
    let Ok(id) = id.parse::<i64>() else {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            &format!("invalid cache id {id:?}"),
        ));
    };
    let upload = match state.db.find_upload_by_id(id).await {
        Ok(Some(u)) if u.repo_id == run.repo_id => u,
        Ok(_) => return Err(not_reserved(id)),
        Err(e) => return Err(internal(&e)),
    };
    if upload.scope != run.write_isolation_key {
        return Err(isolation_mismatch(run, &upload.scope));
    }
    Ok(upload)
}

// -- GET /artifacts/:id ----------------------------------------------------

/// Serves a committed entry. Reads are allowed against the caller's
/// isolation key or the shared `""` scope; the body is streamed by the
/// same code path as the v2 `/download/:id` route (lazy merge included).
pub(super) async fn get_artifact(
    State(state): State<AppState>,
    Extension(run): Extension<ForgejoRun>,
    Path(id): Path<String>,
) -> Response {
    let entry = match state.db.find_cache_entry_by_id(&id).await {
        Ok(Some(e)) if e.repo_id == run.repo_id => e,
        Ok(_) => return not_reserved(&id),
        Err(e) => return internal(&e),
    };
    if !entry.scope.is_empty() && entry.scope != run.write_isolation_key {
        return isolation_mismatch(&run, &entry.scope);
    }

    let metrics = Arc::clone(&state.metrics);
    let resp = blob::download(State(state), Path(entry.id)).await;
    if !resp.status().is_success() {
        return resp;
    }
    let (parts, body) = resp.into_parts();
    let counted = counted(body, metrics, |m, n| m.forgejo.download_bytes.add(n));
    Response::from_parts(parts, Body::from_stream(counted))
}

// -- POST /clean -----------------------------------------------------------

/// act acknowledges and does nothing (force-deleting caches is not
/// supported over this API); so do we.
pub(super) async fn clean() -> Response {
    json_empty()
}

// -- helpers ---------------------------------------------------------------

/// Wraps `body` in a stream that reports every chunk's length to
/// `record` as it passes through.
fn counted(body: Body, metrics: Arc<Metrics>, record: fn(&Metrics, u64)) -> ByteStream {
    body.into_data_stream()
        .inspect_ok(move |chunk| record(&metrics, chunk.len() as u64))
        .map_err(io::Error::other)
        .boxed()
}

fn not_reserved(id: impl std::fmt::Display) -> Response {
    json_error(StatusCode::NOT_FOUND, &format!("cache {id}: not reserved"))
}

fn isolation_mismatch(run: &ForgejoRun, entry_scope: &str) -> Response {
    json_error(
        StatusCode::FORBIDDEN,
        &format!(
            "cache authorized for write isolation {:?}, but attempting to operate on {entry_scope:?}",
            run.write_isolation_key
        ),
    )
}

fn internal(e: &dyn std::fmt::Display) -> Response {
    tracing::error!(error = %e, "forgejo cache: internal error");
    json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

#[cfg(test)]
mod tests {
    use super::parse_content_range;

    #[test]
    fn parses_act_content_range_shapes() {
        assert_eq!(parse_content_range("bytes 0-99/*"), Some((0, 99)));
        assert_eq!(parse_content_range("bytes 100-199/200"), Some((100, 199)));
        assert_eq!(parse_content_range("bytes 5-9"), Some((5, 9)));
    }

    #[test]
    fn rejects_malformed_content_range() {
        for s in [
            "",
            "bytes",
            "bytes -1/*",
            "bytes a-b/*",
            "bytes 0-/*",
            "bytes 1/*",
        ] {
            assert_eq!(parse_content_range(s), None, "{s:?}");
        }
    }
}
