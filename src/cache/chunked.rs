//! Commit path for byte-range ("chunked") uploads — the Forgejo runner
//! v1 cache dialect (`PATCH /_apis/artifactcache/caches/:id` with
//! `Content-Range`).
//!
//! Each `PATCH` streams its body to `<folder>/chunks/<start>`, where
//! `<start>` is the chunk's byte offset as 16 lowercase hex digits
//! (the widest `u64`), so a name-sorted listing is offset-ordered —
//! the same naming act's `Storage.tempName` uses. Chunks can therefore
//! arrive in parallel and out of order with no coordination.
//!
//! On commit we list the chunks, prove they tile `[0, size)` exactly,
//! server-side copy them into the `<folder>/parts/<i>` layout every
//! other code path (lazy merge, parts streaming, cleanup) already
//! understands, and then run the same commit transaction as the v2
//! path. Unlike act — which concatenates whatever it finds and only
//! checks the total — gaps and overlaps are rejected, since either
//! would silently corrupt the archive.

use futures::{StreamExt, TryStreamExt};

use super::{commit_upload_tx, delete_superseded_folder};
use crate::db::Db;
use crate::db::entities::{CacheEntryCoord, Upload};
use crate::storage::{ObjectInfo, StorageAdapter, StorageError};

/// Hex width of a chunk object name. 16 digits cover all of `u64`, so
/// lexicographic order equals numeric order.
const CHUNK_NAME_WIDTH: usize = 16;

/// Server-side copies in flight per commit. ~500 MB caches in 32 MiB
/// chunks are ~16 copies; 8 keeps commit latency low without
/// hammering the backend.
const COPY_CONCURRENCY: usize = 8;

/// Object name for the chunk starting at `start` inside `folder`.
pub fn chunk_object_name(folder: &str, start: u64) -> String {
    format!("{folder}/chunks/{start:0CHUNK_NAME_WIDTH$x}")
}

/// Errors returned by [`complete_chunked_upload`]. The layout variants
/// are client-caused and leave the upload (row + blobs) deleted.
#[derive(Debug, thiserror::Error)]
pub enum ChunkedCommitError {
    #[error("no chunks have been uploaded")]
    NoChunks,

    #[error("unexpected object {0:?} in chunk folder")]
    BadChunkName(String),

    #[error("chunk layout broken at offset {expected}: next chunk starts at {found}")]
    Discontiguous { expected: u64, found: u64 },

    #[error("uploaded {actual} bytes but commit declared {expected}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

impl ChunkedCommitError {
    /// True for failures caused by what the client uploaded (as
    /// opposed to infrastructure errors).
    pub const fn is_client_error(&self) -> bool {
        matches!(
            self,
            Self::NoChunks
                | Self::BadChunkName(_)
                | Self::Discontiguous { .. }
                | Self::SizeMismatch { .. }
        )
    }
}

/// Checks that name-sorted `chunks` tile `[0, total)` with no gaps or
/// overlaps, and that `total == expected_size` when a size is given.
/// Returns `total`.
///
/// # Errors
/// Returns the layout-class [`ChunkedCommitError`] variant that
/// describes the first problem found.
pub fn validate_chunk_layout(
    chunks: &[ObjectInfo],
    expected_size: Option<u64>,
) -> Result<u64, ChunkedCommitError> {
    if chunks.is_empty() {
        return Err(ChunkedCommitError::NoChunks);
    }
    let mut offset: u64 = 0;
    for chunk in chunks {
        let start = parse_chunk_name(&chunk.name)
            .ok_or_else(|| ChunkedCommitError::BadChunkName(chunk.name.clone()))?;
        if start != offset {
            return Err(ChunkedCommitError::Discontiguous {
                expected: offset,
                found: start,
            });
        }
        offset = offset.saturating_add(chunk.size);
    }
    match expected_size {
        Some(expected) if expected != offset => Err(ChunkedCommitError::SizeMismatch {
            expected,
            actual: offset,
        }),
        _ => Ok(offset),
    }
}

fn parse_chunk_name(name: &str) -> Option<u64> {
    if name.len() != CHUNK_NAME_WIDTH {
        return None;
    }
    u64::from_str_radix(name, 16).ok()
}

/// Validates the uploaded chunks of `upload`, rewrites them into the
/// `parts/<i>` layout, and commits the upload as a cache entry.
/// Returns the committed size in bytes.
///
/// `expected_size` is the size the client declared on commit; `None`
/// skips the size check (act does the same for old clients that don't
/// send one).
///
/// # Errors
/// Layout failures delete the upload row and its folder before
/// returning; `Db` / `Storage` errors leave state as-is for the
/// stale-upload cleanup task.
pub async fn complete_chunked_upload(
    db: &dyn Db,
    adapter: &dyn StorageAdapter,
    upload: &Upload,
    expected_size: Option<u64>,
    now_ms: i64,
) -> Result<u64, ChunkedCommitError> {
    let chunks_folder = format!("{}/chunks", upload.folder_name);
    let chunks = adapter.list_folder(&chunks_folder).await?;
    let total = match validate_chunk_layout(&chunks, expected_size) {
        Ok(total) => total,
        Err(e) => {
            discard_upload(db, adapter, upload).await?;
            return Err(e);
        }
    };

    copy_chunks_to_parts(adapter, &upload.folder_name, &chunks).await?;

    let coord = CacheEntryCoord {
        key: &upload.key,
        version: &upload.version,
        scope: &upload.scope,
        repo_id: &upload.repo_id,
    };
    let part_count = i64::try_from(chunks.len()).unwrap_or(i64::MAX);
    let previous = commit_upload_tx(db, upload, coord, part_count, now_ms).await?;
    delete_superseded_folder(adapter, previous).await;

    // The chunk objects are now duplicates of parts/*. Leaving them is
    // harmless (the whole folder goes when the location is reaped), so
    // a failure here is logged, not surfaced.
    if let Err(e) = adapter.delete_folder(&chunks_folder).await {
        tracing::warn!(error = %e, folder = chunks_folder, "failed to delete committed chunks");
    }
    Ok(total)
}

async fn copy_chunks_to_parts(
    adapter: &dyn StorageAdapter,
    folder: &str,
    chunks: &[ObjectInfo],
) -> Result<(), StorageError> {
    // Owned pairs: a closure borrowing `chunks` trips the higher-ranked
    // lifetime check on the `Send` future the handler must return.
    let pairs: Vec<(String, String)> = chunks
        .iter()
        .enumerate()
        .map(|(i, c)| {
            (
                format!("{folder}/chunks/{}", c.name),
                format!("{folder}/parts/{i}"),
            )
        })
        .collect();
    futures::stream::iter(pairs)
        .map(|(from, to)| async move { adapter.copy(&from, &to).await })
        .buffer_unordered(COPY_CONCURRENCY)
        .try_collect::<()>()
        .await
}

async fn discard_upload(
    db: &dyn Db,
    adapter: &dyn StorageAdapter,
    upload: &Upload,
) -> Result<(), sqlx::Error> {
    db.delete_upload(upload.id).await?;
    if let Err(e) = adapter.delete_folder(&upload.folder_name).await {
        tracing::warn!(
            error = %e,
            folder = upload.folder_name,
            "failed to delete rejected chunked upload; cleanup:uploads will not retry it",
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn chunk(start: u64, size: u64) -> ObjectInfo {
        ObjectInfo {
            name: format!("{start:016x}"),
            size,
        }
    }

    #[test]
    fn chunk_names_are_fixed_width_hex() {
        assert_eq!(chunk_object_name("f", 0), "f/chunks/0000000000000000");
        assert_eq!(chunk_object_name("f", 255), "f/chunks/00000000000000ff");
        assert_eq!(
            chunk_object_name("f", u64::MAX),
            "f/chunks/ffffffffffffffff"
        );
        // Lexicographic order must equal numeric order.
        assert!(chunk_object_name("f", 9) < chunk_object_name("f", 10));
    }

    #[test]
    fn contiguous_chunks_validate_to_total() {
        let chunks = [chunk(0, 10), chunk(10, 10), chunk(20, 5)];
        assert_eq!(validate_chunk_layout(&chunks, Some(25)).unwrap(), 25);
        assert_eq!(validate_chunk_layout(&chunks, None).unwrap(), 25);
    }

    #[test]
    fn empty_is_rejected() {
        assert!(matches!(
            validate_chunk_layout(&[], None),
            Err(ChunkedCommitError::NoChunks)
        ));
    }

    #[test]
    fn gap_is_rejected() {
        let chunks = [chunk(0, 10), chunk(11, 10)];
        assert!(matches!(
            validate_chunk_layout(&chunks, None),
            Err(ChunkedCommitError::Discontiguous {
                expected: 10,
                found: 11
            })
        ));
    }

    #[test]
    fn missing_first_chunk_is_rejected() {
        let chunks = [chunk(10, 10)];
        assert!(matches!(
            validate_chunk_layout(&chunks, None),
            Err(ChunkedCommitError::Discontiguous {
                expected: 0,
                found: 10
            })
        ));
    }

    #[test]
    fn overlap_is_rejected() {
        let chunks = [chunk(0, 10), chunk(5, 10)];
        assert!(matches!(
            validate_chunk_layout(&chunks, None),
            Err(ChunkedCommitError::Discontiguous { .. })
        ));
    }

    #[test]
    fn size_mismatch_is_rejected() {
        let chunks = [chunk(0, 10)];
        assert!(matches!(
            validate_chunk_layout(&chunks, Some(11)),
            Err(ChunkedCommitError::SizeMismatch {
                expected: 11,
                actual: 10
            })
        ));
    }

    #[test]
    fn foreign_object_name_is_rejected() {
        let chunks = [ObjectInfo {
            name: "stray".into(),
            size: 1,
        }];
        assert!(matches!(
            validate_chunk_layout(&chunks, None),
            Err(ChunkedCommitError::BadChunkName(_))
        ));
    }
}
