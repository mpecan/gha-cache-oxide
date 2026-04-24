//! Lazy-merge orchestration.
//!
//! On the first download of a cache entry, stream parts concatenated
//! to the client while teeing the same bytes into a background
//! `upload_stream` that writes a single `merged` blob. Subsequent
//! downloads serve the merged blob directly (and can be presigned for
//! direct download — issue #16).
//!
//! # Port of upstream
//!
//! Ports `lib/storage.ts#download` (lines 210-298) in upstream. One
//! documented divergence: the claim on `mergeStartedAt` is a
//! compare-and-swap rather than a read-then-write, so two concurrent
//! first-downloads produce **exactly one** merge (issue #15 AC). The
//! CAS loser falls back to streaming parts directly, matching
//! upstream's `downloadFromCacheEntryLocation` branch when
//! `mergeStartedAt` is set.
//!
//! # Topology
//!
//! On a cold download:
//!
//! ```text
//!   storage parts ─► pump task ─┬─► response mpsc ─► HTTP body
//!                                └─► merger mpsc   ─► upload_stream → merged blob
//! ```
//!
//! Two `mpsc` channels with bounded capacity back-pressure the pump so
//! it never materialises more than a couple of chunks in memory. The
//! pump tolerates a dropped response (client disconnect) and keeps
//! feeding the merger so the merged blob still lands — upstream's
//! `pumpPartsToStreams` has the same behaviour.
//!
//! The merger task is tracked by [`MergeTracker`] (a thin wrapper over
//! `tokio_util::task::TaskTracker`) so the server's graceful shutdown
//! can await in-flight merges — port of upstream's
//! `waitForOngoingMerges` call in `plugins/setup.ts`.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::TaskTracker;

use crate::db::Db;
use crate::db::entities::StorageLocation;
use crate::db::id::now_ms;
use crate::storage::{ByteStream, StorageAdapter, StorageError};

/// Bounded mpsc capacity for the tee channels. Each chunk is a `Bytes`
/// handle (shared buffer, cheap clone) so the real memory footprint is
/// roughly `CHAN_CAP × chunk_size`. 16 × ~64 KiB = ~1 MiB per tee,
/// matching the bounded-memory contract asserted by
/// `tests/streaming_memory.rs`.
const CHAN_CAP: usize = 16;

/// Handle that keeps track of in-flight lazy-merge tasks.
///
/// `AppState` holds one; the server's graceful-shutdown path calls
/// [`MergeTracker::shutdown`] after the listener stops accepting
/// connections so the merged blob is written even when the process
/// is on its way down. Wraps `tokio_util::task::TaskTracker` —
/// cheap to clone (internal `Arc`).
#[derive(Clone, Default)]
pub struct MergeTracker {
    inner: TaskTracker,
}

impl MergeTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawns `future` onto the tokio runtime and records it for
    /// shutdown tracking. Returns the spawned handle so callers can
    /// still `.await` it if they want.
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.spawn(future)
    }

    /// Closes the tracker (stops accepting new spawns) and awaits every
    /// task that was already in flight. Called by the server's
    /// graceful-shutdown path.
    ///
    /// Safe to call once; a second call is a no-op because the tracker
    /// is already closed.
    pub async fn shutdown(&self) {
        self.inner.close();
        self.inner.wait().await;
    }

    /// Test-only: count of tasks still in flight. Useful for asserting
    /// that a shutdown call actually drained something.
    #[cfg(test)]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Test-only: `true` when no tasks are tracked. Paired with
    /// [`len`](Self::len) so clippy's `len_without_is_empty` stays
    /// quiet.
    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Outcome of [`start_lazy_merge`] — either we claimed the merge and
/// hand back the teed response stream, or we lost the CAS race and the
/// caller should fall back to streaming parts directly.
pub(crate) enum LazyMergeOutcome {
    /// CAS won — caller writes this stream as the HTTP response body.
    /// The merger is running in the background, tracked by the
    /// `MergeTracker`.
    Claimed(ByteStream),
    /// CAS lost. Another request is already merging this location;
    /// the caller should stream parts directly (or the merged blob, if
    /// re-reading the location shows `merged_at` is set).
    LostRace,
}

/// Starts a lazy merge for `location`. On CAS success, spawns the tee
/// pump and the merger task and returns a response stream; on CAS
/// failure, returns [`LazyMergeOutcome::LostRace`].
///
/// Preconditions: caller has already verified `location.merged_at` and
/// `location.parts_deleted_at` are both `None` (the
/// download-handler-level branches for those cases are upstream of
/// this call).
///
/// # Errors
/// Returns [`StorageError`] if the DB update or the initial part-count
/// check fails. Errors from the pump or the merger surface through the
/// returned stream (response) and through `reset_merge_flags` (merger),
/// not here.
pub(crate) async fn start_lazy_merge(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    tracker: &MergeTracker,
    location: StorageLocation,
) -> Result<LazyMergeOutcome, StorageError> {
    let claimed = db
        .try_mark_merge_started(&location.id, now_ms())
        .await
        .map_err(|e| StorageError::Io(io::Error::other(e.to_string())))?;
    if !claimed {
        return Ok(LazyMergeOutcome::LostRace);
    }

    // `part_count` is `NOT NULL` in the schema and written via `u64`
    // on insert; a negative value would be a schema invariant
    // violation. Surface it loudly rather than truncating the merge
    // to an empty blob.
    let part_count = u64::try_from(location.part_count).map_err(|_| {
        StorageError::Io(io::Error::other(format!(
            "negative partCount ({}) on storage_location {}",
            location.part_count, location.id
        )))
    })?;

    let (resp_tx, resp_rx) = mpsc::channel::<Result<Bytes, io::Error>>(CHAN_CAP);
    let (merge_tx, merge_rx) = mpsc::channel::<Result<Bytes, io::Error>>(CHAN_CAP);

    let merged_name = format!("{}/merged", location.folder_name);
    let location_id = location.id.clone();
    let folder_name = location.folder_name.clone();

    // Merger task: upload the merged blob, then finalize DB state (or
    // reset flags on failure). Tracked by the MergeTracker so graceful
    // shutdown awaits it.
    let merger_ctx = MergerCtx {
        db: db.clone(),
        storage: storage.clone(),
        location_id,
        folder_name,
        merged_name,
    };
    tracker.spawn(async move { run_merger(merger_ctx, merge_rx).await });

    // Pump task: tee each part's bytes into both mpsc channels. Runs
    // to completion under the merger's supervision — if the merger
    // dies, the pump's send to `merge_tx` fails and it exits.
    let pump_storage = storage.clone();
    let pump_folder = location.folder_name.clone();
    tracker.spawn(async move {
        pump(pump_storage, pump_folder, part_count, resp_tx, merge_tx).await;
    });

    Ok(LazyMergeOutcome::Claimed(
        ReceiverStream::new(resp_rx).boxed(),
    ))
}

/// Streams parts 0..`part_count` concurrently into the response and
/// merger channels. Tolerates a dropped response consumer (client
/// disconnect) and keeps feeding the merger.
async fn pump(
    storage: Arc<dyn StorageAdapter>,
    folder: String,
    part_count: u64,
    resp_tx: mpsc::Sender<Result<Bytes, io::Error>>,
    merge_tx: mpsc::Sender<Result<Bytes, io::Error>>,
) {
    let mut response_alive = true;
    for i in 0..part_count {
        let object_name = format!("{folder}/parts/{i}");
        let mut stream = match storage.download_stream(&object_name).await {
            Ok(s) => s,
            Err(e) => {
                // Downstream never sees these bytes, but merge_tx's Err
                // triggers the merger's reset_merge_flags path.
                let _ = merge_tx.send(Err(io::Error::other(e))).await;
                return;
            }
        };
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    let _ = merge_tx.send(Err(e)).await;
                    return;
                }
            };
            if response_alive && resp_tx.send(Ok(chunk.clone())).await.is_err() {
                // Client went away. Stop feeding response; keep pumping
                // the merger so the merged blob still lands — upstream
                // parity (`pumpPartsToStreams` lines 323-335).
                response_alive = false;
            }
            if merge_tx.send(Ok(chunk)).await.is_err() {
                return;
            }
        }
    }
}

/// Bundle of what the merger task needs. Grouped to keep
/// [`run_merger`]'s arg count under the 5-arg clippy threshold.
struct MergerCtx {
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    /// `storage_locations.id` that this merge is finalising.
    location_id: String,
    /// The cache entry's folder (`<upload_id>`). Parts live under
    /// `<folder_name>/parts/`; the merged blob lands at
    /// `<folder_name>/merged`.
    folder_name: String,
    /// Pre-formatted `<folder_name>/merged` so the merger doesn't
    /// redo the format.
    merged_name: String,
}

/// Runs the background merger: uploads the teed byte stream as
/// `{folder}/merged`; on success marks `merged_at`, then (in a single
/// tx) marks `parts_deleted_at` and deletes the parts folder; on
/// failure resets `merge_started_at`/`merged_at` so the next download
/// retries.
async fn run_merger(ctx: MergerCtx, merge_rx: mpsc::Receiver<Result<Bytes, io::Error>>) {
    let stream: ByteStream = ReceiverStream::new(merge_rx).boxed();
    match ctx.storage.upload_stream(&ctx.merged_name, stream).await {
        Ok(()) => {
            if let Err(e) = finalize_merge(
                ctx.db.as_ref(),
                ctx.storage.as_ref(),
                &ctx.location_id,
                &ctx.folder_name,
            )
            .await
            {
                tracing::error!(
                    error = %e,
                    location_id = %ctx.location_id,
                    "lazy-merge finalise failed after successful upload",
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                location_id = %ctx.location_id,
                "lazy-merge upload failed; resetting merge flags so a later request can retry",
            );
            if let Err(reset_err) = ctx.db.reset_merge_flags(&ctx.location_id).await {
                tracing::error!(
                    error = %reset_err,
                    location_id = %ctx.location_id,
                    "reset_merge_flags failed after upload error",
                );
            }
        }
    }
}

/// Marks the merge as complete in the DB, then in a single tx marks
/// parts as deleted and removes the parts folder. Mirrors upstream's
/// `storage.ts:246-263`. The folder delete and the `partsDeletedAt`
/// update are atomic via tx rollback on delete failure — so the
/// invariant "`parts_deleted_at` set ⇒ parts gone" holds.
async fn finalize_merge(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    location_id: &str,
    folder_name: &str,
) -> Result<(), FinalizeError> {
    db.mark_merged(location_id, now_ms()).await?;

    let mut tx = db.begin().await?;
    tx.mark_parts_deleted(location_id, now_ms()).await?;
    let parts_folder = format!("{folder_name}/parts");
    if let Err(e) = storage.delete_folder(&parts_folder).await {
        // Roll back the `parts_deleted_at` update so a retry sees an
        // accurate snapshot; propagate the storage error to the caller.
        let _ = tx.rollback().await;
        return Err(FinalizeError::Storage(e));
    }
    tx.commit().await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum FinalizeError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
mod tests;
