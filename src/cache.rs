//! Service-layer operations that span the DB and the storage adapter.
//!
//! Handlers stay thin by delegating multi-step dances (tx bounds,
//! cleanup on failure, ordering of DB vs blob mutations) here.
//! Current occupants:
//!
//! - `complete_upload` — validate-and-commit the parts of an upload
//!   into a durable cache entry.
//! - `probe_storage_for_entry` / `purge_broken_entry` — the
//!   storage-health probe and FK-cascade purge primitives used by the
//!   download path's purge-and-retry loop (#72).

use crate::db::Db;
use crate::db::entities::{CacheEntry, CacheEntryCoord, Upload};
use crate::db::id::new_uuid;
use crate::storage::{StorageAdapter, StorageError};

/// Errors returned by [`complete_upload`].
///
/// Validation-class variants leave the `uploads` row deleted — the
/// server has already decided the upload is doomed and further retries
/// against the same id are pointless. `Db` / `Storage` variants are
/// infrastructure failures and leave the DB in whatever state the tx /
/// query produced.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompleteUploadError {
    #[error("upload not found for the given coordinates")]
    UploadNotFound,

    #[error("no parts have been uploaded")]
    NoPartsUploaded,

    #[error("only {finished} of {started} parts uploaded")]
    PartsCountMismatch { started: i64, finished: i64 },

    #[error("uploaded part count {db} does not match disk count {disk}")]
    DiskCountMismatch { db: i64, disk: u64 },

    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Inputs to [`complete_upload`]. Bundled to keep the function at 3 args.
pub(crate) struct CompleteUploadParams<'a> {
    pub coord: CacheEntryCoord<'a>,
    pub now_ms: i64,
}

/// Validates a finished upload and commits it as a cache entry.
///
/// Ports upstream `lib/storage.ts#completeUpload` (lines 110-208):
///
/// 1. Look up the upload by `(key, version, scope, repo_id)`.
/// 2. Require `finished_part_upload_count > 0`.
/// 3. Require `started_part_upload_count == finished_part_upload_count`.
/// 4. Re-count files under `<folder>/parts/` on disk; require equality
///    with `finished_part_upload_count`.
/// 5. In one DB transaction: insert a fresh `storage_locations` row,
///    upsert the `cache_entries` row pointing at it, delete any
///    previous `storage_locations` row that was superseded, delete the
///    `uploads` row.
/// 6. Best-effort: delete the old folder from blob storage.
///
/// The three validation-class failures (2, 3, 4) delete the `uploads`
/// row before returning — the upload is unrecoverable and letting the
/// client retry against the same id would loop on the same error.
///
/// # Errors
/// Returns [`CompleteUploadError`]. See the variant docs for which
/// branch each represents.
pub(crate) async fn complete_upload(
    db: &dyn Db,
    adapter: &dyn StorageAdapter,
    params: CompleteUploadParams<'_>,
) -> Result<Upload, CompleteUploadError> {
    let upload = db
        .find_upload_by_coord(params.coord)
        .await?
        .ok_or(CompleteUploadError::UploadNotFound)?;

    validate_upload_counts(db, &upload).await?;
    validate_disk_parts(db, adapter, &upload).await?;

    let previous = commit_upload_tx(db, &upload, &params).await?;

    // Post-commit cleanup of the superseded blob folder. Failure here
    // does not un-do the commit — the row is already repointed — so we
    // log and continue. Upstream does the same (fire-and-forget
    // `adapter.deleteFolder` inside the tx callback, storage.ts:189).
    if let Some(prev) = previous
        && let Err(e) = adapter.delete_folder(&prev.folder_name).await
    {
        tracing::warn!(
            error = %e,
            folder = prev.folder_name,
            "failed to delete superseded upload folder; storage row already removed",
        );
    }

    Ok(upload)
}

async fn validate_upload_counts(db: &dyn Db, upload: &Upload) -> Result<(), CompleteUploadError> {
    if upload.finished_part_upload_count == 0 {
        db.delete_upload(upload.id).await?;
        return Err(CompleteUploadError::NoPartsUploaded);
    }
    if upload.started_part_upload_count != upload.finished_part_upload_count {
        db.delete_upload(upload.id).await?;
        return Err(CompleteUploadError::PartsCountMismatch {
            started: upload.started_part_upload_count,
            finished: upload.finished_part_upload_count,
        });
    }
    Ok(())
}

async fn validate_disk_parts(
    db: &dyn Db,
    adapter: &dyn StorageAdapter,
    upload: &Upload,
) -> Result<(), CompleteUploadError> {
    let parts_folder = format!("{}/parts", upload.folder_name);
    let disk_count = adapter.count_files_in_folder(&parts_folder).await?;
    if disk_count != u64::try_from(upload.finished_part_upload_count).unwrap_or(0) {
        db.delete_upload(upload.id).await?;
        return Err(CompleteUploadError::DiskCountMismatch {
            db: upload.finished_part_upload_count,
            disk: disk_count,
        });
    }
    Ok(())
}

/// Performs the commit transaction. Returns the previous
/// `storage_location` (if any) so the caller can delete the
/// corresponding blob folder post-commit.
async fn commit_upload_tx(
    db: &dyn Db,
    upload: &Upload,
    params: &CompleteUploadParams<'_>,
) -> Result<Option<crate::db::entities::PreviousLocation>, CompleteUploadError> {
    let mut tx = db.begin().await?;
    let new_location_id = new_uuid();
    tx.insert_storage_location(
        &new_location_id,
        &upload.folder_name,
        upload.finished_part_upload_count,
    )
    .await?;

    let previous = tx
        .upsert_cache_entry(params.coord, &new_location_id, params.now_ms)
        .await?;

    if let Some(prev) = &previous {
        // The cache_entries row was already repointed at new_location_id
        // by upsert_cache_entry, so ON DELETE CASCADE won't fire when we
        // drop the old storage_locations row.
        tx.delete_storage_location(&prev.id).await?;
    }

    tx.delete_upload(upload.id).await?;

    tx.commit().await?;
    Ok(previous)
}

/// Caps the storage-probe loop at three attempts per request. Mirrors
/// the issue spec ("never spins more than 3 storage probes per
/// request" — #72): three is enough for normal multi-scope walks while
/// still bounding work on a misconfigured backend.
pub(crate) const MAX_STORAGE_PROBES: u8 = 3;

/// Errors returned by [`probe_storage_for_entry`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProbeError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Returns `Ok(true)` when storage backing `entry` looks healthy
/// enough to hand the client a URL we expect to resolve, `Ok(false)`
/// when the blob is provably gone, `Err` on transport failures.
///
/// Probe shape:
/// - **Merged + `enable_direct_downloads = true`**: the caller is
///   about to return a presigned URL pointing at `<folder>/merged`.
///   Probe `count_files_in_folder(<folder>)`; if zero, the merged
///   blob (and any leftover parts) is gone.
/// - **Not-yet-merged**: the default URL routes through
///   `/download/<id>` which reads parts. Probe
///   `count_files_in_folder(<folder>/parts)`; if zero, parts are gone.
/// - **Merged + `enable_direct_downloads = false`**: the default
///   server-proxied URL has its own missing-blob recovery
///   (`src/routes/blob.rs` lazy-merge fallback for issue #17), so no
///   probe is needed here.
///
/// The narrow window "merged file deleted while parts/* still exist"
/// is not caught by `count_files_in_folder("<folder>") > 0`. That's
/// the lazy-merge recovery path's territory and is self-healing on
/// the next download.
///
/// # Errors
/// Returns [`ProbeError::Db`] when the location lookup fails (or the
/// FK invariant `cache_entries.locationId` is violated, which would
/// only happen on a corrupted DB), and [`ProbeError::Storage`] when
/// the storage adapter fails the count.
pub(crate) async fn probe_storage_for_entry(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    enable_direct_downloads: bool,
    entry: &CacheEntry,
) -> Result<bool, ProbeError> {
    let location = db
        .find_location_for_entry(&entry.id)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

    if enable_direct_downloads && location.merged_at.is_some() {
        let count = storage.count_files_in_folder(&location.folder_name).await?;
        return Ok(count > 0);
    }
    if location.merged_at.is_none() {
        let parts_folder = format!("{}/parts", location.folder_name);
        let count = storage.count_files_in_folder(&parts_folder).await?;
        return Ok(count > 0);
    }
    Ok(true)
}

/// Deletes the entry's `storage_locations` row, which CASCADE-deletes
/// the `cache_entries` row via the FK. The storage adapter is **not**
/// touched: this is called by the download path after
/// [`probe_storage_for_entry`] has already established the storage is
/// gone, so there's nothing to remove.
///
/// # Errors
/// Returns [`sqlx::Error`] on tx-begin / delete / commit failures.
pub(crate) async fn purge_broken_entry(db: &dyn Db, entry: &CacheEntry) -> Result<(), sqlx::Error> {
    let mut tx = db.begin().await?;
    tx.delete_storage_location(&entry.location_id).await?;
    tx.commit().await
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
