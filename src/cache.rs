//! Service-layer operations that span the DB and the storage adapter.
//!
//! Handlers stay thin by delegating multi-step dances (tx bounds,
//! cleanup on failure, ordering of DB vs blob mutations) here. First
//! occupant is [`complete_upload`] — the validate-and-commit step that
//! turns a finished upload into a durable cache entry. When a second
//! such operation lands, revisit whether these should become methods on
//! a dedicated `Cache` struct; for one function, free functions are
//! simpler.

use crate::db::Db;
use crate::db::entities::{CacheEntryCoord, Upload};
use crate::db::id::new_uuid;
use crate::db::tx::{insert_storage_location_tx, upsert_cache_entry_tx};
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
    db: &Db,
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

async fn validate_upload_counts(db: &Db, upload: &Upload) -> Result<(), CompleteUploadError> {
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
    db: &Db,
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
    db: &Db,
    upload: &Upload,
    params: &CompleteUploadParams<'_>,
) -> Result<Option<crate::db::entities::PreviousLocation>, CompleteUploadError> {
    let mut tx = db.begin().await?;
    let new_location_id = new_uuid();
    insert_storage_location_tx(
        &mut tx,
        &new_location_id,
        &upload.folder_name,
        upload.finished_part_upload_count,
    )
    .await?;

    let previous =
        upsert_cache_entry_tx(&mut tx, params.coord, &new_location_id, params.now_ms).await?;

    if let Some(prev) = &previous {
        // The cache_entries row was already repointed at new_location_id
        // by upsert_cache_entry_tx, so ON DELETE CASCADE won't fire when
        // we drop the old storage_locations row.
        crate::db::tx::delete_storage_location_tx(&mut tx, &prev.id).await?;
    }

    crate::db::tx::delete_upload_tx(&mut tx, upload.id).await?;

    tx.commit().await?;
    Ok(previous)
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
