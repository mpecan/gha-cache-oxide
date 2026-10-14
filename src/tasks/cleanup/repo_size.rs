//! `cleanup:repo-size` — caps each repository's cache at
//! `CACHE_MAX_REPO_SIZE_GB`, the way GitHub's hosted cache caps a
//! repository at 10 GB: when a repository's entries add up to more, its
//! least recently used entries are deleted until it fits. Not in
//! upstream.
//!
//! Sizes are not known at commit time, so each pass first measures the
//! locations that have none yet (`sizeBytes IS NULL`) from storage and
//! records them; a location is measured once. Then, per repository over
//! the cap, it deletes locations (entries cascade) oldest-used first,
//! where "used" is the later of the entry's last save and the
//! location's last download.
//!
//! A single entry larger than the cap is deleted too: the cap is a
//! limit on the repository, not on what is worth keeping.

use super::entries::PAGE_SIZE;
use crate::db::Db;
use crate::db::entities::{RepoUsage, StorageLocation};
use crate::storage::{ObjectInfo, StorageAdapter, StorageError};

const GIB: i64 = 1024 * 1024 * 1024;

/// Measures unmeasured locations, then trims every repository over
/// `max_gb`. Returns the number of locations deleted; `None` (no cap
/// configured) does nothing.
pub(super) async fn run(db: &dyn Db, storage: &dyn StorageAdapter, max_gb: Option<u32>) -> u64 {
    let Some(max_gb) = max_gb else {
        return 0;
    };
    measure_unsized(db, storage).await;
    let max_bytes = i64::from(max_gb) * GIB;
    let repos = match db.find_repos_over_size(max_bytes).await {
        Ok(repos) => repos,
        Err(e) => {
            tracing::warn!(error = %e, "cleanup:repo-size: usage query failed");
            return 0;
        }
    };
    let mut deleted = 0;
    for repo in repos {
        deleted += trim_repo(db, storage, &repo, max_bytes).await;
    }
    deleted
}

/// The bytes a location's cache object takes: the merged object when
/// there is one, otherwise the parts it will be merged from. Never both,
/// so a location measured between merge and parts cleanup is not counted
/// twice.
fn object_size(objects: &[ObjectInfo]) -> u64 {
    objects.iter().find(|o| o.name == "merged").map_or_else(
        || objects.iter().map(|o| o.size).sum(),
        |merged| merged.size,
    )
}

async fn measure_unsized(db: &dyn Db, storage: &dyn StorageAdapter) {
    let mut offset = 0_i64;
    loop {
        let page = match db.find_unsized_locations(PAGE_SIZE, offset).await {
            Ok(page) => page,
            Err(e) => {
                tracing::warn!(error = %e, "cleanup:repo-size: unsized query failed");
                return;
            }
        };
        let page_len = page.len();
        for location in page {
            if let Err(e) = measure_one(db, storage, &location).await {
                tracing::warn!(
                    error = %e,
                    location_id = %location.id,
                    folder = %location.folder_name,
                    "cleanup:repo-size: measuring failed",
                );
                // Still unsized, so it would come back on the same page.
                offset += 1;
            }
        }
        if page_len < usize::try_from(PAGE_SIZE).unwrap_or(0) {
            return;
        }
    }
}

async fn measure_one(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    location: &StorageLocation,
) -> Result<(), RepoSizeError> {
    let objects = storage.list_folder(&location.folder_name).await?;
    let size = i64::try_from(object_size(&objects)).unwrap_or(i64::MAX);
    db.set_location_size(&location.id, size).await?;
    Ok(())
}

async fn trim_repo(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    repo: &RepoUsage,
    max_bytes: i64,
) -> u64 {
    let mut total = repo.total_bytes;
    let mut deleted = 0_u64;
    let mut offset = 0_i64;
    while total > max_bytes {
        let page = match db
            .find_repo_locations_lru(&repo.repo_id, PAGE_SIZE, offset)
            .await
        {
            Ok(page) if !page.is_empty() => page,
            Ok(_) => break,
            Err(e) => {
                tracing::warn!(error = %e, repo_id = %repo.repo_id, "cleanup:repo-size: page query failed");
                break;
            }
        };
        for location in page {
            if total <= max_bytes {
                break;
            }
            match delete_one(db, storage, &location).await {
                Ok(()) => {
                    total -= location.size_bytes.unwrap_or(0);
                    deleted += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        location_id = %location.id,
                        folder = %location.folder_name,
                        "cleanup:repo-size: delete failed",
                    );
                    offset += 1;
                }
            }
        }
    }
    tracing::info!(
        repo_id = %repo.repo_id,
        deleted,
        before_bytes = repo.total_bytes,
        after_bytes = total,
        max_bytes,
        "cleanup:repo-size: trimmed repository",
    );
    deleted
}

/// Deletes the location (its entry cascades), then its folder. Errors
/// only while the row is still there: once it is committed away the
/// entry is gone, and a folder left behind by a failed storage delete is
/// logged, as in `cleanup:cache-entries`.
async fn delete_one(
    db: &dyn Db,
    storage: &dyn StorageAdapter,
    location: &StorageLocation,
) -> Result<(), sqlx::Error> {
    let mut tx = db.begin().await?;
    tx.delete_storage_location(&location.id).await?;
    tx.commit().await?;
    if let Err(e) = storage.delete_folder(&location.folder_name).await {
        tracing::warn!(
            error = %e,
            location_id = %location.id,
            folder = %location.folder_name,
            "cleanup:repo-size: entry deleted, its folder was not",
        );
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum RepoSizeError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
#[path = "repo_size_tests.rs"]
mod tests;
