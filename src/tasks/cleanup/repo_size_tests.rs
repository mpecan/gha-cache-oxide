#![allow(clippy::unwrap_used)]

use super::super::test_utils::FakeStorage;
use super::{GIB, object_size, run};
use crate::db::entities::CacheEntryCoord;
use crate::db::{Db, SqliteDb};
use crate::storage::ObjectInfo;

async fn fresh_db() -> SqliteDb {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    db
}

/// Seeds location `loc` (folder `folder-<loc>`) with one entry saved
/// at `saved_at`, in the repo named by `loc`'s part before the first
/// `-`, and a merged object of `gib` GiB.
async fn seed(db: &SqliteDb, storage: &FakeStorage, loc: &str, saved_at: i64, gib: u64) {
    let folder = format!("folder-{loc}");
    let repo = loc.split('-').next().unwrap();
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(loc, &folder, 1).await.unwrap();
    let coord = CacheEntryCoord {
        key: loc,
        version: "v",
        scope: "s",
        repo_id: repo,
    };
    tx.seed_cache_entry(&format!("entry-{loc}"), coord, saved_at, loc)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    storage.put_folder(&folder, &[("merged", gib * GIB.unsigned_abs())]);
}

async fn present(db: &SqliteDb, loc: &str) -> bool {
    db.find_location_for_entry(&format!("entry-{loc}"))
        .await
        .unwrap()
        .is_some()
}

fn objects(list: &[(&str, u64)]) -> Vec<ObjectInfo> {
    list.iter()
        .map(|(name, size)| ObjectInfo {
            name: (*name).to_string(),
            size: *size,
        })
        .collect()
}

#[test]
fn object_size_prefers_the_merged_object_over_its_parts() {
    assert_eq!(
        object_size(&objects(&[("merged", 7), ("parts/0", 4), ("parts/1", 3)])),
        7
    );
    assert_eq!(object_size(&objects(&[("parts/0", 4), ("parts/1", 3)])), 7);
    assert_eq!(object_size(&[]), 0);
}

#[tokio::test]
async fn measures_each_location_once() {
    let db = fresh_db().await;
    let storage = FakeStorage::new();
    seed(&db, &storage, "r-a", 1, 2).await;
    // Under the cap: nothing is deleted, but the size is recorded.
    assert_eq!(run(&db, &storage, Some(10)).await, 0);
    let loc = db
        .find_location_for_entry("entry-r-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loc.size_bytes, Some(2 * GIB));
    assert!(db.find_unsized_locations(10, 0).await.unwrap().is_empty());

    // A later change in storage is not re-measured.
    storage.put_folder("folder-r-a", &[("merged", 5 * GIB.unsigned_abs())]);
    run(&db, &storage, Some(10)).await;
    let loc = db
        .find_location_for_entry("entry-r-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loc.size_bytes, Some(2 * GIB));
}

#[tokio::test]
async fn trims_least_recently_used_until_the_repo_fits() {
    let db = fresh_db().await;
    let storage = FakeStorage::new();
    // 4 GiB in `big`, cap 2 GiB: the two oldest go, the newest two stay.
    seed(&db, &storage, "big-1", 10, 1).await;
    seed(&db, &storage, "big-2", 20, 1).await;
    seed(&db, &storage, "big-3", 30, 1).await;
    seed(&db, &storage, "big-4", 40, 1).await;
    // Another repo at 2 GiB is at, not over, the cap.
    seed(&db, &storage, "small-1", 1, 2).await;

    assert_eq!(run(&db, &storage, Some(2)).await, 2);

    let mut gone = storage.deleted_folders();
    gone.sort();
    assert_eq!(gone, vec!["folder-big-1", "folder-big-2"]);
    assert!(present(&db, "big-3").await && present(&db, "big-4").await);
    assert!(present(&db, "small-1").await);
}

#[tokio::test]
async fn a_recent_download_counts_as_use() {
    let db = fresh_db().await;
    let storage = FakeStorage::new();
    seed(&db, &storage, "r-old-but-restored", 10, 1).await;
    seed(&db, &storage, "r-newer-unused", 20, 1).await;
    db.touch_location_downloaded("r-old-but-restored", 30)
        .await
        .unwrap();

    assert_eq!(run(&db, &storage, Some(1)).await, 1);
    assert_eq!(storage.deleted_folders(), vec!["folder-r-newer-unused"]);
    assert!(present(&db, "r-old-but-restored").await);
}

#[tokio::test]
async fn an_entry_larger_than_the_cap_is_deleted() {
    let db = fresh_db().await;
    let storage = FakeStorage::new();
    seed(&db, &storage, "r-huge", 10, 3).await;
    assert_eq!(run(&db, &storage, Some(2)).await, 1);
    assert!(!present(&db, "r-huge").await);
}

#[tokio::test]
async fn a_failed_folder_delete_still_counts_the_entry_as_gone() {
    let db = fresh_db().await;
    let storage = FakeStorage::new();
    seed(&db, &storage, "r-first", 10, 1).await;
    seed(&db, &storage, "r-second", 20, 1).await;
    seed(&db, &storage, "r-third", 30, 1).await;
    storage.fail_next_delete();

    // The first delete commits but leaves its folder; the pass moves on
    // to the second and stops there, at the cap.
    assert_eq!(run(&db, &storage, Some(1)).await, 2);
    assert!(!present(&db, "r-first").await && !present(&db, "r-second").await);
    assert!(present(&db, "r-third").await);
    assert_eq!(storage.deleted_folders(), vec!["folder-r-second"]);
}
