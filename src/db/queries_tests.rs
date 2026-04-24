//! Tests for `src/db/sqlite.rs`, split out to keep the production file
//! under the 500-line soft limit. Attached via `#[path]` attribute.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use super::*;
use crate::db::Db;
use crate::db::entities::{CacheEntryCoord, NewUpload, StorageLocation};
use crate::db::id::{new_upload_id, new_uuid};

async fn test_db() -> SqliteDb {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn coord<'a>() -> CacheEntryCoord<'a> {
    CacheEntryCoord {
        key: "cache-key",
        version: "v1",
        scope: "refs/heads/main",
        repo_id: "42",
    }
}

fn new_upload(id: i64, folder: &str) -> NewUpload<'_> {
    NewUpload {
        id,
        coord: coord(),
        folder_name: folder,
        created_at_ms: 1_700_000_000_000,
    }
}

#[tokio::test]
async fn create_upload_then_find_by_id_roundtrip() {
    let db = test_db().await;
    let id = new_upload_id();
    let inserted = db.create_upload(new_upload(id, "fldr-1")).await.unwrap();
    assert_eq!(inserted.id, id);
    assert_eq!(inserted.key, "cache-key");
    assert_eq!(inserted.started_part_upload_count, 0);
    assert_eq!(inserted.finished_part_upload_count, 0);
    assert!(inserted.last_part_uploaded_at.is_none());

    let fetched = db.find_upload_by_id(id).await.unwrap().unwrap();
    assert_eq!(fetched.id, inserted.id);
    assert_eq!(fetched.folder_name, "fldr-1");
}

#[tokio::test]
async fn find_upload_by_id_returns_none_for_unknown() {
    let db = test_db().await;
    assert!(db.find_upload_by_id(9_999_999_999).await.unwrap().is_none());
}

#[tokio::test]
async fn find_upload_by_coord_matches_scope_and_repo() {
    let db = test_db().await;
    let a = new_upload_id();
    let b = new_upload_id();
    db.create_upload(NewUpload {
        id: a,
        coord: CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-A",
            repo_id: "1",
        },
        folder_name: "f-a",
        created_at_ms: 0,
    })
    .await
    .unwrap();
    db.create_upload(NewUpload {
        id: b,
        coord: CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-B",
            repo_id: "2",
        },
        folder_name: "f-b",
        created_at_ms: 0,
    })
    .await
    .unwrap();

    let hit = db
        .find_upload_by_coord(CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-B",
            repo_id: "2",
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hit.id, b);

    let miss = db
        .find_upload_by_coord(CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scope-nonexistent",
            repo_id: "2",
        })
        .await
        .unwrap();
    assert!(miss.is_none());
}

#[tokio::test]
async fn increment_upload_started_bumps_counter() {
    let db = test_db().await;
    let id = new_upload_id();
    db.create_upload(new_upload(id, "fldr")).await.unwrap();
    db.increment_upload_started(id).await.unwrap();
    db.increment_upload_started(id).await.unwrap();
    let u = db.find_upload_by_id(id).await.unwrap().unwrap();
    assert_eq!(u.started_part_upload_count, 2);
    assert_eq!(u.finished_part_upload_count, 0);
}

#[tokio::test]
async fn increment_upload_finished_bumps_counter_and_sets_timestamp() {
    let db = test_db().await;
    let id = new_upload_id();
    db.create_upload(new_upload(id, "fldr")).await.unwrap();
    db.increment_upload_finished(id, 1_700_000_500_000)
        .await
        .unwrap();
    let u = db.find_upload_by_id(id).await.unwrap().unwrap();
    assert_eq!(u.finished_part_upload_count, 1);
    assert_eq!(u.last_part_uploaded_at, Some(1_700_000_500_000));
}

#[tokio::test]
async fn delete_upload_removes_row() {
    let db = test_db().await;
    let id = new_upload_id();
    db.create_upload(new_upload(id, "fldr")).await.unwrap();
    db.delete_upload(id).await.unwrap();
    assert!(db.find_upload_by_id(id).await.unwrap().is_none());
}

#[tokio::test]
async fn insert_storage_location_roundtrip() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-1", "folder-x", 7)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let loc: StorageLocation = sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind("loc-1")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_eq!(loc.folder_name, "folder-x");
    assert_eq!(loc.part_count, 7);
    assert!(loc.merged_at.is_none());
    assert!(loc.merge_started_at.is_none());
    assert!(loc.parts_deleted_at.is_none());
    assert!(loc.last_downloaded_at.is_none());
}

#[tokio::test]
async fn find_location_for_entry_joins_correctly() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-A", "folder-A", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "entry-A",
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "s",
            repo_id: "r",
        },
        1_700_000_000_000,
        "loc-A",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let loc = db
        .find_location_for_entry("entry-A")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loc.id, "loc-A");
    assert_eq!(loc.folder_name, "folder-A");

    let miss = db.find_location_for_entry("unknown").await.unwrap();
    assert!(miss.is_none());
}

#[tokio::test]
async fn touch_location_downloaded_sets_timestamp() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-T", "folder-T", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    db.touch_location_downloaded("loc-T", 1_700_000_999_000)
        .await
        .unwrap();

    let loc: StorageLocation = sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind("loc-T")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_eq!(loc.last_downloaded_at, Some(1_700_000_999_000));
}

#[tokio::test]
async fn upsert_cache_entry_insert_path_returns_none() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-new", "folder-new", 1)
        .await
        .unwrap();
    let previous = tx
        .upsert_cache_entry(coord(), "loc-new", 1_700_000_000_000)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert!(previous.is_none());
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cache_entries")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn upsert_cache_entry_update_path_returns_previous_location() {
    let db = test_db().await;

    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-old", "folder-old", 1)
        .await
        .unwrap();
    tx.upsert_cache_entry(coord(), "loc-old", 1_000)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-new", "folder-new", 2)
        .await
        .unwrap();
    let previous = tx
        .upsert_cache_entry(coord(), "loc-new", 2_000)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let previous = previous.expect("update path should return previous location");
    assert_eq!(previous.id, "loc-old");
    assert_eq!(previous.folder_name, "folder-old");

    let entries: Vec<(String, String, i64)> =
        sqlx::query_as("SELECT id, locationId, updatedAt FROM cache_entries WHERE key = ?")
            .bind("cache-key")
            .fetch_all(db.as_sqlite_pool().expect("SQLite test harness"))
            .await
            .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, "loc-new");
    assert_eq!(entries[0].2, 2_000);
}

#[tokio::test]
async fn create_upload_with_duplicate_id_fails() {
    let db = test_db().await;
    let id = new_upload_id();
    db.create_upload(new_upload(id, "a")).await.unwrap();

    let err = db.create_upload(new_upload(id, "b")).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("unique")
            || msg.to_lowercase().contains("primary")
            || msg.to_lowercase().contains("constraint"),
        "expected unique-constraint error, got: {msg}"
    );
}

#[tokio::test]
async fn update_helpers_are_noops_on_unknown_ids() {
    let db = test_db().await;

    db.increment_upload_started(1).await.unwrap();
    db.increment_upload_finished(1, 0).await.unwrap();
    db.delete_upload(1).await.unwrap();
    db.touch_location_downloaded("does-not-exist", 0)
        .await
        .unwrap();
}

#[tokio::test]
async fn empty_string_coords_round_trip() {
    let db = test_db().await;
    let empty = CacheEntryCoord {
        key: "",
        version: "",
        scope: "",
        repo_id: "",
    };
    let id = new_upload_id();
    db.create_upload(NewUpload {
        id,
        coord: empty,
        folder_name: "fldr",
        created_at_ms: 0,
    })
    .await
    .unwrap();

    let u = db.find_upload_by_coord(empty).await.unwrap().unwrap();
    assert_eq!(u.id, id);
    assert_eq!(u.key, "");
    assert_eq!(u.scope, "");
}

#[tokio::test]
async fn find_upload_by_coord_discriminates_each_column() {
    let db = test_db().await;
    let id = new_upload_id();
    let base = CacheEntryCoord {
        key: "K",
        version: "V",
        scope: "S",
        repo_id: "R",
    };
    db.create_upload(NewUpload {
        id,
        coord: base,
        folder_name: "f",
        created_at_ms: 0,
    })
    .await
    .unwrap();

    for (label, c) in [
        ("wrong key", CacheEntryCoord { key: "X", ..base }),
        (
            "wrong version",
            CacheEntryCoord {
                version: "X",
                ..base
            },
        ),
        ("wrong scope", CacheEntryCoord { scope: "X", ..base }),
        (
            "wrong repo_id",
            CacheEntryCoord {
                repo_id: "X",
                ..base
            },
        ),
    ] {
        assert!(
            db.find_upload_by_coord(c).await.unwrap().is_none(),
            "should miss on {label}"
        );
    }

    assert!(db.find_upload_by_coord(base).await.unwrap().is_some());
}

#[tokio::test]
async fn deleting_old_storage_location_cascades_to_cache_entry() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-doomed", "folder-doomed", 1)
        .await
        .unwrap();
    let entry_id = new_uuid();
    tx.seed_cache_entry(
        &entry_id,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "s",
            repo_id: "r",
        },
        0,
        "loc-doomed",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    sqlx::query("DELETE FROM storage_locations WHERE id = 'loc-doomed'")
        .execute(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();

    let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cache_entries WHERE id = ?")
        .bind(&entry_id)
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_eq!(remaining.0, 0, "CASCADE should have removed the entry");
}
