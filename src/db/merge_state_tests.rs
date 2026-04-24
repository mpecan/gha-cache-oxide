//! Lazy-merge state transition tests (`try_mark_merge_started`,
//! `mark_merged`, `reset_merge_flags`, `mark_parts_deleted`). Split from
//! `queries_tests.rs` to keep both files under the 500-line soft limit.
//! Attached via `#[path]` from `sqlite.rs`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use super::*;
use crate::db::Db;
use crate::db::entities::StorageLocation;

async fn test_db() -> SqliteDb {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    db
}

#[tokio::test]
async fn try_mark_merge_started_wins_then_loses() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-cas", "folder-cas", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert!(
        db.try_mark_merge_started("loc-cas", 1_000).await.unwrap(),
        "first caller wins"
    );
    assert!(
        !db.try_mark_merge_started("loc-cas", 2_000).await.unwrap(),
        "second caller loses while merge is in flight"
    );

    let loc: StorageLocation = sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind("loc-cas")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_eq!(
        loc.merge_started_at,
        Some(1_000),
        "winner's timestamp sticks"
    );
}

#[tokio::test]
async fn try_mark_merge_started_loses_when_already_merged() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-merged", "folder-merged", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.try_mark_merge_started("loc-merged", 1_000)
        .await
        .unwrap();
    db.mark_merged("loc-merged", 2_000).await.unwrap();
    // reset_merge_flags clears both columns; set merged_at again to prove
    // the CAS WHERE clause actually checks the `mergedAt` column.
    db.reset_merge_flags("loc-merged").await.unwrap();
    db.mark_merged("loc-merged", 3_000).await.unwrap();

    assert!(
        !db.try_mark_merge_started("loc-merged", 4_000)
            .await
            .unwrap(),
        "CAS must lose when merged_at is set"
    );
}

#[tokio::test]
async fn mark_merged_sets_timestamp() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-m", "folder-m", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    db.mark_merged("loc-m", 1_700_000_123_000).await.unwrap();
    let loc: StorageLocation = sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind("loc-m")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_eq!(loc.merged_at, Some(1_700_000_123_000));
}

#[tokio::test]
async fn reset_merge_flags_clears_both_columns() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-r", "folder-r", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.try_mark_merge_started("loc-r", 1_000).await.unwrap();
    db.mark_merged("loc-r", 2_000).await.unwrap();

    db.reset_merge_flags("loc-r").await.unwrap();
    let loc: StorageLocation = sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind("loc-r")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert!(loc.merge_started_at.is_none());
    assert!(loc.merged_at.is_none());

    assert!(
        db.try_mark_merge_started("loc-r", 3_000).await.unwrap(),
        "after reset, CAS must win again"
    );
}

#[tokio::test]
async fn mark_parts_deleted_inside_transaction() {
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-pd", "folder-pd", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin().await.unwrap();
    tx.mark_parts_deleted("loc-pd", 1_700_000_777_000)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let loc: StorageLocation = sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind("loc-pd")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert_eq!(loc.parts_deleted_at, Some(1_700_000_777_000));
}

#[tokio::test]
async fn mark_parts_deleted_rollback_is_atomic() {
    // Rolling back the tx leaves `partsDeletedAt` NULL — matches upstream
    // where the adapter.deleteFolder call inside the tx callback rolls the
    // row change back when it fails.
    let db = test_db().await;
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-pd-r", "folder-pd-r", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin().await.unwrap();
    tx.mark_parts_deleted("loc-pd-r", 1_000).await.unwrap();
    tx.rollback().await.unwrap();

    let loc: StorageLocation = sqlx::query_as("SELECT * FROM storage_locations WHERE id = ?")
        .bind("loc-pd-r")
        .fetch_one(db.as_sqlite_pool().expect("SQLite test harness"))
        .await
        .unwrap();
    assert!(
        loc.parts_deleted_at.is_none(),
        "rollback must drop the partsDeletedAt update"
    );
}
