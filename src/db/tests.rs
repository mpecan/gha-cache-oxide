//! Schema-level tests for the `Db` layer (moved out of `mod.rs` to keep
//! it under the 700-line hard limit).

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use super::*;

#[tokio::test]
async fn connect_in_memory_and_migrate_is_idempotent() {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    db.migrate().await.unwrap();
}

#[tokio::test]
async fn migrations_produce_expected_tables() {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let pool = db.as_sqlite_pool().expect("sqlite pool exposed");
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_sqlx%' ESCAPE '\\' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap();

    assert_eq!(
        tables,
        vec!["cache_entries", "storage_locations", "uploads"]
    );
}

#[tokio::test]
async fn cache_entries_foreign_key_is_on_delete_cascade() {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let pool = db.as_sqlite_pool().expect("sqlite pool exposed");
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT \"table\", \"from\", \"on_delete\" FROM pragma_foreign_key_list('cache_entries')",
    )
    .fetch_all(pool)
    .await
    .unwrap();

    assert_eq!(rows.len(), 1, "expected exactly one FK on cache_entries");
    let (table, from, on_delete) = &rows[0];
    assert_eq!(table, "storage_locations");
    assert_eq!(from, "locationId");
    assert_eq!(on_delete, "CASCADE");
}

#[tokio::test]
async fn foreign_keys_are_enforced_at_runtime() {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    let pool = db.as_sqlite_pool().expect("sqlite pool exposed");
    let result = sqlx::query(
        "INSERT INTO cache_entries (id, key, version, updatedAt, locationId, scope, repoId)
         VALUES ('e1', 'k', 'v', 0, 'missing-location', 's', 'r')",
    )
    .execute(pool)
    .await;

    let err = result.expect_err("FK violation should fail the insert");
    assert!(
        format!("{err}").to_lowercase().contains("foreign key"),
        "expected FK error, got: {err}"
    );
}
