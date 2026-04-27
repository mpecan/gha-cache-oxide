//! Management-API listing scenarios — the four `Db` finders that the
//! REST handlers under `/management` (issue #19) call into.
//!
//! Each scenario uses its own `scope` discriminator so the
//! programmatic runner can thread one `Db` through every scenario
//! without cross-test bleed. Pagination scenarios assert on
//! exactly-the-IDs-they-seeded so a co-running scenario's leftover
//! rows can't fool the assertion.

use std::collections::HashSet;

use gha_cache_oxide::db::Db;
use gha_cache_oxide::db::entities::CacheEntryCoord;

/// Identifying triple for a seeded entry. Bundled with `coord` and
/// `updated_at_ms` it keeps `seed_entry` under the 5-arg clippy limit.
struct EntrySeed<'a> {
    loc_id: &'a str,
    folder: &'a str,
    entry_id: &'a str,
}

async fn seed_entry(
    db: &dyn Db,
    seed: EntrySeed<'_>,
    coord: CacheEntryCoord<'_>,
    updated_at_ms: i64,
) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(seed.loc_id, seed.folder, 1)
        .await
        .unwrap();
    tx.seed_cache_entry(seed.entry_id, coord, updated_at_ms, seed.loc_id)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// `list_cache_entries` with no filter returns every row this scenario
/// seeded; `count_cache_entries` returns the same total. Both honour
/// `LIMIT` / `OFFSET` so paging slices the right window.
pub async fn list_cache_entries_no_filter_paginates(db: &dyn Db) {
    let scope = "scn-mgmt-list-no-filter";
    let mut seeded = Vec::new();
    for i in 0..5 {
        let entry_id = format!("entry-mgmt-list-no-filter-{i}");
        let loc_id = format!("loc-mgmt-list-no-filter-{i}");
        let folder = format!("folder-mgmt-list-no-filter-{i}");
        seed_entry(
            db,
            EntrySeed {
                loc_id: &loc_id,
                folder: &folder,
                entry_id: &entry_id,
            },
            CacheEntryCoord {
                key: "k",
                version: "v",
                scope,
                repo_id: "42",
            },
            // updatedAt strictly increasing so DESC ordering is observable.
            i64::from(i) * 1_000,
        )
        .await;
        seeded.push(entry_id);
    }

    // First page (size 3) — newest 3, in DESC updatedAt order, scoped
    // to this scenario's keyspace.
    let page1 = db
        .list_cache_entries(Some(scope), None, 3, 0)
        .await
        .unwrap();
    let page1_ids: Vec<&str> = page1.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(
        page1_ids,
        vec![seeded[4].as_str(), seeded[3].as_str(), seeded[2].as_str(),],
        "first page must return newest 3 in DESC updatedAt order",
    );

    // Second page — remaining 2.
    let page2 = db
        .list_cache_entries(Some(scope), None, 3, 3)
        .await
        .unwrap();
    let page2_ids: Vec<&str> = page2.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(
        page2_ids,
        vec![seeded[1].as_str(), seeded[0].as_str()],
        "second page must return the remaining 2 oldest entries",
    );

    let count = db.count_cache_entries(Some(scope), None).await.unwrap();
    assert_eq!(count, 5, "count must equal seeded total under this scope");
}

/// `list_cache_entries` filters on `scope` and `repo_id` independently
/// and together. The seeding mixes scopes and repo ids so each filter
/// branch demonstrably narrows the result.
pub async fn list_cache_entries_filters_by_scope_and_repo_id(db: &dyn Db) {
    let scope_a = "scn-mgmt-filter-a";
    let scope_b = "scn-mgmt-filter-b";
    seed_filter_fixture(db, scope_a, scope_b).await;

    assert_scope_filter(db, scope_a).await;
    assert_repo_filter(db).await;
    assert_combined_filter(db, scope_a).await;
}

async fn seed_filter_fixture(db: &dyn Db, scope_a: &str, scope_b: &str) {
    seed_entry(
        db,
        EntrySeed {
            loc_id: "loc-mgmt-filter-1",
            folder: "folder-mgmt-filter-1",
            entry_id: "entry-mgmt-filter-1",
        },
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: scope_a,
            repo_id: "100",
        },
        10,
    )
    .await;
    seed_entry(
        db,
        EntrySeed {
            loc_id: "loc-mgmt-filter-2",
            folder: "folder-mgmt-filter-2",
            entry_id: "entry-mgmt-filter-2",
        },
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: scope_a,
            repo_id: "200",
        },
        20,
    )
    .await;
    seed_entry(
        db,
        EntrySeed {
            loc_id: "loc-mgmt-filter-3",
            folder: "folder-mgmt-filter-3",
            entry_id: "entry-mgmt-filter-3",
        },
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: scope_b,
            repo_id: "100",
        },
        30,
    )
    .await;
}

async fn assert_scope_filter(db: &dyn Db, scope_a: &str) {
    let by_scope = db
        .list_cache_entries(Some(scope_a), None, 100, 0)
        .await
        .unwrap();
    let ids: HashSet<&str> = by_scope.iter().map(|e| e.id.as_str()).collect();
    assert!(ids.contains("entry-mgmt-filter-1"));
    assert!(ids.contains("entry-mgmt-filter-2"));
    assert!(!ids.contains("entry-mgmt-filter-3"));
    assert_eq!(
        db.count_cache_entries(Some(scope_a), None).await.unwrap(),
        2,
    );
}

async fn assert_repo_filter(db: &dyn Db) {
    // Repo filter only — both scopes' "100" rows match.
    let by_repo = db
        .list_cache_entries(None, Some("100"), 100, 0)
        .await
        .unwrap();
    let ids: HashSet<&str> = by_repo.iter().map(|e| e.id.as_str()).collect();
    assert!(ids.contains("entry-mgmt-filter-1"));
    assert!(ids.contains("entry-mgmt-filter-3"));
    assert!(!ids.contains("entry-mgmt-filter-2"));
}

async fn assert_combined_filter(db: &dyn Db, scope_a: &str) {
    let combined = db
        .list_cache_entries(Some(scope_a), Some("100"), 100, 0)
        .await
        .unwrap();
    let ids: HashSet<&str> = combined.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(
        ids,
        HashSet::from(["entry-mgmt-filter-1"]),
        "scope=A AND repoId=100 isolates exactly one row",
    );
    assert_eq!(
        db.count_cache_entries(Some(scope_a), Some("100"))
            .await
            .unwrap(),
        1,
    );
}

/// `list_storage_locations` paginates in stable id order;
/// `count_storage_locations` reports the total across every row the
/// driver carries. Filters by id-prefix so co-running scenarios'
/// rows don't pollute the assertion.
pub async fn list_storage_locations_paginates(db: &dyn Db) {
    for i in 0..4 {
        let loc_id = format!("loc-mgmt-locs-list-{i}");
        let folder = format!("folder-mgmt-locs-list-{i}");
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(&loc_id, &folder, 1)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let page = db.list_storage_locations(2, 0).await.unwrap();
    assert_eq!(page.len(), 2, "page must respect LIMIT");

    // Total must include every seeded row from this scenario; co-running
    // scenarios may have added more, so the assertion is "at least 4".
    let total = db.count_storage_locations().await.unwrap();
    assert!(
        total >= 4,
        "count must include every seeded location (got {total})",
    );

    // Pagination is contiguous, ordered by id.
    let everything = db.list_storage_locations(1_000, 0).await.unwrap();
    let mut ids: Vec<&str> = everything
        .iter()
        .map(|l| l.id.as_str())
        .filter(|id| id.starts_with("loc-mgmt-locs-list-"))
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![
            "loc-mgmt-locs-list-0",
            "loc-mgmt-locs-list-1",
            "loc-mgmt-locs-list-2",
            "loc-mgmt-locs-list-3",
        ],
    );
}
