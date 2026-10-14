//! Scenarios for the `cleanup:repo-size` queries
//! (`CACHE_MAX_REPO_SIZE_GB`).

use gha_cache_oxide::db::Db;
use gha_cache_oxide::db::entities::{CacheEntryCoord, RepoUsage};

const REPO: &str = "size-repo";

async fn seed(db: &dyn Db, loc: &str, repo: &str, saved_at: i64) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(loc, loc, 1).await.unwrap();
    let coord = CacheEntryCoord {
        key: loc,
        version: "v",
        scope: "scn-size",
        repo_id: repo,
    };
    tx.seed_cache_entry(&format!("entry-{loc}"), coord, saved_at, loc)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// Unsized locations are those with an entry and no `sizeBytes`;
/// per-repo totals count unmeasured locations as zero and compare
/// strictly; the LRU order takes the later of save and download.
pub async fn repo_size_queries_measure_sum_and_order(db: &dyn Db) {
    seed(db, "sz-1", REPO, 10).await;
    seed(db, "sz-2", REPO, 20).await;
    seed(db, "sz-3", REPO, 30).await;
    seed(db, "sz-other", "size-repo-other", 10).await;
    db.touch_location_downloaded("sz-2", 50).await.unwrap();
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("sz-orphan", "sz-orphan", 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    db.set_location_size("sz-1", 3).await.unwrap();
    db.set_location_size("sz-2", 5).await.unwrap();
    db.set_location_size("sz-other", 100).await.unwrap();
    let measured = db.find_storage_location_by_id("sz-1").await.unwrap();
    assert_eq!(measured.unwrap().size_bytes, Some(3));

    let unmeasured: Vec<String> = db
        .find_unsized_locations(1000, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|l| l.id)
        .filter(|id| id.starts_with("sz-"))
        .collect();
    assert_eq!(unmeasured, vec!["sz-3"]);

    let over = |max| async move {
        db.find_repos_over_size(max)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.repo_id == REPO)
            .collect::<Vec<_>>()
    };
    let expected = RepoUsage {
        repo_id: REPO.to_string(),
        total_bytes: 8,
    };
    assert_eq!(over(7).await, vec![expected]);
    assert!(over(8).await.is_empty(), "a repo at the cap is not over it");

    let lru: Vec<String> = db
        .find_repo_locations_lru(REPO, 10, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|l| l.id)
        .collect();
    assert_eq!(lru, vec!["sz-1", "sz-3", "sz-2"]);
    let second_page = db.find_repo_locations_lru(REPO, 2, 2).await.unwrap();
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].id, "sz-2");
}
