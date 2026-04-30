//! Recovery scenarios — crash-recovery / inconsistent-state DB helpers
//! (issue #17). Kept in a sibling submodule so the main scenarios file
//! stays under the 700-line hard limit.
//!
//! Each scenario asserts on per-row read-back rather than the global
//! `rows_affected` count — the programmatic runner shares one `Db`
//! across every scenario, so earlier ones (e.g.
//! `lazy_merge_cas_winner_and_loser`) can leave unrelated stale
//! claims in place. Per-row assertions are both more precise and
//! runner-safe.

use gha_cache_oxide::db::Db;
use gha_cache_oxide::db::entities::CacheEntryCoord;

/// Inputs to [`seed_location_with_flags`]. Grouped so the helper keeps
/// under the 5-argument clippy limit and so scenario bodies stay
/// readable (every field is labelled at the call site).
struct Seed<'a> {
    loc_id: &'a str,
    folder: &'a str,
    entry_id: &'a str,
    scope: &'a str,
    merge_started_at: Option<i64>,
    merged_at: Option<i64>,
}

/// Shared seeding for the recovery scenarios — a `storage_locations`
/// row plus a `cache_entries` row that points at it (so
/// `find_location_for_entry` works for read-back). The caller supplies
/// the `merge_started_at` and `merged_at` timestamps to load via the
/// `Seed` struct; both are optional.
async fn seed_location_with_flags(db: &dyn Db, seed: Seed<'_>) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(seed.loc_id, seed.folder, 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        seed.entry_id,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: seed.scope,
            repo_id: "r",
        },
        0,
        seed.loc_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    if let Some(started) = seed.merge_started_at {
        assert!(
            db.try_mark_merge_started(seed.loc_id, started)
                .await
                .unwrap(),
            "initial CAS must win so the seed lands"
        );
    }
    if let Some(merged) = seed.merged_at {
        db.mark_merged(seed.loc_id, merged).await.unwrap();
    }
}

/// Stale claim: `mergeStartedAt` older than cutoff, `mergedAt` NULL.
/// Sweep clears `mergeStartedAt`, leaves `mergedAt` NULL. The cleared
/// row is then eligible for the CAS claim at the next download
/// (covered by `lazy_merge_cas_winner_and_loser`).
pub async fn clear_stale_merge_claims_clears_old_claims(db: &dyn Db) {
    seed_location_with_flags(
        db,
        Seed {
            loc_id: "loc-recov-old",
            folder: "folder-recov-old",
            entry_id: "entry-recov-old",
            scope: "scn-recov-old",
            merge_started_at: Some(1_000),
            merged_at: None,
        },
    )
    .await;

    db.clear_stale_merge_claims(5_000).await.unwrap();

    let after = db
        .find_location_for_entry("entry-recov-old")
        .await
        .unwrap()
        .unwrap();
    assert!(
        after.merge_started_at.is_none(),
        "mergeStartedAt must be cleared"
    );
    assert!(
        after.merged_at.is_none(),
        "mergedAt must stay NULL (invariant: both NULL = idle)"
    );
}

/// Fresh claim: `mergeStartedAt` newer than cutoff. Sweep leaves it
/// alone so a merger that's legitimately in flight is never yanked.
pub async fn clear_stale_merge_claims_leaves_fresh_claims(db: &dyn Db) {
    seed_location_with_flags(
        db,
        Seed {
            loc_id: "loc-recov-fresh",
            folder: "folder-recov-fresh",
            entry_id: "entry-recov-fresh",
            scope: "scn-recov-fresh",
            merge_started_at: Some(10_000),
            merged_at: None,
        },
    )
    .await;

    db.clear_stale_merge_claims(5_000).await.unwrap();

    let after = db
        .find_location_for_entry("entry-recov-fresh")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.merge_started_at,
        Some(10_000),
        "fresh claim must be preserved"
    );
    assert!(after.merged_at.is_none());
}

/// Two non-stale states — the sweep must touch neither:
///   (a) completed merge (`mergedAt` set): the merger finished; clearing
///       would not help the download path and the row is already healthy.
///   (b) idle row (both NULL): no claim to clear.
/// A stale claim on a third row is seeded so the sweep has something
/// it *should* clear; we assert only that row transitions.
pub async fn clear_stale_merge_claims_ignores_completed_and_idle_rows(db: &dyn Db) {
    let seeds = [
        // (a) completed merge with a very old mergeStartedAt — mergedAt set
        Seed {
            loc_id: "loc-recov-done",
            folder: "folder-recov-done",
            entry_id: "entry-recov-done",
            scope: "scn-recov-done",
            merge_started_at: Some(100),
            merged_at: Some(200),
        },
        // (b) idle — never claimed
        Seed {
            loc_id: "loc-recov-idle",
            folder: "folder-recov-idle",
            entry_id: "entry-recov-idle",
            scope: "scn-recov-idle",
            merge_started_at: None,
            merged_at: None,
        },
        // (c) stale — the only row that should flip
        Seed {
            loc_id: "loc-recov-stale",
            folder: "folder-recov-stale",
            entry_id: "entry-recov-stale",
            scope: "scn-recov-stale",
            merge_started_at: Some(100),
            merged_at: None,
        },
    ];
    for seed in seeds {
        seed_location_with_flags(db, seed).await;
    }

    db.clear_stale_merge_claims(1_000).await.unwrap();

    // Completed merge untouched.
    let done = db
        .find_location_for_entry("entry-recov-done")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        done.merge_started_at,
        Some(100),
        "completed merge's mergeStartedAt must be preserved"
    );
    assert_eq!(done.merged_at, Some(200));

    // Idle untouched.
    let idle = db
        .find_location_for_entry("entry-recov-idle")
        .await
        .unwrap()
        .unwrap();
    assert!(idle.merge_started_at.is_none());
    assert!(idle.merged_at.is_none());

    // Stale cleared.
    let stale = db
        .find_location_for_entry("entry-recov-stale")
        .await
        .unwrap()
        .unwrap();
    assert!(stale.merge_started_at.is_none());
    assert!(stale.merged_at.is_none());
}

/// `get_merge_state` returns each of the four observable
/// `(mergeStartedAt, mergedAt)` combinations, plus `None` for a
/// non-existent location. Issue #51 — the loser-wait path polls
/// this query, so both drivers must agree on the snapshot semantics.
pub async fn get_merge_state_covers_all_four_combinations(db: &dyn Db) {
    // (a) Non-existent location → None.
    assert!(
        db.get_merge_state("missing-conf-ms")
            .await
            .unwrap()
            .is_none(),
        "non-existent location must return None"
    );

    // Seed an idle location.
    seed_location_with_flags(
        db,
        Seed {
            loc_id: "loc-conf-ms",
            folder: "folder-conf-ms",
            entry_id: "entry-conf-ms",
            scope: "scn-conf-ms",
            merge_started_at: None,
            merged_at: None,
        },
    )
    .await;

    // (b) Idle (both NULL).
    let s = db.get_merge_state("loc-conf-ms").await.unwrap().unwrap();
    assert!(s.merge_started_at.is_none(), "idle: mergeStartedAt is None");
    assert!(s.merged_at.is_none(), "idle: mergedAt is None");

    // (c) After CAS claim → mergeStartedAt set, mergedAt NULL.
    assert!(db.try_mark_merge_started("loc-conf-ms", 100).await.unwrap());
    let s = db.get_merge_state("loc-conf-ms").await.unwrap().unwrap();
    assert_eq!(s.merge_started_at, Some(100));
    assert!(s.merged_at.is_none());

    // (d) After mark_merged → both set; mergeStartedAt unchanged.
    db.mark_merged("loc-conf-ms", 200).await.unwrap();
    let s = db.get_merge_state("loc-conf-ms").await.unwrap().unwrap();
    assert_eq!(s.merge_started_at, Some(100));
    assert_eq!(s.merged_at, Some(200));
}
