//! Cleanup scenarios — the four `Db` finders and the one `DbTx`
//! gated-delete that the background cleanup tasks (issue #18) call into.
//! Kept in a sibling submodule so the main scenarios file stays under
//! the 700-line hard limit.
//!
//! Each scenario asserts on per-row read-back rather than aggregate
//! counts — the programmatic runner threads one `Db` through every
//! scenario, so earlier scenarios may leave unrelated rows in
//! `uploads` / `storage_locations`. Filtering the finder's output by
//! the IDs this scenario seeded keeps the assertions tight.
//!
//! All `lastDownloadedAt`-style cutoffs use timestamps in the
//! `0..200_000` range so they cannot collide with the `1_700_000_*`
//! timestamps the upload-lifecycle scenarios use.
//!
//! Mirrors upstream `tasks/cleanup/{uploads,cache-entries,
//! storage-locations,parts}.ts` — one scenario per finder and one for
//! the `delete_upload_if_stale` re-check that upstream does inside its
//! transaction (`uploads.ts:45-57`).

use gha_cache_oxide::db::Db;
use gha_cache_oxide::db::entities::{CacheEntryCoord, NewUpload};
use gha_cache_oxide::db::id::new_upload_id;

/// Seeds a `storage_locations` row plus a `cache_entries` row that
/// points at it, so `find_location_for_entry` is a working read-back
/// path for the scenarios that need to check post-call state.
async fn seed_location_with_entry(
    db: &dyn Db,
    loc_id: &str,
    folder: &str,
    entry_id: &str,
    scope: &str,
) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(loc_id, folder, 1).await.unwrap();
    tx.seed_cache_entry(
        entry_id,
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope,
            repo_id: "r",
        },
        0,
        loc_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// Seeds a `storage_locations` row with no associated `cache_entries`
/// row — i.e. an orphan location. Used by the orphan-locations
/// scenario.
async fn seed_orphan_location(db: &dyn Db, loc_id: &str, folder: &str) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(loc_id, folder, 1).await.unwrap();
    tx.commit().await.unwrap();
}

/// Bundles `seed_upload`'s discriminating fields so the helper stays
/// under the 5-arg clippy limit.
struct UploadSeed<'a> {
    id: i64,
    key: &'a str,
    scope: &'a str,
    folder: &'a str,
    created_at_ms: i64,
}

/// Seeds an `uploads` row. `version`/`repo` are constant across this
/// file's scenarios; `scope` discriminates between scenarios so the
/// programmatic runner doesn't see cross-scenario key collisions.
async fn seed_upload(db: &dyn Db, seed: UploadSeed<'_>) {
    db.create_upload(NewUpload {
        id: seed.id,
        coord: CacheEntryCoord {
            key: seed.key,
            version: "v",
            scope: seed.scope,
            repo_id: "r",
        },
        folder_name: seed.folder,
        created_at_ms: seed.created_at_ms,
    })
    .await
    .unwrap();
}

/// Inserts the four uploads `find_stale_uploads_filters_on_both_predicates`
/// needs and returns their ids in a fixed order.
async fn seed_stale_uploads_quartet(db: &dyn Db) -> [i64; 4] {
    let scope = "scn-cleanup-stale-uploads";
    let ids = [
        new_upload_id(),
        new_upload_id(),
        new_upload_id(),
        new_upload_id(),
    ];
    // (a) old createdAt, NULL lastPartUploadedAt → stale
    seed_upload(
        db,
        UploadSeed {
            id: ids[0],
            key: "k",
            scope,
            folder: "stale-no-parts",
            created_at_ms: 0,
        },
    )
    .await;
    // (b) old createdAt + old lastPartUploadedAt → stale
    seed_upload(
        db,
        UploadSeed {
            id: ids[1],
            key: "k2",
            scope,
            folder: "stale-old-parts",
            created_at_ms: 0,
        },
    )
    .await;
    db.increment_upload_finished(ids[1], 1_000).await.unwrap();
    // (c) old createdAt + recent lastPartUploadedAt → fresh
    seed_upload(
        db,
        UploadSeed {
            id: ids[2],
            key: "k3",
            scope,
            folder: "fresh-recent-parts",
            created_at_ms: 0,
        },
    )
    .await;
    db.increment_upload_finished(ids[2], 100_000).await.unwrap();
    // (d) recent createdAt → fresh
    seed_upload(
        db,
        UploadSeed {
            id: ids[3],
            key: "k4",
            scope,
            folder: "fresh-recent-created",
            created_at_ms: 100_000,
        },
    )
    .await;
    ids
}

/// `find_stale_uploads` filters on **both** predicates upstream uses:
/// the `createdAt < cutoff` outer guard and the
/// `lastPartUploadedAt IS NULL OR lastPartUploadedAt < cutoff` inner
/// disjunction. Seeds four uploads, asserts that exactly the two
/// matching rows come back.
pub async fn find_stale_uploads_filters_on_both_predicates(db: &dyn Db) {
    let cutoff = 50_000_i64;
    let [
        stale_no_parts,
        stale_old_parts,
        fresh_recent_parts,
        fresh_recent_created,
    ] = seed_stale_uploads_quartet(db).await;

    let stale = db.find_stale_uploads(cutoff, 100, 0).await.unwrap();
    let stale_ids: std::collections::HashSet<i64> = stale.iter().map(|u| u.id).collect();

    assert!(
        stale_ids.contains(&stale_no_parts),
        "old createdAt + NULL lastPartUploadedAt must be stale"
    );
    assert!(
        stale_ids.contains(&stale_old_parts),
        "old createdAt + old lastPartUploadedAt must be stale"
    );
    assert!(
        !stale_ids.contains(&fresh_recent_parts),
        "old createdAt but recent lastPartUploadedAt must NOT be stale"
    );
    assert!(
        !stale_ids.contains(&fresh_recent_created),
        "recent createdAt must NOT be stale even with NULL lastPartUploadedAt"
    );
}

/// `delete_upload_if_stale` re-checks the staleness predicate inside
/// the transaction. Upstream's `uploads.ts:45-57` does the same — a
/// concurrent `completeUpload` between the SELECT and the DELETE could
/// have promoted the row, and deleting blindly would wipe live data.
///
/// Seeds a stale upload, mutates it to fresh **before** the gated
/// delete fires, and asserts the row is preserved and the call
/// returns `false`. Then mutates back to stale and confirms the call
/// returns `true` and the row is gone.
pub async fn delete_upload_if_stale_re_checks_predicate(db: &dyn Db) {
    let cutoff = 50_000_i64;
    let upload_id = new_upload_id();
    let seed = || UploadSeed {
        id: upload_id,
        key: "k-recheck",
        scope: "scn-cleanup-recheck",
        folder: "fldr-recheck",
        created_at_ms: 0,
    };
    seed_upload(db, seed()).await;

    // Race window: the cleanup task SELECTed the row as stale, but
    // between then and the DELETE, an in-flight upload bumped
    // lastPartUploadedAt past the cutoff.
    db.increment_upload_finished(upload_id, 100_000)
        .await
        .unwrap();

    let mut tx = db.begin().await.unwrap();
    let deleted = tx.delete_upload_if_stale(upload_id, cutoff).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        !deleted,
        "row mutated to fresh between SELECT and DELETE must be preserved"
    );
    assert!(
        db.find_upload_by_id(upload_id).await.unwrap().is_some(),
        "row must still exist"
    );

    // Reset the row to genuinely stale (DELETE + INSERT, since we
    // can't move lastPartUploadedAt backwards through the public
    // surface) and verify the gated delete now succeeds.
    db.delete_upload(upload_id).await.unwrap();
    seed_upload(db, seed()).await;

    let mut tx = db.begin().await.unwrap();
    let deleted = tx.delete_upload_if_stale(upload_id, cutoff).await.unwrap();
    tx.commit().await.unwrap();
    assert!(deleted, "stale row must be deleted");
    assert!(
        db.find_upload_by_id(upload_id).await.unwrap().is_none(),
        "row must be gone"
    );
}

/// `find_expired_locations` returns rows whose `lastDownloadedAt` is
/// strictly less than the cutoff. NULL `lastDownloadedAt` is **not**
/// considered expired (matches SQL semantics: `NULL < cutoff` is
/// neither true nor false, and upstream's Kysely `where(..., '<', ...)`
/// uses the same SQL operator). This matches upstream parity —
/// never-downloaded entries are never cleaned up by `cache-entries.ts`.
pub async fn find_expired_locations_respects_cutoff(db: &dyn Db) {
    let cutoff = 50_000_i64;

    seed_location_with_entry(
        db,
        "loc-cleanup-exp-old",
        "folder-exp-old",
        "entry-cleanup-exp-old",
        "scn-cleanup-exp-old",
    )
    .await;
    seed_location_with_entry(
        db,
        "loc-cleanup-exp-fresh",
        "folder-exp-fresh",
        "entry-cleanup-exp-fresh",
        "scn-cleanup-exp-fresh",
    )
    .await;
    seed_location_with_entry(
        db,
        "loc-cleanup-exp-null",
        "folder-exp-null",
        "entry-cleanup-exp-null",
        "scn-cleanup-exp-null",
    )
    .await;

    db.touch_location_downloaded("loc-cleanup-exp-old", 1_000)
        .await
        .unwrap();
    db.touch_location_downloaded("loc-cleanup-exp-fresh", 100_000)
        .await
        .unwrap();
    // loc-cleanup-exp-null is never touched — lastDownloadedAt stays NULL.

    let expired = db.find_expired_locations(cutoff, 100, 0).await.unwrap();
    let expired_ids: std::collections::HashSet<&str> =
        expired.iter().map(|l| l.id.as_str()).collect();

    assert!(
        expired_ids.contains("loc-cleanup-exp-old"),
        "lastDownloadedAt < cutoff must be expired"
    );
    assert!(
        !expired_ids.contains("loc-cleanup-exp-fresh"),
        "lastDownloadedAt > cutoff must NOT be expired"
    );
    assert!(
        !expired_ids.contains("loc-cleanup-exp-null"),
        "NULL lastDownloadedAt must NOT be expired (upstream parity: NULL < cutoff is unknown)"
    );
}

/// `find_orphan_locations` returns only rows that no `cache_entries`
/// row points at. Upstream `storage-locations.ts:22-36` uses the same
/// `NOT EXISTS (SELECT 1 FROM cache_entries WHERE locationId = sl.id)`
/// shape.
pub async fn find_orphan_locations_excludes_referenced_rows(db: &dyn Db) {
    seed_location_with_entry(
        db,
        "loc-cleanup-orphan-referenced",
        "folder-orphan-ref",
        "entry-cleanup-orphan-ref",
        "scn-cleanup-orphan-ref",
    )
    .await;
    seed_orphan_location(db, "loc-cleanup-orphan-bare", "folder-orphan-bare").await;

    let orphans = db.find_orphan_locations(100, 0).await.unwrap();
    let orphan_ids: std::collections::HashSet<&str> =
        orphans.iter().map(|l| l.id.as_str()).collect();

    assert!(
        orphan_ids.contains("loc-cleanup-orphan-bare"),
        "location with no cache_entries reference must be reported as orphan"
    );
    assert!(
        !orphan_ids.contains("loc-cleanup-orphan-referenced"),
        "location referenced by a cache_entries row must NOT be reported as orphan"
    );
}

/// Seeds a location used by `find_merged_with_parts` scenario, then
/// puts it into the requested merge state.
async fn seed_parts_loc(db: &dyn Db, name: &str, merged: bool, parts_deleted: bool) {
    seed_location_with_entry(
        db,
        &format!("loc-cleanup-parts-{name}"),
        &format!("folder-parts-{name}"),
        &format!("entry-cleanup-parts-{name}"),
        &format!("scn-cleanup-parts-{name}"),
    )
    .await;
    let loc_id = format!("loc-cleanup-parts-{name}");
    if merged {
        assert!(db.try_mark_merge_started(&loc_id, 100).await.unwrap());
        db.mark_merged(&loc_id, 200).await.unwrap();
    }
    if parts_deleted {
        let mut tx = db.begin().await.unwrap();
        tx.mark_parts_deleted(&loc_id, 300).await.unwrap();
        tx.commit().await.unwrap();
    }
}

/// `find_merged_with_parts` returns rows where the merge has completed
/// (`mergedAt IS NOT NULL`) but the parts folder hasn't yet been
/// reaped (`partsDeletedAt IS NULL`). Upstream `parts.ts:21-28` uses
/// the same shape. The other three flag combinations must be excluded.
pub async fn find_merged_with_parts_filters_on_merge_and_parts_flags(db: &dyn Db) {
    seed_parts_loc(db, "eligible", true, false).await;
    seed_parts_loc(db, "already", true, true).await;
    seed_parts_loc(db, "unmerged", false, false).await;
    seed_location_with_entry(
        db,
        "loc-cleanup-parts-claimed",
        "folder-parts-claimed",
        "entry-cleanup-parts-claimed",
        "scn-cleanup-parts-claimed",
    )
    .await;
    // Claimed but not merged: mergeStartedAt set, mergedAt NULL.
    assert!(
        db.try_mark_merge_started("loc-cleanup-parts-claimed", 100)
            .await
            .unwrap()
    );

    let eligible = db.find_merged_with_parts(100, 0).await.unwrap();
    let eligible_ids: std::collections::HashSet<&str> =
        eligible.iter().map(|l| l.id.as_str()).collect();

    assert!(
        eligible_ids.contains("loc-cleanup-parts-eligible"),
        "merged + parts not deleted must be reported"
    );
    assert!(
        !eligible_ids.contains("loc-cleanup-parts-already"),
        "merged + parts already deleted must NOT be reported"
    );
    assert!(
        !eligible_ids.contains("loc-cleanup-parts-unmerged"),
        "idle (never merged) must NOT be reported"
    );
    assert!(
        !eligible_ids.contains("loc-cleanup-parts-claimed"),
        "claimed but not merged must NOT be reported"
    );
}
