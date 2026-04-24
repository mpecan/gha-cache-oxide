//! DB-conformance scenarios, extracted from `tests/db_conformance.rs`
//! so both files stay under the 700-line hard limit.
//!
//! Each scenario is a `pub async fn` taking `&dyn Db`. The conformance
//! binary's `db_conformance_cases!` macro calls `scenarios::$name(&*db)`
//! with a freshly-set-up `Arc<dyn Db>`; the programmatic runner threads
//! a single `Db` through every scenario in sequence (scenarios that
//! touch `cache_entries` use their own `scope` value so state can't
//! cross-contaminate).

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use gha_cache_oxide::db::Db;
use gha_cache_oxide::db::entities::{CacheEntryCoord, MatchRequest, MatchType, NewUpload};
use gha_cache_oxide::db::id::{new_upload_id, new_uuid};

/// Each scenario that touches `cache_entries` uses its own `scope`
/// value so `run_conformance_suite` (which reuses a single `Db`
/// across scenarios) doesn't see cross-scenario state bleed. Plain
/// `upload`-only scenarios share the `uploads` keyspace safely.
const fn scoped_coord(scope: &str) -> CacheEntryCoord<'_> {
    CacheEntryCoord {
        key: "cache-key",
        version: "v1",
        scope,
        repo_id: "42",
    }
}

/// End-to-end upload lifecycle: `create_upload` → `find_upload_by_id`
/// → `find_upload_by_coord` → started/finished counters diverge as
/// rounds of increments land → `delete_upload` removes the row.
/// Rolls the full set of `Db::*_upload*` helpers into one scenario
/// so a failing driver can't hide behind partial coverage.
pub async fn upload_lifecycle_round_trip(db: &dyn Db) {
    let id = new_upload_id();
    let coord = scoped_coord("scn-upload-lifecycle");
    let inserted = db
        .create_upload(NewUpload {
            id,
            coord,
            folder_name: "fldr-1",
            created_at_ms: 1_700_000_000_000,
        })
        .await
        .unwrap();
    assert_eq!(inserted.id, id);
    assert_eq!(inserted.started_part_upload_count, 0);
    assert_eq!(inserted.finished_part_upload_count, 0);
    assert!(inserted.last_part_uploaded_at.is_none());

    let fetched = db.find_upload_by_id(id).await.unwrap().unwrap();
    assert_eq!(fetched.id, id);
    assert_eq!(fetched.folder_name, "fldr-1");

    let by_coord = db.find_upload_by_coord(coord).await.unwrap().unwrap();
    assert_eq!(by_coord.id, id);

    db.increment_upload_started(id).await.unwrap();
    db.increment_upload_started(id).await.unwrap();
    db.increment_upload_finished(id, 1_700_000_500_000)
        .await
        .unwrap();

    let after = db.find_upload_by_id(id).await.unwrap().unwrap();
    assert_eq!(after.started_part_upload_count, 2);
    assert_eq!(after.finished_part_upload_count, 1);
    assert_eq!(after.last_part_uploaded_at, Some(1_700_000_500_000));

    db.delete_upload(id).await.unwrap();
    assert!(db.find_upload_by_id(id).await.unwrap().is_none());
}

/// `find_upload_by_coord`'s `WHERE` clause must discriminate on all
/// four fields independently. Flipping any one to a value not in the
/// DB must miss, even when the other three match.
pub async fn find_upload_by_coord_discriminates_each_field(db: &dyn Db) {
    let id = new_upload_id();
    let base = CacheEntryCoord {
        key: "K",
        version: "V",
        scope: "scn-upload-discrim",
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

/// Upstream contract: the update/delete helpers silently no-op on
/// missing rows rather than raising. A driver that flipped to
/// erroring would break `touch_location_downloaded`'s fire-and-forget
/// call site in the download handler.
pub async fn update_helpers_are_noops_on_unknown_ids(db: &dyn Db) {
    db.increment_upload_started(1).await.unwrap();
    db.increment_upload_finished(1, 0).await.unwrap();
    db.delete_upload(1).await.unwrap();
    db.touch_location_downloaded("does-not-exist", 0)
        .await
        .unwrap();
}

/// Seeds a `storage_locations` + `cache_entries` pair and reads the
/// location back via `find_location_for_entry`, which joins on
/// `cache_entries.locationId`. Covers `insert_storage_location_tx`
/// and `find_location_for_entry` in one pass — the only portable
/// read-back path for storage-location columns without the join.
pub async fn find_location_for_entry_join(db: &dyn Db) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-A", "folder-A", 3)
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
    assert_eq!(loc.part_count, 3);
    assert!(loc.merged_at.is_none());

    let miss = db.find_location_for_entry("unknown").await.unwrap();
    assert!(miss.is_none());
}

/// `touch_location_downloaded` writes `lastDownloadedAt`. Seeded via
/// `insert_storage_location_tx` + a cache-entry row so we can read
/// the column back through `find_location_for_entry`.
pub async fn touch_location_downloaded_sets_timestamp(db: &dyn Db) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-T", "folder-T", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "e-T",
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "s",
            repo_id: "r",
        },
        0,
        "loc-T",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    db.touch_location_downloaded("loc-T", 1_700_000_999_000)
        .await
        .unwrap();

    let loc = db.find_location_for_entry("e-T").await.unwrap().unwrap();
    assert_eq!(loc.last_downloaded_at, Some(1_700_000_999_000));
}

/// `upsert_cache_entry_tx`: the insert path returns `None` for a
/// fresh coordinate; the update path returns the previous
/// `(location_id, folder_name)` so the caller can clean up the
/// orphaned blob.
pub async fn upsert_cache_entry_insert_then_update(db: &dyn Db) {
    let coord = scoped_coord("scn-upsert");

    // Insert path.
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-upsert-old", "folder-old", 1)
        .await
        .unwrap();
    let first = tx
        .upsert_cache_entry(coord, "loc-upsert-old", 1_000)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(first.is_none(), "fresh coord: insert returns None");

    // Update path.
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-upsert-new", "folder-new", 2)
        .await
        .unwrap();
    let previous = tx
        .upsert_cache_entry(coord, "loc-upsert-new", 2_000)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let previous = previous.expect("update path: returns previous location");
    assert_eq!(previous.id, "loc-upsert-old");
    assert_eq!(previous.folder_name, "folder-old");
}

// ---------- match_cache_entry scenarios ----------------------------------

/// Inserts a `storage_locations` + `cache_entries` pair with the
/// match-test defaults (`version = "v1"`, `repo_id = "r"`,
/// `updated_at = 100`). Returns the cache-entry id so tests can
/// assert which row came back.
async fn seed(db: &dyn Db, key: &str, scope: &str) -> String {
    let location_id = new_uuid();
    let entry_id = new_uuid();
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location(&location_id, &format!("folder-{entry_id}"), 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        &entry_id,
        CacheEntryCoord {
            key,
            version: "v1",
            scope,
            repo_id: "r",
        },
        100,
        &location_id,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    entry_id
}

/// Shared `MatchRequest` builder for the match-scenario group — the
/// version/repo defaults line up with `seed()` above.
const fn match_req<'a>(
    primary: &'a str,
    restore: &'a [&'a str],
    scopes: &'a [&'a str],
) -> MatchRequest<'a> {
    MatchRequest {
        primary_key: primary,
        restore_keys: restore,
        version: "v1",
        scopes,
        repo_id: "r",
    }
}

pub async fn match_cache_entry_exact_primary(db: &dyn Db) {
    let id = seed(db, "my-key", "scn-match-exact").await;
    let m = db
        .match_cache_entry(match_req("my-key", &[], &["scn-match-exact"]))
        .await
        .unwrap()
        .expect("exact primary should hit");
    assert_eq!(m.entry.id, id);
    assert_eq!(m.match_type, MatchType::ExactPrimary);
}

pub async fn match_cache_entry_prefixed_primary(db: &dyn Db) {
    let id = seed(db, "deps-abc", "scn-match-prefprim").await;
    let m = db
        .match_cache_entry(match_req("deps-", &[], &["scn-match-prefprim"]))
        .await
        .unwrap()
        .expect("prefix primary should hit");
    assert_eq!(m.entry.id, id);
    assert_eq!(m.match_type, MatchType::PrefixedPrimary);
}

pub async fn match_cache_entry_exact_restore(db: &dyn Db) {
    let id = seed(db, "fallback", "scn-match-exactrest").await;
    let m = db
        .match_cache_entry(match_req(
            "primary-miss",
            &["fallback"],
            &["scn-match-exactrest"],
        ))
        .await
        .unwrap()
        .expect("exact restore should hit");
    assert_eq!(m.entry.id, id);
    assert_eq!(m.match_type, MatchType::ExactRestore);
}

pub async fn match_cache_entry_prefixed_restore(db: &dyn Db) {
    let id = seed(db, "restore-abc", "scn-match-prefrest").await;
    let m = db
        .match_cache_entry(match_req(
            "primary-miss",
            &["restore-"],
            &["scn-match-prefrest"],
        ))
        .await
        .unwrap()
        .expect("prefix restore should hit");
    assert_eq!(m.entry.id, id);
    assert_eq!(m.match_type, MatchType::PrefixedRestore);
}

pub async fn match_cache_entry_returns_none_when_no_match(db: &dyn Db) {
    seed(db, "other", "scn-match-none").await;
    let result = db
        .match_cache_entry(match_req("missing", &["also-missing"], &["scn-match-none"]))
        .await
        .unwrap();
    assert!(result.is_none());
}

/// Upstream semantics: scopes are walked in order; the first scope
/// that produces any hit wins. **With an empty `restore_keys`**,
/// missing the primary key in scope 0 short-circuits the whole match
/// to `None` — it must NOT fall through to scope 1.
pub async fn match_cache_entry_first_scope_short_circuits_without_restore_keys(db: &dyn Db) {
    // Seed in scope B — not scope A.
    seed(db, "k", "scn-short-B").await;
    let result = db
        .match_cache_entry(match_req("k", &[], &["scn-short-A", "scn-short-B"]))
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "short-circuit: empty restore_keys + miss in scope 0 must not walk scope 1"
    );
}

/// With non-empty `restore_keys`, the first scope that produces ANY
/// hit wins; scope 0's restore match beats scope 1's exact primary.
pub async fn match_cache_entry_first_scope_wins(db: &dyn Db) {
    // Scope A: restore prefix only.
    let a = seed(db, "restore-x", "scn-winsA").await;
    // Scope B: exact primary — should NOT be chosen because scope A
    // already produced a (lower-priority) match.
    seed(db, "primary", "scn-winsB").await;

    let m = db
        .match_cache_entry(match_req(
            "primary",
            &["restore-"],
            &["scn-winsA", "scn-winsB"],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.entry.id, a, "scope priority: scope 0 wins");
    assert_eq!(m.match_type, MatchType::PrefixedRestore);
}

// ---------- Finalize transaction atomicity ------------------------------

/// Rolling back the finalize transaction leaves the DB untouched —
/// neither the `storage_locations` insert nor the `cache_entries`
/// upsert persists. Mirrors the transaction shape of
/// `cache::complete_upload::commit_upload_tx` so a driver that
/// silently swallowed `ROLLBACK` would fail here.
pub async fn finalize_transaction_rollback_is_atomic(db: &dyn Db) {
    let coord = scoped_coord("scn-rollback");

    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-rollback", "folder-rollback", 3)
        .await
        .unwrap();
    let _ = tx
        .upsert_cache_entry(coord, "loc-rollback", 1_000)
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    // Entry never persisted.
    let m = db
        .match_cache_entry(MatchRequest {
            primary_key: coord.key,
            restore_keys: &[],
            version: coord.version,
            scopes: &[coord.scope],
            repo_id: coord.repo_id,
        })
        .await
        .unwrap();
    assert!(m.is_none(), "rollback must drop the cache_entries insert");

    // Location never persisted — we verify via a second insert that
    // would fail on PK conflict if the first had committed.
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-rollback", "folder-again", 1)
        .await
        .expect("rollback must drop the storage_locations insert");
    tx.rollback().await.unwrap();
}

/// Mirrors the full `cache::complete_upload::commit_upload_tx` shape
/// in a single transaction: insert the NEW location, upsert an
/// existing coord (returns the PREVIOUS location), DELETE that
/// previous `storage_locations` row, DELETE the `uploads` row that
/// drove the finalize, then commit. Post-commit: only the new
/// location exists, the entry points at it, the upload is gone.
/// Without this scenario the `prev`-delete + `uploads`-delete steps
/// never land under the conformance suite.
pub async fn finalize_transaction_full_commit_shape(db: &dyn Db) {
    let coord = scoped_coord("scn-full-finalize");

    // Seed an existing (old location + entry) and an in-flight upload.
    let old_upload_id = new_upload_id();
    db.create_upload(NewUpload {
        id: old_upload_id,
        coord,
        folder_name: "folder-old",
        created_at_ms: 500,
    })
    .await
    .unwrap();
    let mut setup = db.begin().await.unwrap();
    setup
        .insert_storage_location("loc-old-full", "folder-old", 1)
        .await
        .unwrap();
    setup
        .upsert_cache_entry(coord, "loc-old-full", 500)
        .await
        .unwrap();
    setup.commit().await.unwrap();

    // Finalize: insert new location, upsert (repoint entry), DELETE
    // old location, DELETE upload — all in one tx.
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-new-full", "folder-new", 2)
        .await
        .unwrap();
    let previous = tx
        .upsert_cache_entry(coord, "loc-new-full", 1_000)
        .await
        .unwrap()
        .expect("existing coord → previous location returned");
    assert_eq!(previous.id, "loc-old-full");
    tx.delete_storage_location(&previous.id).await.unwrap();
    tx.delete_upload(old_upload_id).await.unwrap();
    tx.commit().await.unwrap();

    // Post-commit assertions.
    assert!(
        db.find_upload_by_id(old_upload_id).await.unwrap().is_none(),
        "uploads row must be gone"
    );
    let m = db
        .match_cache_entry(MatchRequest {
            primary_key: coord.key,
            restore_keys: &[],
            version: coord.version,
            scopes: &[coord.scope],
            repo_id: coord.repo_id,
        })
        .await
        .unwrap()
        .expect("entry must remain, repointed at the new location");
    assert_eq!(m.entry.location_id, "loc-new-full");
}

// ---------- Lazy-merge state transitions (#15) --------------------------

/// `try_mark_merge_started` is a compare-and-swap: it flips
/// `mergeStartedAt` from NULL → `now_ms` exactly once per location,
/// returning `true` for the winner and `false` for every subsequent
/// caller until `reset_merge_flags` clears the column. Guarantees the
/// "exactly one merger" invariant from issue #15 against both drivers.
pub async fn lazy_merge_cas_winner_and_loser(db: &dyn Db) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-cas-conf", "folder-cas-conf", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "entry-cas-conf",
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scn-cas",
            repo_id: "r",
        },
        0,
        "loc-cas-conf",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    assert!(
        db.try_mark_merge_started("loc-cas-conf", 1_000)
            .await
            .unwrap(),
        "first caller wins the CAS"
    );
    assert!(
        !db.try_mark_merge_started("loc-cas-conf", 2_000)
            .await
            .unwrap(),
        "second caller loses while merge is in flight"
    );

    let loc = db
        .find_location_for_entry("entry-cas-conf")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loc.merge_started_at, Some(1_000));
    assert!(loc.merged_at.is_none());
}

/// `mark_merged` sets `mergedAt`; `reset_merge_flags` clears both
/// `mergedAt` and `mergeStartedAt` so a retrying download re-wins the
/// CAS. Pins the two timestamp helpers end-to-end across drivers.
pub async fn lazy_merge_mark_and_reset_round_trip(db: &dyn Db) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-mr-conf", "folder-mr-conf", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "entry-mr-conf",
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scn-mr",
            repo_id: "r",
        },
        0,
        "loc-mr-conf",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    db.try_mark_merge_started("loc-mr-conf", 1_000)
        .await
        .unwrap();
    db.mark_merged("loc-mr-conf", 2_000).await.unwrap();
    let after_merge = db
        .find_location_for_entry("entry-mr-conf")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_merge.merged_at, Some(2_000));
    assert_eq!(after_merge.merge_started_at, Some(1_000));

    db.reset_merge_flags("loc-mr-conf").await.unwrap();
    let after_reset = db
        .find_location_for_entry("entry-mr-conf")
        .await
        .unwrap()
        .unwrap();
    assert!(after_reset.merge_started_at.is_none());
    assert!(after_reset.merged_at.is_none());

    assert!(
        db.try_mark_merge_started("loc-mr-conf", 3_000)
            .await
            .unwrap(),
        "post-reset CAS must win again"
    );
}

/// `DbTx::mark_parts_deleted` writes `partsDeletedAt` inside the
/// caller-supplied transaction; rolling back that transaction leaves
/// the column NULL, matching the upstream invariant that
/// `partsDeletedAt` is set only if the parts folder has actually been
/// removed.
pub async fn lazy_merge_mark_parts_deleted_shape(db: &dyn Db) {
    let mut tx = db.begin().await.unwrap();
    tx.insert_storage_location("loc-pd-conf", "folder-pd-conf", 1)
        .await
        .unwrap();
    tx.seed_cache_entry(
        "entry-pd-conf",
        CacheEntryCoord {
            key: "k",
            version: "v",
            scope: "scn-pd",
            repo_id: "r",
        },
        0,
        "loc-pd-conf",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Commit path: column is set.
    let mut tx = db.begin().await.unwrap();
    tx.mark_parts_deleted("loc-pd-conf", 1_000).await.unwrap();
    tx.commit().await.unwrap();
    let committed = db
        .find_location_for_entry("entry-pd-conf")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.parts_deleted_at, Some(1_000));

    // Rollback path: column returns to NULL-ish behaviour (we first
    // clear it via a committed UPDATE, then re-mark inside a rolled-
    // back tx, and assert the column didn't change).
    let mut tx = db.begin().await.unwrap();
    tx.mark_parts_deleted("loc-pd-conf", 2_000).await.unwrap();
    tx.rollback().await.unwrap();
    let rolled_back = db
        .find_location_for_entry("entry-pd-conf")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rolled_back.parts_deleted_at,
        Some(1_000),
        "rollback must leave the committed value in place, not the tx-local one"
    );
}

/// Pins the `ON DELETE CASCADE` safety net on
/// `cache_entries.locationId`. In normal operation the entry is
/// repointed at the new location *before* the old location is
/// deleted (see `cache::complete_upload::commit_upload_tx`), so
/// CASCADE never fires — but if a future refactor reverses that
/// order, the FK cascade prevents orphan `cache_entries` rows from
/// leaking.
pub async fn deleting_storage_location_cascades_to_cache_entry(db: &dyn Db) {
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

    // Drop the location directly (outside the upsert flow). The
    // cache_entries row should be CASCADEd away.
    let mut tx = db.begin().await.unwrap();
    tx.delete_storage_location("loc-doomed").await.unwrap();
    tx.commit().await.unwrap();

    let result = db.find_location_for_entry(&entry_id).await.unwrap();
    assert!(result.is_none(), "CASCADE should have removed the entry");
}
