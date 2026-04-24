//! Driver-agnostic DB conformance suite.
//!
//! Same pattern as `tests/storage_conformance.rs` (#11): scenarios live
//! under [`scenarios`] as `pub async fn` taking `&Db`; the
//! [`db_conformance_cases!`] macro expands to one `#[tokio::test]` per
//! scenario inside a named sub-module, so failures identify the exact
//! case. Adding a scenario is a three-line change in this one file.
//!
//! # Why `&Db`, not `sqlx::Pool<Any>`
//!
//! The issue's notes acknowledge that `AnyPool` has known caveats —
//! bind-parameter placeholder differences (`?` vs `$1`), subtly
//! divergent column-type mappings, and a smaller subset of supported
//! features than the per-dialect pools. The suite instead accepts
//! `&Db`, treating `Db` itself as the thin trait-like wrapper: its
//! public method surface is dialect-neutral even though its current
//! implementation wraps a `SqlitePool`. When Postgres lands (#14), the
//! same suite drives `postgres_setup()` without any scenario edits.
//!
//! # Migrations
//!
//! Each driver ships its own migration directory under
//! `migrations/<driver>/` (today: `migrations/sqlite/`; future:
//! `migrations/postgres/`). The setup function for a driver is
//! responsible for creating the pool, running its migrations, and
//! returning a ready-to-use `Db`. The runtime query layer is
//! dialect-agnostic (standard SQL, `?` placeholders that sqlx
//! translates per-driver), so the SAME scenarios run against whichever
//! driver the setup wires up.
//!
//! # Transaction helpers
//!
//! `insert_storage_location_tx` / `upsert_cache_entry_tx` take
//! `Transaction<'_, Sqlite>` today. When `Db` becomes multi-driver in
//! #14, those signatures will widen; the scenarios here already fetch
//! their transaction via `db.begin()` so nothing in this file reaches
//! past the `Db` surface for a SQLite-specific type directly.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::too_long_first_doc_paragraph
)]

use gha_cache_oxide::db::Db;

// ------------------------------------------------------------------------
// Setup plumbing
// ------------------------------------------------------------------------

/// Tuple returned by a driver's setup: `Db` + RAII guard. The guard is
/// `Box<dyn Send>` so the macro stays driver-agnostic — `SQLite`'s
/// in-memory pool needs no extra handle today; Postgres may hand back a
/// container-scoped cleanup future tomorrow without changing the macro
/// body.
pub type SetupResult = (Db, Box<dyn Send>);

async fn sqlite_setup() -> SetupResult {
    let db = Db::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    (db, Box::new(()))
}

// ------------------------------------------------------------------------
// Scenarios — each tests one trait-level contract over `&Db`.
// ------------------------------------------------------------------------

pub mod scenarios {
    use gha_cache_oxide::db::Db;
    use gha_cache_oxide::db::entities::{CacheEntryCoord, MatchRequest, MatchType, NewUpload};
    use gha_cache_oxide::db::id::{new_upload_id, new_uuid};
    use gha_cache_oxide::db::queries::{insert_storage_location_tx, upsert_cache_entry_tx};

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
    pub async fn upload_lifecycle_round_trip(db: &Db) {
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
    /// four fields independently. Flipping any one to a value not in
    /// the DB must miss, even when the other three match.
    pub async fn find_upload_by_coord_discriminates_each_field(db: &Db) {
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

    /// Upstream contract: the update/delete helpers silently no-op on
    /// missing rows rather than raising. A driver that flipped to
    /// erroring would break `touch_location_downloaded`'s
    /// fire-and-forget call site in the download handler.
    pub async fn update_helpers_are_noops_on_unknown_ids(db: &Db) {
        db.increment_upload_started(1).await.unwrap();
        db.increment_upload_finished(1, 0).await.unwrap();
        db.delete_upload(1).await.unwrap();
        db.touch_location_downloaded("does-not-exist", 0)
            .await
            .unwrap();
    }

    /// Seeds a `storage_locations` + `cache_entries` pair and reads
    /// the location back via `find_location_for_entry`, which joins on
    /// `cache_entries.locationId`. Covers `insert_storage_location_tx`
    /// and `find_location_for_entry` in one pass — the only portable
    /// read-back path for storage-location columns without the join.
    pub async fn find_location_for_entry_join(db: &Db) {
        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, "loc-A", "folder-A", 3)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("entry-A")
        .bind("k")
        .bind("v")
        .bind("s")
        .bind("r")
        .bind(1_700_000_000_000_i64)
        .bind("loc-A")
        .execute(&mut *tx)
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

    /// `touch_location_downloaded` writes `lastDownloadedAt`. Seeded
    /// via `insert_storage_location_tx` + a cache-entry row so we can
    /// read the column back through `find_location_for_entry`.
    pub async fn touch_location_downloaded_sets_timestamp(db: &Db) {
        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, "loc-T", "folder-T", 1)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
             VALUES ('e-T', 'k', 'v', 's', 'r', 0, 'loc-T')",
        )
        .execute(&mut *tx)
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
    pub async fn upsert_cache_entry_insert_then_update(db: &Db) {
        let coord = scoped_coord("scn-upsert");

        // Insert path.
        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, "loc-upsert-old", "folder-old", 1)
            .await
            .unwrap();
        let first = upsert_cache_entry_tx(&mut tx, coord, "loc-upsert-old", 1_000)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(first.is_none(), "fresh coord: insert returns None");

        // Update path.
        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, "loc-upsert-new", "folder-new", 2)
            .await
            .unwrap();
        let previous = upsert_cache_entry_tx(&mut tx, coord, "loc-upsert-new", 2_000)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let previous = previous.expect("update path: returns previous location");
        assert_eq!(previous.id, "loc-upsert-old");
        assert_eq!(previous.folder_name, "folder-old");
    }

    // ------- match_cache_entry scenarios --------------------------------

    /// Inserts a `storage_locations` + `cache_entries` pair with the
    /// match-test defaults (`version = "v1"`, `repo_id = "r"`,
    /// `updated_at = 100`). Returns the cache-entry id so tests can
    /// assert which row came back.
    async fn seed(db: &Db, key: &str, scope: &str) -> String {
        let location_id = new_uuid();
        let entry_id = new_uuid();
        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, &location_id, &format!("folder-{entry_id}"), 1)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
             VALUES (?, ?, 'v1', ?, 'r', 100, ?)",
        )
        .bind(&entry_id)
        .bind(key)
        .bind(scope)
        .bind(&location_id)
        .execute(&mut *tx)
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

    pub async fn match_cache_entry_exact_primary(db: &Db) {
        let id = seed(db, "my-key", "scn-match-exact").await;
        let m = db
            .match_cache_entry(match_req("my-key", &[], &["scn-match-exact"]))
            .await
            .unwrap()
            .expect("exact primary should hit");
        assert_eq!(m.entry.id, id);
        assert_eq!(m.match_type, MatchType::ExactPrimary);
    }

    pub async fn match_cache_entry_prefixed_primary(db: &Db) {
        let id = seed(db, "deps-abc", "scn-match-prefprim").await;
        let m = db
            .match_cache_entry(match_req("deps-", &[], &["scn-match-prefprim"]))
            .await
            .unwrap()
            .expect("prefix primary should hit");
        assert_eq!(m.entry.id, id);
        assert_eq!(m.match_type, MatchType::PrefixedPrimary);
    }

    pub async fn match_cache_entry_exact_restore(db: &Db) {
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

    pub async fn match_cache_entry_prefixed_restore(db: &Db) {
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

    pub async fn match_cache_entry_returns_none_when_no_match(db: &Db) {
        seed(db, "other", "scn-match-none").await;
        let result = db
            .match_cache_entry(match_req("missing", &["also-missing"], &["scn-match-none"]))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    /// Upstream semantics: scopes are walked in order; the first scope
    /// that produces any hit wins. **With an empty `restore_keys`**,
    /// missing the primary key in scope 0 short-circuits the whole
    /// match to `None` — it must NOT fall through to scope 1.
    pub async fn match_cache_entry_first_scope_short_circuits_without_restore_keys(db: &Db) {
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
    pub async fn match_cache_entry_first_scope_wins(db: &Db) {
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

    // ------- Finalize transaction atomicity ------------------------------

    /// Rolling back the finalize transaction leaves the DB untouched —
    /// neither the `storage_locations` insert nor the `cache_entries`
    /// upsert persists. Mirrors the transaction shape of
    /// `cache::complete_upload::commit_upload_tx` so a driver that
    /// silently swallowed `ROLLBACK` would fail here.
    pub async fn finalize_transaction_rollback_is_atomic(db: &Db) {
        let coord = scoped_coord("scn-rollback");

        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, "loc-rollback", "folder-rollback", 3)
            .await
            .unwrap();
        let _ = upsert_cache_entry_tx(&mut tx, coord, "loc-rollback", 1_000)
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
        insert_storage_location_tx(&mut tx, "loc-rollback", "folder-again", 1)
            .await
            .expect("rollback must drop the storage_locations insert");
        tx.rollback().await.unwrap();
    }

    /// Committing the finalize transaction makes both writes visible
    /// together — used to pin the happy-path atomicity counterpart to
    /// the rollback scenario.
    pub async fn finalize_transaction_commit_is_atomic(db: &Db) {
        let coord = scoped_coord("scn-commit");

        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, "loc-commit", "folder-commit", 2)
            .await
            .unwrap();
        let previous = upsert_cache_entry_tx(&mut tx, coord, "loc-commit", 2_000)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(previous.is_none(), "fresh coord: no previous location");

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
            .expect("commit must make the new entry matchable");
        assert_eq!(m.match_type, MatchType::ExactPrimary);
    }

    /// Pins the `ON DELETE CASCADE` safety net on
    /// `cache_entries.locationId`. In normal operation the entry is
    /// repointed at the new location *before* the old location is
    /// deleted (see `cache::complete_upload::commit_upload_tx`), so
    /// CASCADE never fires — but if a future refactor reverses that
    /// order, the FK cascade prevents orphan `cache_entries` rows from
    /// leaking.
    pub async fn deleting_storage_location_cascades_to_cache_entry(db: &Db) {
        let mut tx = db.begin().await.unwrap();
        insert_storage_location_tx(&mut tx, "loc-doomed", "folder-doomed", 1)
            .await
            .unwrap();
        let entry_id = new_uuid();
        sqlx::query(
            "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
             VALUES (?, 'k', 'v', 's', 'r', 0, 'loc-doomed')",
        )
        .bind(&entry_id)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();

        // Drop the location directly (outside the upsert flow). The
        // cache_entries row should be CASCADEd away.
        let mut tx = db.begin().await.unwrap();
        sqlx::query("DELETE FROM storage_locations WHERE id = 'loc-doomed'")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let result = db.find_location_for_entry(&entry_id).await.unwrap();
        assert!(result.is_none(), "CASCADE should have removed the entry");
    }
}

// ------------------------------------------------------------------------
// Programmatic runner
// ------------------------------------------------------------------------

/// Sequential runner — exercises every scenario against the same fresh
/// `Db`. Each scenario owns its own keyspace (unique UUIDs, unique
/// coords) so they don't collide.
///
/// # Panics
/// Any failing scenario panics with the scenario-level assertion.
pub async fn run_conformance_suite(db: &Db) {
    scenarios::upload_lifecycle_round_trip(db).await;
    scenarios::find_upload_by_coord_discriminates_each_field(db).await;
    scenarios::update_helpers_are_noops_on_unknown_ids(db).await;
    scenarios::find_location_for_entry_join(db).await;
    scenarios::touch_location_downloaded_sets_timestamp(db).await;
    scenarios::upsert_cache_entry_insert_then_update(db).await;
    scenarios::match_cache_entry_exact_primary(db).await;
    scenarios::match_cache_entry_prefixed_primary(db).await;
    scenarios::match_cache_entry_exact_restore(db).await;
    scenarios::match_cache_entry_prefixed_restore(db).await;
    scenarios::match_cache_entry_returns_none_when_no_match(db).await;
    scenarios::match_cache_entry_first_scope_short_circuits_without_restore_keys(db).await;
    scenarios::match_cache_entry_first_scope_wins(db).await;
    scenarios::finalize_transaction_rollback_is_atomic(db).await;
    scenarios::finalize_transaction_commit_is_atomic(db).await;
    scenarios::deleting_storage_location_cascades_to_cache_entry(db).await;
}

// ------------------------------------------------------------------------
// db_conformance_cases! — one named #[tokio::test] per scenario inside
// a named sub-module. Plain arm runs by default; `ignore: "..."` arm
// gates every generated test on `--ignored` (reserved for drivers
// needing external services, e.g. the Postgres setup when #14 lands).
// Pattern mirrors tests/storage_conformance.rs#storage_conformance_cases!.
// ------------------------------------------------------------------------

macro_rules! db_conformance_cases {
    ($mod_name:ident, $setup:ident, $($scenario:ident),+ $(,)?) => {
        mod $mod_name {
            use super::{scenarios, $setup};
            $(
                #[tokio::test]
                async fn $scenario() {
                    let (db, _guard) = $setup().await;
                    scenarios::$scenario(&db).await;
                }
            )+
        }
    };
    ($mod_name:ident, $setup:ident, ignore: $reason:literal, $($scenario:ident),+ $(,)?) => {
        mod $mod_name {
            use super::{scenarios, $setup};
            $(
                #[ignore = $reason]
                #[tokio::test]
                async fn $scenario() {
                    let (db, _guard) = $setup().await;
                    scenarios::$scenario(&db).await;
                }
            )+
        }
    };
}

db_conformance_cases!(
    sqlite,
    sqlite_setup,
    upload_lifecycle_round_trip,
    find_upload_by_coord_discriminates_each_field,
    update_helpers_are_noops_on_unknown_ids,
    find_location_for_entry_join,
    touch_location_downloaded_sets_timestamp,
    upsert_cache_entry_insert_then_update,
    match_cache_entry_exact_primary,
    match_cache_entry_prefixed_primary,
    match_cache_entry_exact_restore,
    match_cache_entry_prefixed_restore,
    match_cache_entry_returns_none_when_no_match,
    match_cache_entry_first_scope_short_circuits_without_restore_keys,
    match_cache_entry_first_scope_wins,
    finalize_transaction_rollback_is_atomic,
    finalize_transaction_commit_is_atomic,
    deleting_storage_location_cascades_to_cache_entry,
);

/// Smoke test for the programmatic runner alongside the macro-generated
/// tests — exercises both entry points on every `cargo test`.
#[tokio::test]
async fn runner_executes_full_suite_against_sqlite() {
    let (db, _guard) = sqlite_setup().await;
    run_conformance_suite(&db).await;
}
