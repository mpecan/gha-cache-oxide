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
//! sqlx's `Any` pool passes query strings through verbatim — it
//! doesn't translate `?` into `$N`, and some types are narrower than
//! the per-dialect pool offers. The suite instead accepts `&Db`: `Db`
//! is an enum over `SqlitePool` / `PgPool`, its public method surface
//! is dialect-neutral, and every query dispatches at the `impl Db`
//! level with the adjacent SQL literal the only per-arm difference.
//!
//! # Migrations
//!
//! Each driver ships its own migration directory under
//! `migrations/<driver>/` (`migrations/sqlite/` + `migrations/postgres/`).
//! The setup function for a driver is responsible for creating the
//! pool, running its migrations, and returning a ready-to-use `Db`.
//! The scenarios here are agnostic: whichever driver the setup wires
//! up, the same 16 scenarios run against it.
//!
//! # Transaction helpers
//!
//! The `insert_storage_location_tx`, `upsert_cache_entry_tx`,
//! `seed_cache_entry_tx`, `delete_*_tx` helpers live in
//! `crate::db::tx` and take `&mut DbTx<'_>` — the runtime-enum tx
//! handle. Scenarios call `db.begin().await` and thread the returned
//! `DbTx` through them, so nothing in the scenario bodies reaches
//! past the enum API.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::too_long_first_doc_paragraph
)]

use std::sync::Arc;

use gha_cache_oxide::db::{Db, MysqlDb, PostgresDb, SqliteDb};

// ------------------------------------------------------------------------
// Setup plumbing
// ------------------------------------------------------------------------

/// Tuple returned by a driver's setup: `Arc<dyn Db>` + RAII guard. The
/// guard is `Box<dyn Send>` so the macro stays driver-agnostic —
/// `SQLite`'s in-memory pool needs no extra handle today; Postgres
/// hands back a schema-cleanup `SchemaGuard` that drops the per-test
/// schema on scope exit.
pub type SetupResult = (Arc<dyn Db>, Box<dyn Send>);

async fn sqlite_setup() -> SetupResult {
    let db = SqliteDb::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    (Arc::new(db), Box::new(()))
}

/// RAII guard that drops the per-test Postgres schema on scope exit.
/// A fresh thread-owned runtime spins up a one-shot pool so the cleanup
/// isn't entangled with the per-test pool's lifecycle.
struct SchemaGuard {
    url: String,
    schema: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        use sqlx::Executor;
        use sqlx::postgres::PgPoolOptions;
        let url = self.url.clone();
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Runtime::new() else {
                return;
            };
            rt.block_on(async move {
                if let Ok(pool) = PgPoolOptions::new().max_connections(1).connect(&url).await {
                    let _ = pool
                        .execute(format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE").as_str())
                        .await;
                    pool.close().await;
                }
            });
        })
        .join();
    }
}

/// Postgres setup for the conformance suite. Reads `DATABASE_URL` and
/// stamps out a per-test isolation schema (`test_<uuid>`) so parallel
/// scenarios sharing one bootstrapped database don't clobber each
/// other's tables. The guard drops the schema on scope exit — safe for
/// repeated `cargo test` runs against a long-lived container.
///
/// # Required env
/// - `DATABASE_URL` — libpq URL, e.g.
///   `postgres://postgres:postgres@localhost:5432/gha_cache_test`.
///
/// # Panics
/// When `DATABASE_URL` is unset. Callers are `#[ignore]`'d so default
/// `cargo test` never reaches this path.
async fn postgres_setup() -> SetupResult {
    use sqlx::Executor;
    use sqlx::postgres::PgPoolOptions;

    let base_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set for Postgres conformance; run `cargo test -- --ignored`");
    let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

    // Bootstrap a bare pool to create the schema and wire up search_path,
    // then open a second pool scoped to that search_path for the actual
    // `Db`. Two pools look heavy but it's the simplest way to inject
    // the schema without mutating the base URL.
    let bootstrap = PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    bootstrap
        .execute(format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"").as_str())
        .await
        .unwrap();
    bootstrap.close().await;

    // Append (or merge with existing) `options=-csearch_path=<schema>`
    // so sqlx::migrate! applies migrations inside the test schema.
    let url = if base_url.contains('?') {
        format!("{base_url}&options=-c%20search_path%3D{schema}")
    } else {
        format!("{base_url}?options=-c%20search_path%3D{schema}")
    };
    let db = PostgresDb::connect(&url).await.unwrap();
    db.migrate().await.unwrap();

    (
        Arc::new(db),
        Box::new(SchemaGuard {
            url: base_url,
            schema,
        }),
    )
}

/// `MySQL` setup for the conformance suite. Reads `DATABASE_URL` and
/// stamps out a per-test database (`test_<uuid>`) so parallel scenarios
/// sharing one bootstrapped server don't clobber each other's tables.
/// `MySQL` has no per-pool `search_path` like Postgres; we instead create
/// a dedicated database, reconnect to it, and `DROP DATABASE` on guard
/// drop.
///
/// # Required env
/// - `DATABASE_URL` — `MySQL` URL, e.g.
///   `mysql://root:mysql@127.0.0.1:3306/gha_cache_bootstrap`.
///   The path component is the bootstrap database the per-test
///   `CREATE DATABASE` runs against; it is rewritten to point at the
///   per-test database for the actual `Db`.
///
/// # Panics
/// When `DATABASE_URL` is unset. Callers are `#[ignore]`'d so default
/// `cargo test` never reaches this path.
async fn mysql_setup() -> SetupResult {
    use sqlx::Executor;
    use sqlx::mysql::MySqlPoolOptions;

    let base_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set for MySQL conformance; run `cargo test -- --ignored`");
    let database = format!("test_{}", uuid::Uuid::new_v4().simple());

    let bootstrap = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    bootstrap
        .execute(format!("CREATE DATABASE `{database}`").as_str())
        .await
        .unwrap();
    bootstrap.close().await;

    // Re-point the URL's path component at the per-test database. The
    // bootstrap URL ends with `/<bootstrap_db>(?...)?`; we replace the
    // path between the host and any query string.
    let url = swap_db_in_mysql_url(&base_url, &database);
    let db = MysqlDb::connect(&url).await.unwrap();
    db.migrate().await.unwrap();

    (
        Arc::new(db),
        Box::new(MysqlDbGuard {
            url: base_url,
            database,
        }),
    )
}

/// RAII guard that drops the per-test `MySQL` database on scope exit.
/// Mirrors `SchemaGuard` for Postgres — a fresh thread-owned runtime
/// runs a one-shot pool so cleanup isn't entangled with the per-test
/// pool's lifecycle.
struct MysqlDbGuard {
    url: String,
    database: String,
}

impl Drop for MysqlDbGuard {
    fn drop(&mut self) {
        use sqlx::Executor;
        use sqlx::mysql::MySqlPoolOptions;
        let url = self.url.clone();
        let database = self.database.clone();
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Runtime::new() else {
                return;
            };
            rt.block_on(async move {
                if let Ok(pool) = MySqlPoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await
                {
                    let _ = pool
                        .execute(format!("DROP DATABASE IF EXISTS `{database}`").as_str())
                        .await;
                    pool.close().await;
                }
            });
        })
        .join();
    }
}

/// Replaces the path-component database name in a `mysql://` URL with
/// `database`, preserving any query string. Specifically: between the
/// host's `/` and the next `?` (or end of string) is overwritten.
///
/// Used by [`mysql_setup`] to retarget a bootstrap URL at a per-test
/// database after `CREATE DATABASE`.
fn swap_db_in_mysql_url(base: &str, database: &str) -> String {
    let after_scheme_idx = base.find("://").map_or(0, |i| i + 3);
    let path_start = base[after_scheme_idx..]
        .find('/')
        .map(|i| after_scheme_idx + i);
    path_start.map_or_else(
        || format!("{base}/{database}"),
        |start| {
            let rest = &base[start + 1..];
            let query = rest.find('?').map_or("", |i| &rest[i..]);
            format!("{}/{database}{query}", &base[..start])
        },
    )
}

// Scenarios live in a directory submodule so neither file crosses the
// 700-line hard limit.
#[path = "db_conformance_scenarios/mod.rs"]
mod scenarios;

// ------------------------------------------------------------------------
// Programmatic runner
// ------------------------------------------------------------------------

/// Sequential runner — exercises every scenario against the same fresh
/// `Db`. Each scenario owns its own keyspace (unique UUIDs, unique
/// coords) so they don't collide.
///
/// # Panics
/// Any failing scenario panics with the scenario-level assertion.
pub async fn run_conformance_suite(db: &dyn Db) {
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
    scenarios::finalize_transaction_full_commit_shape(db).await;
    scenarios::deleting_storage_location_cascades_to_cache_entry(db).await;
    scenarios::lazy_merge_cas_winner_and_loser(db).await;
    scenarios::lazy_merge_mark_and_reset_round_trip(db).await;
    scenarios::lazy_merge_mark_parts_deleted_shape(db).await;
    scenarios::clear_stale_merge_claims_clears_old_claims(db).await;
    scenarios::clear_stale_merge_claims_leaves_fresh_claims(db).await;
    scenarios::clear_stale_merge_claims_ignores_completed_and_idle_rows(db).await;
    scenarios::get_merge_state_covers_all_four_combinations(db).await;
    scenarios::find_stale_uploads_filters_on_both_predicates(db).await;
    scenarios::delete_upload_if_stale_re_checks_predicate(db).await;
    scenarios::find_expired_locations_respects_cutoff(db).await;
    scenarios::find_expired_locations_excludes_null_last_downloaded_at(db).await;
    scenarios::find_orphan_locations_excludes_referenced_rows(db).await;
    scenarios::find_merged_with_parts_filters_on_merge_and_parts_flags(db).await;
    scenarios::list_cache_entries_no_filter_paginates(db).await;
    scenarios::list_cache_entries_filters_by_scope_and_repo_id(db).await;
    scenarios::list_storage_locations_paginates(db).await;
    scenarios::find_cache_entry_by_id_returns_row_or_none(db).await;
    scenarios::find_storage_location_by_id_returns_row_or_none(db).await;
    scenarios::delete_cache_entries_by_filter_narrows_and_counts(db).await;
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
                    scenarios::$scenario(&*db).await;
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
                    scenarios::$scenario(&*db).await;
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
    finalize_transaction_full_commit_shape,
    deleting_storage_location_cascades_to_cache_entry,
    lazy_merge_cas_winner_and_loser,
    lazy_merge_mark_and_reset_round_trip,
    lazy_merge_mark_parts_deleted_shape,
    clear_stale_merge_claims_clears_old_claims,
    clear_stale_merge_claims_leaves_fresh_claims,
    clear_stale_merge_claims_ignores_completed_and_idle_rows,
    get_merge_state_covers_all_four_combinations,
    find_stale_uploads_filters_on_both_predicates,
    delete_upload_if_stale_re_checks_predicate,
    find_expired_locations_respects_cutoff,
    find_expired_locations_excludes_null_last_downloaded_at,
    find_orphan_locations_excludes_referenced_rows,
    find_merged_with_parts_filters_on_merge_and_parts_flags,
    list_cache_entries_no_filter_paginates,
    list_cache_entries_filters_by_scope_and_repo_id,
    list_storage_locations_paginates,
    find_cache_entry_by_id_returns_row_or_none,
    find_storage_location_by_id_returns_row_or_none,
    delete_cache_entries_by_filter_narrows_and_counts,
);

db_conformance_cases!(
    postgres,
    postgres_setup,
    ignore: "requires DATABASE_URL + running Postgres; `cargo test -- --ignored`",
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
    finalize_transaction_full_commit_shape,
    deleting_storage_location_cascades_to_cache_entry,
    lazy_merge_cas_winner_and_loser,
    lazy_merge_mark_and_reset_round_trip,
    lazy_merge_mark_parts_deleted_shape,
    clear_stale_merge_claims_clears_old_claims,
    clear_stale_merge_claims_leaves_fresh_claims,
    clear_stale_merge_claims_ignores_completed_and_idle_rows,
    get_merge_state_covers_all_four_combinations,
    find_stale_uploads_filters_on_both_predicates,
    delete_upload_if_stale_re_checks_predicate,
    find_expired_locations_respects_cutoff,
    find_expired_locations_excludes_null_last_downloaded_at,
    find_orphan_locations_excludes_referenced_rows,
    find_merged_with_parts_filters_on_merge_and_parts_flags,
    list_cache_entries_no_filter_paginates,
    list_cache_entries_filters_by_scope_and_repo_id,
    list_storage_locations_paginates,
    find_cache_entry_by_id_returns_row_or_none,
    find_storage_location_by_id_returns_row_or_none,
    delete_cache_entries_by_filter_narrows_and_counts,
);

db_conformance_cases!(
    mysql,
    mysql_setup,
    ignore: "requires DATABASE_URL + running MySQL; `cargo test -- --ignored`",
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
    finalize_transaction_full_commit_shape,
    deleting_storage_location_cascades_to_cache_entry,
    lazy_merge_cas_winner_and_loser,
    lazy_merge_mark_and_reset_round_trip,
    lazy_merge_mark_parts_deleted_shape,
    clear_stale_merge_claims_clears_old_claims,
    clear_stale_merge_claims_leaves_fresh_claims,
    clear_stale_merge_claims_ignores_completed_and_idle_rows,
    get_merge_state_covers_all_four_combinations,
    find_stale_uploads_filters_on_both_predicates,
    delete_upload_if_stale_re_checks_predicate,
    find_expired_locations_respects_cutoff,
    find_expired_locations_excludes_null_last_downloaded_at,
    find_orphan_locations_excludes_referenced_rows,
    find_merged_with_parts_filters_on_merge_and_parts_flags,
    list_cache_entries_no_filter_paginates,
    list_cache_entries_filters_by_scope_and_repo_id,
    list_storage_locations_paginates,
    find_cache_entry_by_id_returns_row_or_none,
    find_storage_location_by_id_returns_row_or_none,
    delete_cache_entries_by_filter_narrows_and_counts,
);

/// Smoke test for the programmatic runner — `SQLite` entry point always
/// runs on `cargo test`; Postgres / `MySQL` variants are `#[ignore]`'d
/// alongside their macro-generated peers.
#[tokio::test]
async fn runner_executes_full_suite_against_sqlite() {
    let (db, _guard) = sqlite_setup().await;
    run_conformance_suite(&*db).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL + running Postgres; `cargo test -- --ignored`"]
async fn runner_executes_full_suite_against_postgres() {
    let (db, _guard) = postgres_setup().await;
    run_conformance_suite(&*db).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL + running MySQL; `cargo test -- --ignored`"]
async fn runner_executes_full_suite_against_mysql() {
    let (db, _guard) = mysql_setup().await;
    run_conformance_suite(&*db).await;
}

// ------------------------------------------------------------------------
// `swap_db_in_mysql_url` — direct unit tests for the URL helper used by
// `mysql_setup`. The helper has subtle path-vs-query handling; integration
// tests only exercise the plain-URL case (the bootstrap URL CI sets), so
// these pin the rare shapes operators feed in their own fixtures.
// ------------------------------------------------------------------------

#[test]
fn swap_db_in_mysql_url_plain_path() {
    assert_eq!(
        swap_db_in_mysql_url("mysql://u:p@h:3306/boot", "test_42"),
        "mysql://u:p@h:3306/test_42"
    );
}

#[test]
fn swap_db_in_mysql_url_preserves_query_string() {
    assert_eq!(
        swap_db_in_mysql_url("mysql://u:p@h:3306/boot?ssl-mode=REQUIRED", "test_42"),
        "mysql://u:p@h:3306/test_42?ssl-mode=REQUIRED"
    );
}

#[test]
fn swap_db_in_mysql_url_no_path_appends() {
    // No `/` after the host → append a path component for the new DB.
    assert_eq!(
        swap_db_in_mysql_url("mysql://u:p@h:3306", "test_42"),
        "mysql://u:p@h:3306/test_42"
    );
}

#[test]
fn swap_db_in_mysql_url_no_scheme_treats_first_slash_as_path() {
    // No `://` → `find` returns 0; the first `/` is treated as the
    // path delimiter. Documents the fall-through; not a shape any
    // real `mysql://` URL takes, just pinning the helper's behaviour.
    assert_eq!(
        swap_db_in_mysql_url("h:3306/boot", "test_42"),
        "h:3306/test_42"
    );
}
