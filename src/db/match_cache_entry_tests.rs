//! Tests for [`Db::match_cache_entry`], split into its own file so the
//! production module and the sibling tests each stay well under the
//! 500-line soft limit. Attached via `#[path]` from `queries.rs`.
//!
//! Every scenario seeds rows through a small `seed_entry` helper (a
//! `storage_locations` + `cache_entries` pair per entry) so the tests
//! exercise the real schema, not a mock.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use super::*;
use crate::db::entities::{MatchRequest, MatchType};
use crate::db::id::new_uuid;

async fn test_db() -> Db {
    let db = Db::connect_in_memory().await.unwrap();
    db.migrate().await.unwrap();
    db
}

/// Test-only shape combining cache entry coordinates with the
/// `updatedAt` value that the prefix query orders by.
struct Seed<'a> {
    key: &'a str,
    version: &'a str,
    scope: &'a str,
    repo_id: &'a str,
    updated_at_ms: i64,
}

/// Inserts a `storage_locations` row plus a `cache_entries` row pointing
/// at it. Returns the generated cache-entry id so callers can assert
/// which row came back.
async fn seed_entry(db: &Db, s: Seed<'_>) -> String {
    let location_id = new_uuid();
    let entry_id = new_uuid();
    let mut tx = db.pool().begin().await.unwrap();
    insert_storage_location_tx(&mut tx, &location_id, &format!("folder-{entry_id}"), 1)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO cache_entries (id, key, version, scope, repoId, updatedAt, locationId) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&entry_id)
    .bind(s.key)
    .bind(s.version)
    .bind(s.scope)
    .bind(s.repo_id)
    .bind(s.updated_at_ms)
    .bind(&location_id)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    entry_id
}

/// Shorthand: seed a row in the default scope/repo/version with a given
/// key and `updatedAt` value. Most tests only vary key and time.
async fn seed(db: &Db, key: &str, updated_at_ms: i64) -> String {
    seed_entry(
        db,
        Seed {
            key,
            version: "v1",
            scope: "s",
            repo_id: "42",
            updated_at_ms,
        },
    )
    .await
}

/// Seed helper that varies the scope on top of the default version/repo.
async fn seed_in_scope(db: &Db, key: &str, scope: &str, updated_at_ms: i64) -> String {
    seed_entry(
        db,
        Seed {
            key,
            version: "v1",
            scope,
            repo_id: "42",
            updated_at_ms,
        },
    )
    .await
}

fn req<'a>(
    primary_key: &'a str,
    restore_keys: &'a [&'a str],
    scopes: &'a [&'a str],
) -> MatchRequest<'a> {
    MatchRequest {
        primary_key,
        restore_keys,
        version: "v1",
        scopes,
        repo_id: "42",
    }
}

#[tokio::test]
async fn exact_primary_returns_exact_primary() {
    let db = test_db().await;
    let id = seed_in_scope(&db, "build-cache", "refs/heads/main", 1_000).await;

    let m = db
        .match_cache_entry(req("build-cache", &[], &["refs/heads/main"]))
        .await
        .unwrap()
        .expect("should match");
    assert_eq!(m.match_type, MatchType::ExactPrimary);
    assert_eq!(m.entry.id, id);
    assert_eq!(m.entry.key, "build-cache");
}

#[tokio::test]
async fn prefixed_primary_returns_prefixed_primary_newest_first() {
    let db = test_db().await;
    // Two candidates share the prefix; the newer one must win.
    let _older = seed(&db, "build-cache-abc", 1_000).await;
    let newer = seed(&db, "build-cache-xyz", 2_000).await;

    let m = db
        .match_cache_entry(req("build-cache", &[], &["s"]))
        .await
        .unwrap()
        .expect("prefix should match");
    assert_eq!(m.match_type, MatchType::PrefixedPrimary);
    assert_eq!(m.entry.id, newer);
}

#[tokio::test]
async fn exact_restore_wins_when_primary_misses() {
    let db = test_db().await;
    let restore_hit = seed(&db, "deps", 1_000).await;
    // Decoy for a different primary that won't match.
    let _ = seed(&db, "deps-other", 2_000).await;

    let m = db
        .match_cache_entry(req("missing-primary", &["deps"], &["s"]))
        .await
        .unwrap()
        .expect("restore exact should match");
    assert_eq!(m.match_type, MatchType::ExactRestore);
    assert_eq!(m.entry.id, restore_hit);
}

#[tokio::test]
async fn prefixed_restore_wins_when_exact_restore_misses() {
    let db = test_db().await;
    let hit = seed(&db, "deps-lockfile-abc123", 1_000).await;

    let m = db
        .match_cache_entry(req("nope", &["deps-lockfile-"], &["s"]))
        .await
        .unwrap()
        .expect("restore prefix should match");
    assert_eq!(m.match_type, MatchType::PrefixedRestore);
    assert_eq!(m.entry.id, hit);
}

#[tokio::test]
async fn exact_primary_beats_prefix_in_same_scope() {
    // Proves the exact-primary query runs before the prefix query on the
    // first scope — an entry that would prefix-match is ignored in favor
    // of the exact primary hit.
    let db = test_db().await;
    let exact = seed(&db, "build-cache", 1_000).await;
    let _prefix_decoy = seed(&db, "build-cache-newer", 9_999).await;

    let m = db
        .match_cache_entry(req("build-cache", &[], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.match_type, MatchType::ExactPrimary);
    assert_eq!(m.entry.id, exact);
}

#[tokio::test]
async fn higher_priority_scope_wins_on_exact_primary() {
    // Same primary key seeded in two scopes; scope[0] must win because
    // the outer loop walks scopes in priority order and returns on the
    // first hit.
    let db = test_db().await;
    let priority = seed_in_scope(&db, "build-cache", "scope-high", 1_000).await;
    let _secondary = seed_in_scope(&db, "build-cache", "scope-low", 9_999).await;

    let m = db
        .match_cache_entry(req("build-cache", &[], &["scope-high", "scope-low"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.match_type, MatchType::ExactPrimary);
    assert_eq!(m.entry.id, priority);
}

#[tokio::test]
async fn prefix_match_orders_by_updated_at_desc() {
    let db = test_db().await;
    let _oldest = seed(&db, "build-cache-a", 1_000).await;
    let middle = seed(&db, "build-cache-b", 3_000).await;
    let _ = middle; // used as a decoy between oldest and newest
    let newest = seed(&db, "build-cache-c", 5_000).await;

    let m = db
        .match_cache_entry(req("build-cache", &[], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.entry.id, newest);
}

#[tokio::test]
async fn empty_restore_keys_short_circuits_on_primary_miss() {
    // Acceptance criterion: "No primary match + restore_keys.is_empty()
    // → None (doesn't walk)". Upstream: the `if (restoreKeys.length ===
    // 0) return` inside the scopes loop terminates the whole function.
    let db = test_db().await;
    let _ = seed(&db, "other", 1_000).await;

    let out = db
        .match_cache_entry(req("absent", &[], &["s"]))
        .await
        .unwrap();
    assert!(out.is_none());
}

#[tokio::test]
async fn like_pattern_escapes_underscore() {
    // Without escaping, LIKE treats `_` as any-single-char. Seed a key
    // containing a literal underscore AND a sibling key where a letter
    // stands where the underscore would be — only the literal-underscore
    // row should match.
    let db = test_db().await;
    let literal = seed(&db, "prefix_v1", 1_000).await;
    let _wildcard_collision = seed(&db, "prefixAv1", 2_000).await;

    // Prefix-primary search with `prefix_` — escaping must pin `_` to a
    // literal, so only `prefix_v1` can match.
    let m = db
        .match_cache_entry(req("prefix_", &[], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.match_type, MatchType::PrefixedPrimary);
    assert_eq!(m.entry.id, literal);
}

#[tokio::test]
async fn like_pattern_escapes_percent() {
    // A key containing `%` should match a prefix that contains `%`
    // literally and not act as a wildcard.
    let db = test_db().await;
    let literal = seed(&db, "50%-off-abc", 1_000).await;
    let _unrelated = seed(&db, "50-off-abc", 2_000).await;

    // Prefix "50%" escaped as literal "%" must match only the literal-%
    // row, not `50-off-abc` (which would match if `%` were treated as a
    // wildcard).
    let m = db
        .match_cache_entry(req("50%", &[], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.match_type, MatchType::PrefixedPrimary);
    assert_eq!(m.entry.id, literal);
}

#[tokio::test]
async fn like_pattern_escapes_backslash() {
    // Backslash is the escape character itself. Seed a key containing a
    // literal backslash and confirm both exact and prefix matching work.
    let db = test_db().await;
    let literal = seed(&db, r"dir\sub-v1", 1_000).await;

    let exact = db
        .match_cache_entry(req(r"dir\sub-v1", &[], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact.match_type, MatchType::ExactPrimary);
    assert_eq!(exact.entry.id, literal);

    let prefix = db
        .match_cache_entry(req(r"dir\", &[], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prefix.match_type, MatchType::PrefixedPrimary);
    assert_eq!(prefix.entry.id, literal);
}

#[tokio::test]
async fn no_scope_matches_returns_none() {
    let db = test_db().await;
    let _ = seed_in_scope(&db, "k", "scope-a", 1_000).await;

    let out = db
        .match_cache_entry(req("k", &["k"], &["scope-b", "scope-c"]))
        .await
        .unwrap();
    assert!(out.is_none());
}

#[tokio::test]
async fn version_mismatch_never_matches() {
    let db = test_db().await;
    let _ = seed(&db, "k", 1_000).await;

    let out = db
        .match_cache_entry(MatchRequest {
            primary_key: "k",
            restore_keys: &["k"],
            version: "v-different",
            scopes: &["s"],
            repo_id: "42",
        })
        .await
        .unwrap();
    assert!(out.is_none());
}

#[tokio::test]
async fn repo_id_mismatch_never_matches() {
    let db = test_db().await;
    let _ = seed(&db, "k", 1_000).await;

    let out = db
        .match_cache_entry(MatchRequest {
            primary_key: "k",
            restore_keys: &["k"],
            version: "v1",
            scopes: &["s"],
            repo_id: "99",
        })
        .await
        .unwrap();
    assert!(out.is_none());
}

#[tokio::test]
async fn restore_keys_are_tried_in_order() {
    // First restore key that matches wins. Seed only the second restore
    // key's prefix target; the first restore key must miss before the
    // second hits.
    let db = test_db().await;
    let second_hit = seed(&db, "fallback-abc", 1_000).await;

    let m = db
        .match_cache_entry(req("primary", &["first-miss", "fallback-"], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.match_type, MatchType::PrefixedRestore);
    assert_eq!(m.entry.id, second_hit);
}

#[tokio::test]
async fn exact_restore_beats_prefix_restore_on_same_scope() {
    let db = test_db().await;
    let exact = seed(&db, "deps", 1_000).await;
    let _prefix_newer = seed(&db, "deps-newer", 9_999).await;

    let m = db
        .match_cache_entry(req("missing", &["deps"], &["s"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.match_type, MatchType::ExactRestore);
    assert_eq!(m.entry.id, exact);
}
