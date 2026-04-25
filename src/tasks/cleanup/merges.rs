//! `cleanup:merges` — port of upstream `tasks/cleanup/merges.ts`.
//!
//! Resets `mergeStartedAt` on `storage_locations` rows whose claim is
//! older than [`STALENESS_MS`] and never reached `mergedAt`. Reuses
//! [`Db::clear_stale_merge_claims`](crate::db::Db::clear_stale_merge_claims),
//! which is the same query the startup sweep (issue #17) calls — only
//! the cutoff differs.
//!
//! # Threshold
//!
//! 15 minutes matches upstream `tasks/cleanup/merges.ts:12`. The
//! background scheduler runs while the process is up, so a claim
//! older than 15 min is necessarily orphaned (nothing in the request
//! path holds a claim that long). The startup sweep uses a more
//! conservative 1-hour cutoff
//! ([`STALE_MERGE_THRESHOLD_MS`](crate::STALE_MERGE_THRESHOLD_MS))
//! because at startup we cannot tell how long a pre-crash merge had
//! been in flight.

use crate::db::Db;

/// 15-minute background-cleanup staleness threshold. Matches upstream
/// `tasks/cleanup/merges.ts`.
pub(super) const STALENESS_MS: i64 = 15 * 60 * 1_000;

/// Runs one cleanup pass. Returns the number of rows whose
/// `mergeStartedAt` was reset.
pub(super) async fn run(db: &dyn Db, now_ms: i64) -> u64 {
    let cutoff = now_ms.saturating_sub(STALENESS_MS);
    match db.clear_stale_merge_claims(cutoff).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "cleanup:merges: clear_stale_merge_claims failed");
            0
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{STALENESS_MS, run};
    use crate::db::entities::CacheEntryCoord;
    use crate::db::{Db, SqliteDb};

    async fn fresh_db() -> SqliteDb {
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        db
    }

    async fn seed_claim(db: &SqliteDb, loc: &str, started_at: i64) {
        let mut tx = db.begin().await.unwrap();
        tx.insert_storage_location(loc, "fldr", 1).await.unwrap();
        tx.seed_cache_entry(
            &format!("entry-{loc}"),
            CacheEntryCoord {
                key: "k",
                version: "v",
                scope: loc,
                repo_id: "r",
            },
            0,
            loc,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(db.try_mark_merge_started(loc, started_at).await.unwrap());
    }

    #[tokio::test]
    async fn stale_claim_is_reset() {
        let db = fresh_db().await;
        seed_claim(&db, "loc-merge-stale", 0).await;
        let now = STALENESS_MS + 1;

        let count = run(&db, now).await;
        assert_eq!(count, 1);
        let row = db
            .find_location_for_entry("entry-loc-merge-stale")
            .await
            .unwrap()
            .unwrap();
        assert!(row.merge_started_at.is_none());
    }

    #[tokio::test]
    async fn empty_db_run_is_noop() {
        let db = fresh_db().await;
        assert_eq!(run(&db, STALENESS_MS + 1).await, 0);
    }

    #[tokio::test]
    async fn fresh_claim_is_preserved() {
        let db = fresh_db().await;
        // Claim at time = STALENESS_MS, scheduler clock = STALENESS_MS:
        // cutoff = 0, so the strict `<` keeps the claim.
        seed_claim(&db, "loc-merge-fresh", STALENESS_MS).await;
        let now = STALENESS_MS;

        let count = run(&db, now).await;
        assert_eq!(count, 0);
        let row = db
            .find_location_for_entry("entry-loc-merge-fresh")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.merge_started_at, Some(STALENESS_MS));
    }
}
