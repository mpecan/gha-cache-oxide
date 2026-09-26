//! Three independent cadence schedulers (issue #73).
//!
//! Splits the work that drives [`super::run_all`]'s sub-tasks across
//! three [`tokio::spawn`]s, each driven by its own [`Schedule`],
//! mirroring upstream's `nitro.config.ts:25` cron lines:
//!
//! - [`CleanupSchedules::uploads`] runs `cleanup:uploads`.
//! - [`CleanupSchedules::hourly`] runs `cleanup:merges` then
//!   `cleanup:parts`.
//! - [`CleanupSchedules::daily`] runs `cleanup:cache-entries` then
//!   `cleanup:storage-locations`.
//!
//! All three share one [`CancellationToken`]; cancelling it shuts down
//! every loop at its next `select!` poll. [`Schedulers::shutdown`]
//! joins them.
//!
//! # Cron-driven by default
//!
//! Production wiring uses [`Schedule::Cron`] so cadences fire on
//! wall-clock boundaries (e.g. the daily reap lands at 00:00 UTC, just
//! like upstream's cron). Operators override the cron strings via
//! `CLEANUP_{UPLOADS,HOURLY,DAILY}_SCHEDULE` env vars; defaults match
//! upstream verbatim.
//!
//! Tests use [`Schedule::Every`] for compressed (50–250 ms) cadences —
//! cron's minimum granularity is one second, which would be too slow
//! for the integration suite.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{entries, locations, merges, parts, uploads};
use crate::db::Db;
use crate::db::id::now_ms;
use crate::storage::StorageAdapter;

/// How a single scheduler decides when to fire next.
///
/// Two variants because production and tests have incompatible
/// requirements: production wants wall-clock-aligned cron firings;
/// tests want sub-second cadences for fast integration coverage.
/// Wrapping both in one enum keeps `spawn_schedulers` agnostic.
///
/// `cron::Schedule` is a heavy struct (200+ bytes of ordinal sets) so
/// it lives behind a `Box` to keep the enum size balanced — clippy's
/// `large_enum_variant` lint flags the alternative, and we'd otherwise
/// pay the larger variant's size on every clone.
#[derive(Clone, Debug)]
pub enum Schedule {
    /// Wall-clock-aligned cron expression. Production default; matches
    /// upstream `nitro.config.ts:25` semantics.
    Cron(Box<cron::Schedule>),
    /// Repeat every `Duration` after spawn. Used by tests so cadences
    /// can fire faster than 1 s (cron's minimum granularity). Operators
    /// could in principle hit this path too, but the public env-var
    /// contract only exposes [`Schedule::Cron`].
    Every(Duration),
}

impl Schedule {
    /// Computes how long to sleep before the next fire.
    ///
    /// For `Every`, returns the configured period verbatim.
    ///
    /// For `Cron`, two cases are distinguished:
    /// - **No upcoming match** (e.g. `0 0 30 2 *` — Feb 30 never
    ///   exists): sleep 1 hour and re-poll on the next loop iteration.
    ///   The operator will see the misconfiguration in tracing.
    /// - **Next match is in the past** (clock skew or jitter pushing
    ///   `next - now < 0`): sleep zero so we fire immediately on the
    ///   next iteration. Conflating this with the first case would
    ///   stall a cadence for a full hour after a clock blip.
    fn next_sleep(&self) -> Duration {
        match self {
            Self::Cron(s) => s.upcoming(Utc).next().map_or_else(
                || Duration::from_secs(3600),
                |next| (next - Utc::now()).to_std().unwrap_or(Duration::ZERO),
            ),
            Self::Every(d) => *d,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::str::FromStr;

    use super::*;

    /// `Schedule::Cron::next_sleep` returns roughly the wall-clock
    /// distance to the next match. With `* * * * * *` (every second)
    /// the next sleep is at most one second.
    #[test]
    fn cron_next_sleep_returns_at_most_one_second_for_every_second() {
        let schedule = Schedule::Cron(Box::new(cron::Schedule::from_str("* * * * * *").unwrap()));
        let dur = schedule.next_sleep();
        assert!(
            dur <= Duration::from_secs(1),
            "every-second cron must sleep ≤ 1 s, got {dur:?}",
        );
    }

    /// `Schedule::Every` returns the configured period verbatim — used
    /// by tests for compressed cadences. Asserting the contract pins
    /// the behaviour against accidental refactors.
    #[test]
    fn every_next_sleep_returns_configured_duration() {
        let schedule = Schedule::Every(Duration::from_millis(50));
        assert_eq!(schedule.next_sleep(), Duration::from_millis(50));
    }
}

/// Per-cadence schedule bundle.
#[derive(Clone, Debug)]
pub struct CleanupSchedules {
    pub uploads: Schedule,
    pub hourly: Schedule,
    pub daily: Schedule,
}

/// Bundle of inputs `spawn_schedulers` needs. Hoisted into a struct so
/// the public `maybe_spawn` / `spawn_schedulers` signatures stay
/// within clippy's 5-argument limit when fields are added later.
pub struct SchedulerSpawn {
    pub db: Arc<dyn Db>,
    pub storage: Arc<dyn StorageAdapter>,
    pub retention: super::EntryRetention,
    pub schedules: CleanupSchedules,
}

/// Three live scheduler handles. Each owns its own `tokio::spawn`;
/// [`Self::shutdown`] joins all three after a `CancellationToken::cancel()`.
pub struct Schedulers {
    uploads: JoinHandle<()>,
    hourly: JoinHandle<()>,
    daily: JoinHandle<()>,
}

impl Schedulers {
    /// Awaits all three handles. Logs (warn) any join error rather than
    /// propagating — a panicking scheduler shouldn't block process exit.
    pub async fn shutdown(self) {
        let (u, h, d) = tokio::join!(self.uploads, self.hourly, self.daily);
        for (name, res) in [("uploads", u), ("hourly", h), ("daily", d)] {
            if let Err(e) = res {
                tracing::warn!(scheduler = name, error = %e, "cleanup scheduler join failed");
            }
        }
    }
}

/// Spawns the three background schedulers (uploads / hourly / daily).
///
/// All share the same `shutdown` token; cancelling it stops every loop
/// at its next select-poll. Each tick logs the per-task counts produced.
pub fn spawn_schedulers(spawn: SchedulerSpawn, shutdown: CancellationToken) -> Schedulers {
    let SchedulerSpawn {
        db,
        storage,
        retention,
        schedules,
    } = spawn;
    let uploads = spawn_uploads(
        db.clone(),
        storage.clone(),
        schedules.uploads,
        shutdown.clone(),
    );
    let hourly = spawn_hourly(
        db.clone(),
        storage.clone(),
        schedules.hourly,
        shutdown.clone(),
    );
    let daily = spawn_daily(db, storage, retention, schedules.daily, shutdown);
    Schedulers {
        uploads,
        hourly,
        daily,
    }
}

/// Convenience wrapper used by `src/main.rs`.
///
/// Spawns the three schedulers iff `disable_cleanup_jobs` is `false`,
/// logging the decision either way. Returns `None` when disabled.
/// Tests bypass this and call [`spawn_schedulers`] directly so they
/// own the `CancellationToken`.
pub fn maybe_spawn(
    spawn: SchedulerSpawn,
    disable_cleanup_jobs: bool,
    shutdown: CancellationToken,
) -> Option<Schedulers> {
    if disable_cleanup_jobs {
        tracing::info!("DISABLE_CLEANUP_JOBS=true - cleanup schedulers not started");
        return None;
    }
    tracing::info!(
        uploads = ?spawn.schedules.uploads,
        hourly = ?spawn.schedules.hourly,
        daily = ?spawn.schedules.daily,
        cache_cleanup_older_than_days = spawn.retention.older_than_days,
        cache_cleanup_unused_older_than_days = ?spawn.retention.unused_older_than_days,
        "cleanup schedulers starting (uploads/hourly/daily)",
    );
    Some(spawn_schedulers(spawn, shutdown))
}

fn spawn_uploads(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    schedule: Schedule,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_cadence("uploads", schedule, shutdown, move || {
        let db = db.clone();
        let storage = storage.clone();
        async move { run_uploads_cycle(db, storage).await }
    })
}

fn spawn_hourly(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    schedule: Schedule,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_cadence("hourly", schedule, shutdown, move || {
        let db = db.clone();
        let storage = storage.clone();
        async move { run_hourly_cycle(db, storage).await }
    })
}

fn spawn_daily(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    retention: super::EntryRetention,
    schedule: Schedule,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_cadence("daily", schedule, shutdown, move || {
        let db = db.clone();
        let storage = storage.clone();
        async move { run_daily_cycle(db, storage, retention).await }
    })
}

/// Loops `run` driven by `schedule`, breaking when `shutdown` fires.
///
/// On each iteration the schedule computes how long to sleep until the
/// next fire (cron: until the next wall-clock match; every: the fixed
/// duration). Cancellation is checked before and during the sleep, so
/// shutdown happens within at most one tick worth of latency.
fn spawn_cadence<F, Fut>(
    name: &'static str,
    schedule: Schedule,
    shutdown: CancellationToken,
    mut run: F,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        loop {
            let sleep = schedule.next_sleep();
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!(scheduler = name, "cleanup scheduler shutting down");
                    break;
                }
                () = tokio::time::sleep(sleep) => {
                    run().await;
                }
            }
        }
    })
}

async fn run_uploads_cycle(db: Arc<dyn Db>, storage: Arc<dyn StorageAdapter>) {
    let deleted = uploads::run(&*db, &*storage, now_ms()).await;
    tracing::info!(deleted, "cleanup:uploads cycle complete");
}

async fn run_hourly_cycle(db: Arc<dyn Db>, storage: Arc<dyn StorageAdapter>) {
    // Order matches upstream `nitro.config.ts:28` listing
    // (`['cleanup:parts', 'cleanup:merges']`). Tasks operate on
    // disjoint row states (parts: already-merged rows; merges: stale
    // claims) so order is arbitrary; matching upstream keeps the diff
    // boring.
    let parts_deleted = parts::run(&*db, &*storage, now_ms()).await;
    let merges_reset = merges::run(&*db, now_ms()).await;
    tracing::info!(
        parts_deleted,
        merges_reset,
        "cleanup hourly cycle complete (parts + merges)",
    );
}

async fn run_daily_cycle(
    db: Arc<dyn Db>,
    storage: Arc<dyn StorageAdapter>,
    retention: super::EntryRetention,
) {
    let entries_deleted = entries::run(&*db, &*storage, now_ms(), retention).await;
    let locations_deleted = locations::run(&*db, &*storage).await;
    tracing::info!(
        entries_deleted,
        locations_deleted,
        "cleanup daily cycle complete (entries + locations)",
    );
}
