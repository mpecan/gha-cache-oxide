//! Prometheus counters, served as text exposition format at `/metrics`.
//!
//! A handful of monotonic `AtomicU64` counters and one histogram,
//! rendered by hand — no registry crate. The only open-ended labels are
//! the Forgejo `{repo, key_prefix}` sources, which are capped (see
//! [`SourceMetrics`]). Counters live on [`AppState`] (not in
//! statics) so parallel tests each observe their own.
//!
//! Currently instrumented: the Forgejo runner v1 cache dialect
//! (`src/routes/forgejo/`) and blob merges (`src/merge.rs`, both the
//! lazy first-download merge and the post-commit background merge).
//!
//! [`AppState`]: crate::state::AppState

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

mod sources;
pub use sources::{MAX_SOURCES, SourceCounters, SourceMetrics, key_prefix};

/// All counters exported by the process.
#[derive(Debug, Default)]
pub struct Metrics {
    pub forgejo: ForgejoMetrics,
    pub merges: MergeMetrics,
}

/// Counters for merging `parts/*` into the single `merged` blob.
#[derive(Debug, Default)]
pub struct MergeMetrics {
    /// Merges whose blob was uploaded and finalised.
    pub completed: Counter,
    /// Merges that failed (flags reset; a later download retries).
    pub failed: Counter,
    /// Wall time from claim to finalise, successful merges only.
    pub duration: Histogram,
}

impl MergeMetrics {
    fn render(&self, out: &mut String) {
        family(
            out,
            "gha_cache_oxide_merges_total",
            "Blob merges (parts into one merged object) by result.",
            &[
                ("result=\"ok\"", self.completed.get()),
                ("result=\"error\"", self.failed.get()),
            ],
        );
        self.duration.render(
            out,
            "gha_cache_oxide_merge_duration_seconds",
            "Time to merge an entry's parts into its merged blob.",
        );
    }
}

/// Upper bounds (seconds) of [`Histogram`] buckets. Chosen for merges of
/// ~10 MB to ~1 GB caches over object storage.
pub const DURATION_BUCKETS_SECS: [f64; 9] = [0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0];

/// A fixed-bucket Prometheus histogram over seconds.
#[derive(Debug, Default)]
pub struct Histogram {
    /// Non-cumulative count per bucket in [`DURATION_BUCKETS_SECS`];
    /// observations above the last bound only count toward `count`.
    buckets: [Counter; DURATION_BUCKETS_SECS.len()],
    count: Counter,
    sum_micros: Counter,
}

impl Histogram {
    pub fn observe(&self, elapsed: std::time::Duration) {
        let secs = elapsed.as_secs_f64();
        if let Some(i) = DURATION_BUCKETS_SECS.iter().position(|b| secs <= *b) {
            self.buckets[i].inc();
        }
        self.count.inc();
        self.sum_micros
            .add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
    }

    pub fn count(&self) -> u64 {
        self.count.get()
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
        let mut cumulative = 0;
        for (bound, bucket) in DURATION_BUCKETS_SECS.iter().zip(&self.buckets) {
            cumulative += bucket.get();
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let count = self.count.get();
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        #[allow(clippy::cast_precision_loss)] // µs sums stay far below 2^52
        let sum = self.sum_micros.get() as f64 / 1e6;
        let _ = writeln!(out, "{name}_sum {sum}\n{name}_count {count}");
    }
}

/// Counters for the Forgejo runner v1 cache dialect.
#[derive(Debug, Default)]
pub struct ForgejoMetrics {
    /// Lookups (hit/miss), bytes up/down and commits, labelled
    /// `{repo, key_prefix}`. Lookups are attributed to the primary
    /// requested key; bytes and commits to the entry's key.
    pub sources: SourceMetrics,
    /// `PATCH /caches/:id` requests that failed after authentication.
    pub upload_errors: Counter,
    /// `POST /caches/:id` commits that failed after authentication.
    pub commit_errors: Counter,
    /// Requests rejected with 403 by MAC / timestamp validation.
    pub auth_failures: Counter,
}

/// A monotonic counter.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Metrics {
    /// Renders every counter in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let f = &self.forgejo;
        let mut out = String::new();
        f.sources.render(&mut out);
        let singles = [
            (
                "gha_cache_oxide_forgejo_upload_errors_total",
                "Failed Forgejo v1 chunk uploads.",
                &f.upload_errors,
            ),
            (
                "gha_cache_oxide_forgejo_commit_errors_total",
                "Failed Forgejo v1 commits.",
                &f.commit_errors,
            ),
            (
                "gha_cache_oxide_forgejo_auth_failures_total",
                "Forgejo v1 requests rejected by MAC or timestamp validation.",
                &f.auth_failures,
            ),
        ];
        for (name, help, counter) in singles {
            family(&mut out, name, help, &[("", counter.get())]);
        }
        self.merges.render(&mut out);
        out
    }
}

fn family(out: &mut String, name: &str, help: &str, samples: &[(&str, u64)]) {
    // `write!` into a `String` cannot fail.
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} counter");
    for (labels, value) in samples {
        if labels.is_empty() {
            let _ = writeln!(out, "{name} {value}");
        } else {
            let _ = writeln!(out, "{name}{{{labels}}} {value}");
        }
    }
}

/// `GET /metrics`.
pub async fn handler(State(state): State<AppState>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_text_format() {
        let m = Metrics::default();
        let src = m.forgejo.sources.get("o/r", "v0-rust-x");
        src.hits.inc();
        src.hits.inc();
        src.upload_bytes.add(1024);
        let text = m.render();
        let l = "repo=\"o/r\",key_prefix=\"v0-rust\"";
        assert!(text.contains("# TYPE gha_cache_oxide_forgejo_cache_lookups_total counter\n"));
        assert!(text.contains(&format!(
            "gha_cache_oxide_forgejo_cache_lookups_total{{result=\"hit\",{l}}} 2\n"
        )));
        assert!(text.contains(&format!(
            "gha_cache_oxide_forgejo_cache_lookups_total{{result=\"miss\",{l}}} 0\n"
        )));
        assert!(text.contains(&format!(
            "gha_cache_oxide_forgejo_upload_bytes_total{{{l}}} 1024\n"
        )));
        assert!(text.contains("gha_cache_oxide_forgejo_auth_failures_total 0\n"));
        assert!(text.contains("gha_cache_oxide_merges_total{result=\"ok\"} 0\n"));
        assert!(text.contains("# TYPE gha_cache_oxide_merge_duration_seconds histogram\n"));
    }

    #[test]
    fn histogram_buckets_are_cumulative() {
        let m = Metrics::default();
        let h = &m.merges.duration;
        h.observe(std::time::Duration::from_millis(300)); // le 0.5
        h.observe(std::time::Duration::from_secs(4)); // le 5
        h.observe(std::time::Duration::from_secs(400)); // above every bound
        let text = m.render();
        let name = "gha_cache_oxide_merge_duration_seconds";
        assert!(text.contains(&format!("{name}_bucket{{le=\"0.5\"}} 1\n")));
        assert!(text.contains(&format!("{name}_bucket{{le=\"2.5\"}} 1\n")));
        assert!(text.contains(&format!("{name}_bucket{{le=\"5\"}} 2\n")));
        assert!(text.contains(&format!("{name}_bucket{{le=\"300\"}} 2\n")));
        assert!(text.contains(&format!("{name}_bucket{{le=\"+Inf\"}} 3\n")));
        assert!(text.contains(&format!("{name}_count 3\n")));
        assert!(text.contains(&format!("{name}_sum 404.3\n")));
    }
}
