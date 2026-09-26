//! Prometheus counters, served as text exposition format at `/metrics`.
//!
//! A handful of monotonic `AtomicU64` counters rendered by hand — no
//! registry crate, because there are no labels beyond a fixed `result`
//! split and no histograms. Counters live on [`AppState`] (not in
//! statics) so parallel tests each observe their own.
//!
//! Currently instrumented: the Forgejo runner v1 cache dialect
//! (`src/routes/forgejo/`).
//!
//! [`AppState`]: crate::state::AppState

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

/// All counters exported by the process.
#[derive(Debug, Default)]
pub struct Metrics {
    pub forgejo: ForgejoMetrics,
}

/// Counters for the Forgejo runner v1 cache dialect.
#[derive(Debug, Default)]
pub struct ForgejoMetrics {
    /// `GET /cache` lookups that returned a hit.
    pub find_hits: Counter,
    /// `GET /cache` lookups that returned 204 (no entry, or blob gone).
    pub find_misses: Counter,
    /// Request-body bytes written to storage by `PATCH /caches/:id`.
    pub upload_bytes: Counter,
    /// Response-body bytes streamed by `GET /artifacts/:id`.
    pub download_bytes: Counter,
    /// `PATCH /caches/:id` requests that failed after authentication.
    pub upload_errors: Counter,
    /// `POST /caches/:id` commits that produced a cache entry.
    pub commits: Counter,
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
        family(
            &mut out,
            "gha_cache_oxide_forgejo_cache_lookups_total",
            "Forgejo v1 cache lookups by result.",
            &[
                ("result=\"hit\"", f.find_hits.get()),
                ("result=\"miss\"", f.find_misses.get()),
            ],
        );
        let singles = [
            (
                "gha_cache_oxide_forgejo_upload_bytes_total",
                "Bytes uploaded through the Forgejo v1 dialect.",
                &f.upload_bytes,
            ),
            (
                "gha_cache_oxide_forgejo_download_bytes_total",
                "Bytes downloaded through the Forgejo v1 dialect.",
                &f.download_bytes,
            ),
            (
                "gha_cache_oxide_forgejo_upload_errors_total",
                "Failed Forgejo v1 chunk uploads.",
                &f.upload_errors,
            ),
            (
                "gha_cache_oxide_forgejo_commits_total",
                "Committed Forgejo v1 cache entries.",
                &f.commits,
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
        m.forgejo.find_hits.inc();
        m.forgejo.find_hits.inc();
        m.forgejo.upload_bytes.add(1024);
        let text = m.render();
        assert!(text.contains("# TYPE gha_cache_oxide_forgejo_cache_lookups_total counter\n"));
        assert!(text.contains("gha_cache_oxide_forgejo_cache_lookups_total{result=\"hit\"} 2\n"));
        assert!(text.contains("gha_cache_oxide_forgejo_cache_lookups_total{result=\"miss\"} 0\n"));
        assert!(text.contains("gha_cache_oxide_forgejo_upload_bytes_total 1024\n"));
        assert!(text.contains("gha_cache_oxide_forgejo_auth_failures_total 0\n"));
    }
}
