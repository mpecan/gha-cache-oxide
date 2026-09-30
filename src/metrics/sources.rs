//! Per-source Forgejo cache counters, labelled `{repo, key_prefix}`.
//!
//! `repo` is the MAC-validated `Forgejo-Cache-Repo` (`owner/name`), so a
//! client cannot invent label values. `key_prefix` is a coarse class of
//! the cache key ([`key_prefix`]): the tool that wrote it, not the key
//! itself. Distinct label sets are capped at [`MAX_SOURCES`]; beyond
//! that everything is counted under `repo="_other", key_prefix="_other"`,
//! so cardinality stays bounded whatever the workload does.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};

use super::Counter;

/// Upper bound on distinct `{repo, key_prefix}` label sets.
pub const MAX_SOURCES: usize = 256;

/// Label value used once [`MAX_SOURCES`] is reached.
pub const OVERFLOW: &str = "_other";

/// Longest `key_prefix` segment kept; longer segments are truncated.
const SEGMENT_MAX: usize = 32;

/// Longest `repo` label kept.
const REPO_MAX: usize = 100;

/// Counters for one `{repo, key_prefix}` source.
#[derive(Debug, Default)]
pub struct SourceCounters {
    pub hits: Counter,
    pub misses: Counter,
    pub upload_bytes: Counter,
    pub download_bytes: Counter,
    pub commits: Counter,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Source {
    repo: String,
    key_prefix: String,
}

/// Registry of per-source counters.
#[derive(Debug, Default)]
pub struct SourceMetrics {
    sources: Mutex<HashMap<Source, Arc<SourceCounters>>>,
}

/// Classifies a cache key by the tool that wrote it.
///
/// The first `-`-separated segment, plus the second when it is purely alphabetic
/// (so `v0-rust-…` → `v0-rust`, `node-cache-Linux-…` → `node-cache`,
/// `buildkit-blob-1-sha256:…` → `buildkit-blob`, `index-buildkit-1-…`
/// → `index-buildkit`, `Linux-x64-…` → `linux`). Lowercased, restricted
/// to `[a-z0-9_.]`, each segment capped at 32 chars; an empty result is
/// `_empty`.
pub fn key_prefix(key: &str) -> String {
    let mut parts = key.split('-').map(clean_segment);
    let first = parts.next().unwrap_or_default();
    if first.is_empty() {
        return "_empty".to_string();
    }
    match parts.next() {
        Some(second) if !second.is_empty() && second.bytes().all(|b| b.is_ascii_lowercase()) => {
            format!("{first}-{second}")
        }
        _ => first,
    }
}

fn clean_segment(segment: &str) -> String {
    segment
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '.')
        .take(SEGMENT_MAX)
        .collect()
}

impl SourceMetrics {
    /// Counters for `repo` (`owner/name`) and the class of `key`.
    pub fn get(&self, repo: &str, key: &str) -> Arc<SourceCounters> {
        let source = Source {
            repo: repo.chars().take(REPO_MAX).collect(),
            key_prefix: key_prefix(key),
        };
        let mut map = self.sources.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = map.get(&source) {
            return Arc::clone(c);
        }
        let source = if map.len() < MAX_SOURCES {
            source
        } else {
            Source {
                repo: OVERFLOW.into(),
                key_prefix: OVERFLOW.into(),
            }
        };
        Arc::clone(map.entry(source).or_default())
    }

    /// Every tracked source and its counters, sorted by label. Clones the
    /// `Arc`s so the lock is released before any formatting or summing.
    fn snapshot(&self) -> Vec<(Source, Arc<SourceCounters>)> {
        let mut v: Vec<_> = self
            .sources
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(s, c)| (s.clone(), Arc::clone(c)))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Sum of every source's counters, for unlabelled totals in tests.
    pub fn totals(&self) -> SourceCounters {
        let t = SourceCounters::default();
        for (_, c) in self.snapshot() {
            t.hits.add(c.hits.get());
            t.misses.add(c.misses.get());
            t.upload_bytes.add(c.upload_bytes.get());
            t.download_bytes.add(c.download_bytes.get());
            t.commits.add(c.commits.get());
        }
        t
    }

    pub(super) fn render(&self, out: &mut String) {
        let sources = self.snapshot();

        header(
            out,
            "gha_cache_oxide_forgejo_cache_lookups_total",
            "Forgejo v1 cache lookups by result, repository and key prefix.",
        );
        for (s, c) in &sources {
            for (result, n) in [("hit", c.hits.get()), ("miss", c.misses.get())] {
                let _ = writeln!(
                    out,
                    "gha_cache_oxide_forgejo_cache_lookups_total{{result=\"{result}\",{}}} {n}",
                    labels(s)
                );
            }
        }
        let per_source: [Family; 3] = [
            (
                "gha_cache_oxide_forgejo_upload_bytes_total",
                "Bytes uploaded through the Forgejo v1 dialect.",
                |c| c.upload_bytes.get(),
            ),
            (
                "gha_cache_oxide_forgejo_download_bytes_total",
                "Bytes downloaded through the Forgejo v1 dialect.",
                |c| c.download_bytes.get(),
            ),
            (
                "gha_cache_oxide_forgejo_commits_total",
                "Committed Forgejo v1 cache entries.",
                |c| c.commits.get(),
            ),
        ];
        for (name, help, value) in per_source {
            header(out, name, help);
            for (s, c) in &sources {
                let _ = writeln!(out, "{name}{{{}}} {}", labels(s), value(c));
            }
        }
    }
}

/// `(metric name, help text, value accessor)` for one per-source family.
type Family = (&'static str, &'static str, fn(&SourceCounters) -> u64);

fn header(out: &mut String, name: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} counter");
}

fn labels(s: &Source) -> String {
    format!(
        "repo=\"{}\",key_prefix=\"{}\"",
        escape(&s.repo),
        escape(&s.key_prefix)
    )
}

/// Prometheus text-format label-value escaping.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_prefix_classifies_common_tools() {
        for (key, want) in [
            ("v0-rust-build-Linux-x64-abc123", "v0-rust"),
            ("node-cache-Linux-x64-pnpm-9f2e", "node-cache"),
            ("buildkit-blob-1-sha256:0123abcd", "buildkit-blob"),
            ("index-buildkit-1-89abcdef", "index-buildkit"),
            ("setup-go-Linux-x64-go-1.23", "setup-go"),
            ("Linux-x64-cargo-deadbeef", "linux"),
            ("docker", "docker"),
            ("", "_empty"),
            ("-leading-dash", "_empty"),
            ("weird\"chars\n-x", "weirdchars-x"),
        ] {
            assert_eq!(key_prefix(key), want, "{key:?}");
        }
        let long = "a".repeat(100);
        assert_eq!(key_prefix(&long).len(), 32);
    }

    #[test]
    fn same_source_shares_counters() {
        let m = SourceMetrics::default();
        m.get("o/r", "v0-rust-a").hits.inc();
        m.get("o/r", "v0-rust-b").hits.inc();
        assert_eq!(m.get("o/r", "v0-rust-zzz").hits.get(), 2);
        assert_eq!(m.get("o/other", "v0-rust-a").hits.get(), 0);
    }

    #[test]
    fn cardinality_is_capped_into_overflow() {
        let m = SourceMetrics::default();
        for i in 0..MAX_SOURCES + 50 {
            m.get(&format!("owner/repo{i}"), "k-x").misses.inc();
        }
        let map = m.sources.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(map.len(), MAX_SOURCES + 1, "cap plus one overflow bucket");
        let overflow = Source {
            repo: OVERFLOW.into(),
            key_prefix: OVERFLOW.into(),
        };
        assert_eq!(map[&overflow].misses.get(), 50, "every call past the cap");
        drop(map);
        assert_eq!(
            m.totals().misses.get(),
            u64::try_from(MAX_SOURCES + 50).unwrap_or(0)
        );
    }

    #[test]
    fn renders_labelled_families_sorted_and_escaped() {
        let m = SourceMetrics::default();
        m.get("zed/app", "node-cache-x").misses.inc();
        let a = m.get("ann/lib", "v0-rust-x");
        a.hits.add(3);
        a.upload_bytes.add(10);
        a.download_bytes.add(20);
        a.commits.inc();
        m.get("odd\"repo", "k").hits.inc();
        let mut out = String::new();
        m.render(&mut out);

        let l = "repo=\"ann/lib\",key_prefix=\"v0-rust\"";
        assert!(out.contains(&format!(
            "gha_cache_oxide_forgejo_cache_lookups_total{{result=\"hit\",{l}}} 3\n"
        )));
        assert!(out.contains(&format!(
            "gha_cache_oxide_forgejo_upload_bytes_total{{{l}}} 10\n"
        )));
        assert!(out.contains(&format!(
            "gha_cache_oxide_forgejo_download_bytes_total{{{l}}} 20\n"
        )));
        assert!(out.contains(&format!("gha_cache_oxide_forgejo_commits_total{{{l}}} 1\n")));
        assert!(out.contains("repo=\"odd\\\"repo\""), "quote escaped: {out}");
        let ann = out.find("repo=\"ann/lib\"").unwrap_or(usize::MAX);
        let zed = out.find("repo=\"zed/app\"").unwrap_or(0);
        assert!(ann < zed, "sorted by repo");
    }
}
