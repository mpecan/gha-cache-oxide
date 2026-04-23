//! Golden-file comparator.
//!
//! `assert_golden(label, actual)` is the public entry point: reads
//! `tests/golden/<label>.json` and compares actual vs stored. Under
//! `UPDATE_GOLDEN=1` it writes `actual` to the file instead of
//! asserting — the canonical way to regenerate goldens.
//!
//! `write_or_compare_golden` is the filesystem-parametric core, split
//! out so tests can drive both the write and compare branches against
//! a temp dir without touching the real `tests/golden/` tree.

use std::path::{Path, PathBuf};

use serde_json::Value;

/// Returns the repo-relative path of a golden file.
fn golden_path(label: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(format!("{label}.json"))
}

/// Compares `actual` against the golden file at `tests/golden/<label>.json`.
/// Under `UPDATE_GOLDEN=1` writes `actual` to the file instead of
/// asserting.
pub fn assert_golden(label: &str, actual: &Value) {
    let path = golden_path(label);
    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    write_or_compare_golden(&path, actual, update, label);
}

/// Filesystem-parametric core. `update=true` writes; `update=false`
/// reads + compares. Exposed so tests can drive it against a temp dir.
pub fn write_or_compare_golden(path: &Path, actual: &Value, update: bool, label: &str) {
    if update {
        let parent = path.parent().unwrap();
        std::fs::create_dir_all(parent).unwrap();
        let pretty = serde_json::to_string_pretty(actual).unwrap();
        std::fs::write(path, format!("{pretty}\n")).unwrap();
        return;
    }
    let expected_raw = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "missing golden file {} ({e}) — run once with UPDATE_GOLDEN=1 to populate",
            path.display()
        )
    });
    let expected: Value = serde_json::from_str(&expected_raw).unwrap();
    compare_to_golden_value(&expected, actual, label);
}

/// Pretty-prints a diff and panics if the two values aren't equal.
/// Kept separate from `assert_golden` so the comparator self-test can
/// drive it without touching the filesystem.
pub fn compare_to_golden_value(expected: &Value, actual: &Value, label: &str) {
    if expected == actual {
        return;
    }
    let expected_str = serde_json::to_string_pretty(expected).unwrap();
    let actual_str = serde_json::to_string_pretty(actual).unwrap();
    let diff = line_diff(&expected_str, &actual_str);
    panic!("golden mismatch for `{label}` (run with UPDATE_GOLDEN=1 to regenerate):\n{diff}");
}

/// Minimal line-oriented diff. Avoids adding a `similar`/`difference`
/// dep for a helper only ever read on failure.
fn line_diff(a: &str, b: &str) -> String {
    let a_lines: Vec<&str> = a.lines().collect();
    let b_lines: Vec<&str> = b.lines().collect();
    let max = a_lines.len().max(b_lines.len());
    let mut out = String::new();
    for i in 0..max {
        let al = a_lines.get(i).copied().unwrap_or("");
        let bl = b_lines.get(i).copied().unwrap_or("");
        if al == bl {
            out.push_str("  ");
            out.push_str(al);
        } else {
            out.push_str("- ");
            out.push_str(al);
            out.push('\n');
            out.push_str("+ ");
            out.push_str(bl);
        }
        out.push('\n');
    }
    out
}
