//! Redacting wrapper for sensitive configuration values.
//!
//! `Secret` hides its inner value from `Debug` and `Display`. Callers that
//! need the raw string must say `.expose()` explicitly — this makes accidental
//! logging a compile error rather than a runtime surprise. We deliberately do
//! not implement `AsRef<str>`, `Deref<Target=str>`, or `Into<String>`.

use std::fmt;

/// A string-valued secret that redacts itself in `Debug`/`Display` output.
///
/// Use [`Secret::expose`] when the underlying value is actually needed
/// (passing to an SDK, formatting a connection string, etc.).
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// Wraps a string as a secret.
    #[must_use]
    pub const fn new(value: String) -> Self {
        Self(value)
    }

    /// Returns the underlying value. Named to make accidental use obvious
    /// in code review.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Secret {}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW: &str = "correct-horse-battery-staple";

    #[test]
    fn debug_redacts_value() {
        let s = Secret::new(RAW.to_string());
        let formatted = format!("{s:?}");
        assert!(
            formatted.contains("[redacted]"),
            "expected [redacted] marker in {formatted}"
        );
        assert!(
            !formatted.contains(RAW),
            "raw secret leaked in Debug output: {formatted}"
        );
    }

    #[test]
    fn display_redacts_value() {
        let s = Secret::new(RAW.to_string());
        let formatted = format!("{s}");
        assert_eq!(formatted, "[redacted]");
        assert!(
            !formatted.contains(RAW),
            "raw secret leaked in Display output: {formatted}"
        );
    }

    #[test]
    fn expose_returns_inner() {
        let s = Secret::new(RAW.to_string());
        assert_eq!(s.expose(), RAW);
    }

    #[test]
    fn debug_redacts_in_parent_struct() {
        // Mimics how Secret will be used in AppConfig:
        // the whole struct's Debug must not leak the inner value.
        #[derive(Debug)]
        struct Parent {
            _token: Secret,
        }
        let p = Parent {
            _token: Secret::new(RAW.to_string()),
        };
        let formatted = format!("{p:?}");
        assert!(
            !formatted.contains(RAW),
            "parent struct leaked secret: {formatted}"
        );
    }

    #[test]
    fn eq_respects_inner_value() {
        assert_eq!(Secret::new("a".into()), Secret::new("a".into()));
        assert_ne!(Secret::new("a".into()), Secret::new("b".into()));
    }
}
