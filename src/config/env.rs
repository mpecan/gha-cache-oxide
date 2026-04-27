//! Shared env-var parsing helpers.
//!
//! All three config sub-modules (base, storage, db) converge on the same
//! small vocabulary: "this var is required", "this var is optional",
//! "this u16 field". Hoisting them here keeps the semantics uniform —
//! in particular, **empty string is treated as unset** everywhere, so
//! `FOO=""` behaves exactly like `FOO` being missing and never silently
//! produces a zero-valued field.

use std::num::ParseIntError;

use super::Secret;
use super::error::ConfigError;

/// Reads a required env var. Returns [`ConfigError::Missing`] when unset
/// **or empty**.
pub(super) fn required(var: &'static str) -> Result<String, ConfigError> {
    match std::env::var(var) {
        Ok(s) if !s.is_empty() => Ok(s),
        _ => Err(ConfigError::Missing { var }),
    }
}

/// Reads an optional env var. Empty string is treated as unset.
pub(super) fn optional(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

/// Reads an optional env var and wraps it as a [`Secret`].
pub(super) fn optional_secret(var: &str) -> Option<Secret> {
    optional(var).map(Secret::new)
}

/// Reads a required u16 (e.g. port). Empty/missing → `Missing`; non-numeric
/// or overflow → `Invalid`.
pub(super) fn required_u16(var: &'static str) -> Result<u16, ConfigError> {
    let raw = required(var)?;
    raw.parse::<u16>().map_err(|e| invalid(var, raw, &e))
}

/// Reads an optional URL. Empty/missing → `Ok(None)`. Malformed → `InvalidUrl`.
pub(super) fn optional_url(var: &'static str) -> Result<Option<url::Url>, ConfigError> {
    optional(var)
        .map(|s| url::Url::parse(&s).map_err(|source| ConfigError::InvalidUrl { var, source }))
        .transpose()
}

/// Reads a required URL.
pub(super) fn required_url(var: &'static str) -> Result<url::Url, ConfigError> {
    let raw = required(var)?;
    url::Url::parse(&raw).map_err(|source| ConfigError::InvalidUrl { var, source })
}

/// Reads a URL env var with a static default. Empty/missing → parse
/// `default`. Malformed → [`ConfigError::InvalidUrl`].
///
/// `default` is taken `&'static str` because every call site has a
/// schema literal — saves an allocation in the happy path.
///
/// # Panics
/// Never. The default is parsed eagerly; callers that pass a malformed
/// default would surface it as `InvalidUrl` from the parser. In
/// practice the only call site (`DEFAULT_ACTIONS_RESULTS_URL`) is
/// covered by a unit test.
pub(super) fn url_with_default(
    var: &'static str,
    default: &'static str,
) -> Result<url::Url, ConfigError> {
    let raw = optional(var);
    let s = raw.as_deref().unwrap_or(default);
    url::Url::parse(s).map_err(|source| ConfigError::InvalidUrl { var, source })
}

/// Reads a boolean env var with a default. Only `"true"` / `"false"` parse;
/// empty/missing → `Ok(default)`; anything else → `InvalidBool`.
pub(super) fn bool_or_default(var: &'static str, default: bool) -> Result<bool, ConfigError> {
    match optional(var).as_deref() {
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(other) => Err(ConfigError::InvalidBool {
            var,
            value: other.to_string(),
        }),
        None => Ok(default),
    }
}

/// Reads a u32 env var with a default. Empty/missing → `Ok(default)`;
/// non-numeric → `Invalid`.
pub(super) fn u32_or_default(var: &'static str, default: u32) -> Result<u32, ConfigError> {
    optional(var).map_or(Ok(default), |s| {
        s.parse::<u32>().map_err(|e| invalid(var, s, &e))
    })
}

fn invalid(var: &'static str, value: String, source: &ParseIntError) -> ConfigError {
    ConfigError::Invalid {
        var,
        value,
        reason: source.to_string(),
    }
}
