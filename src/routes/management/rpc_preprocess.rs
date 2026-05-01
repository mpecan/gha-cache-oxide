//! Serde helpers that mirror upstream's
//! `z.preprocess((val) => Array.isArray(val) ? val : [val], ...)`
//! at `lib/api/cache-entries.ts:46-50`.
//!
//! `cacheEntries.match` accepts either a single string or an array of
//! strings for `scopes` and `restoreKeys`; the upstream Zod schema
//! normalises both shapes into a `string[]` before the handler runs.
//! These deserialize functions reproduce that tolerance so SDK
//! callers that send a scalar (e.g. `scopes: "main"`) don't get a
//! type error.

use serde::Deserialize;
use serde_json::Value;

/// Parses a JSON value that's either `"x"` (scalar) or `["a", "b"]`
/// (array) into a `Vec<String>`. `null` becomes an empty `Vec`.
pub(super) fn deserialize_one_or_many<'de, D>(d: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    match Value::deserialize(d)? {
        Value::String(s) => Ok(vec![s]),
        Value::Array(items) => collect_strings::<D>(items),
        Value::Null => Ok(Vec::new()),
        other => Err(D::Error::custom(format!(
            "expected string or array of strings, got {other}"
        ))),
    }
}

/// Optional variant of [`deserialize_one_or_many`]. `null` and absent
/// (via `#[serde(default)]`) both map to `None`.
pub(super) fn deserialize_one_or_many_opt<'de, D>(d: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    match Value::deserialize(d)? {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(vec![s])),
        Value::Array(items) => collect_strings::<D>(items).map(Some),
        other => Err(D::Error::custom(format!(
            "expected string, array of strings, or null, got {other}"
        ))),
    }
}

fn collect_strings<'de, D>(items: Vec<Value>) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    items
        .into_iter()
        .map(|item| match item {
            Value::String(s) => Ok(s),
            other => Err(D::Error::custom(format!(
                "expected string in array, got {other}"
            ))),
        })
        .collect()
}
