//! Serde helpers that mirror oRPC's
//! `z.preprocess((val) => Array.isArray(val) ? val : [val], ...)`
//! tolerance.
//!
//! Many upstream procedures accept either a single string or an
//! array of strings on the wire (e.g. `cacheEntries.match`'s
//! `scopes`/`restoreKeys` at upstream `lib/api/cache-entries.ts:46-50`).
//! Use these via `#[serde(deserialize_with = "...")]`.

use serde::Deserialize;
use serde_json::Value;

/// Deserialize a JSON value that's either a single string OR a JSON
/// array of strings into a `Vec<String>`. `null` becomes an empty
/// `Vec`.
///
/// ```ignore
/// #[derive(serde::Deserialize)]
/// struct Input {
///     #[serde(deserialize_with = "orpc_server::preprocess::one_or_many")]
///     scopes: Vec<String>,
/// }
/// ```
///
/// # Errors
///
/// Returns the deserializer's error if the JSON value is none of
/// `string` / `array of strings` / `null`, or if any array element
/// is not a string.
pub fn one_or_many<'de, D>(d: D) -> Result<Vec<String>, D::Error>
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

/// Optional variant of [`one_or_many`]. `null` and absent (via
/// `#[serde(default)]`) both map to `None`.
///
/// # Errors
///
/// Returns the deserializer's error if the JSON value is none of
/// `string` / `array of strings` / `null`, or if any array element
/// is not a string.
pub fn one_or_many_opt<'de, D>(d: D) -> Result<Option<Vec<String>>, D::Error>
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{one_or_many, one_or_many_opt};
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize, Debug, PartialEq, Eq)]
    struct Required {
        #[serde(deserialize_with = "one_or_many")]
        v: Vec<String>,
    }

    #[derive(Deserialize, Debug, PartialEq, Eq)]
    struct Optional {
        #[serde(default, deserialize_with = "one_or_many_opt")]
        v: Option<Vec<String>>,
    }

    #[test]
    fn scalar_becomes_one_element_vec() {
        let parsed: Required = serde_json::from_value(json!({ "v": "x" })).unwrap();
        assert_eq!(parsed.v, vec!["x".to_string()]);
    }

    #[test]
    fn array_passes_through() {
        let parsed: Required = serde_json::from_value(json!({ "v": ["a", "b"] })).unwrap();
        assert_eq!(parsed.v, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn null_required_becomes_empty_vec() {
        let parsed: Required = serde_json::from_value(json!({ "v": null })).unwrap();
        assert_eq!(parsed.v, Vec::<String>::new());
    }

    #[test]
    fn non_string_array_element_errors() {
        let err = serde_json::from_value::<Required>(json!({ "v": ["a", 7] })).unwrap_err();
        assert!(err.to_string().contains("expected string in array"));
    }

    #[test]
    fn optional_scalar_becomes_some_one_element_vec() {
        let parsed: Optional = serde_json::from_value(json!({ "v": "x" })).unwrap();
        assert_eq!(parsed.v, Some(vec!["x".to_string()]));
    }

    #[test]
    fn optional_null_becomes_none() {
        let parsed: Optional = serde_json::from_value(json!({ "v": null })).unwrap();
        assert_eq!(parsed.v, None);
    }

    #[test]
    fn optional_absent_becomes_none() {
        let parsed: Optional = serde_json::from_value(json!({})).unwrap();
        assert_eq!(parsed.v, None);
    }

    #[test]
    fn optional_array_passes_through() {
        let parsed: Optional = serde_json::from_value(json!({ "v": ["a", "b"] })).unwrap();
        assert_eq!(parsed.v, Some(vec!["a".to_string(), "b".to_string()]));
    }
}
