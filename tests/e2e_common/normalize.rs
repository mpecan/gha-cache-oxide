//! Envelope capture + normalization for golden comparison.
//!
//! The full flow: `capture_envelope` wraps a `reqwest::Response` as
//! `{status, headers, body}`. `normalize` walks the JSON tree and
//! replaces dynamic values (upload ids, cache-entry UUIDs, base URLs,
//! `x-ms-request-id` header values) with stable placeholders so
//! goldens don't change between runs.
//!
//! Placeholders are `<UPPER_SNAKE>` (not `{snake_case}`) to keep clippy
//! from mistaking them for stray format args.

use reqwest::Response;
use serde_json::{Value, json};

// Placeholders. Exposed for tests that want to assert against literal
// placeholder strings.
pub const PH_UPLOAD_ID: &str = "<UPLOAD_ID>";
pub const PH_CACHE_ENTRY_ID: &str = "<CACHE_ENTRY_ID>";
pub const PH_BASE_URL: &str = "<BASE_URL>";
pub const PH_REQUEST_ID: &str = "<REQUEST_ID>";

/// Dynamic values the normalizer should replace with placeholders.
#[derive(Clone)]
pub struct Dynamics {
    pub upload_id: Option<i64>,
    pub cache_entry_id: Option<String>,
}

impl Dynamics {
    pub const fn none() -> Self {
        Self {
            upload_id: None,
            cache_entry_id: None,
        }
    }
}

/// Wraps the reqwest response as a JSON envelope:
/// `{status, headers: {...}, body: <parsed-json|null>}`. Only the
/// headers in `include_headers` are surfaced — so CI-variant headers
/// like `content-length` / `date` / `connection` don't leak into
/// goldens.
pub async fn capture_envelope(
    resp: Response,
    include_headers: &[&str],
    body_is_json: bool,
) -> (u16, Value) {
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let bytes = resp.bytes().await.unwrap();

    let mut hmap = serde_json::Map::new();
    for name in include_headers {
        if let Some(v) = headers.get(*name) {
            hmap.insert(
                (*name).to_string(),
                Value::String(v.to_str().unwrap().to_string()),
            );
        }
    }

    // Fail loudly on invalid JSON — collapsing to `null` would let a
    // broken serialization regression slip past the golden check.
    let body = if bytes.is_empty() {
        Value::Null
    } else if body_is_json {
        serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            panic!(
                "expected JSON body, got error {e}; preview: {}",
                String::from_utf8_lossy(&bytes)
            );
        })
    } else {
        json!({ "body_len": bytes.len() })
    };

    (
        status,
        json!({
            "status": status,
            "headers": Value::Object(hmap),
            "body": body,
        }),
    )
}

/// Recursively rewrites dynamic values to stable placeholders. See
/// module docstring for the replacement rules.
pub fn normalize(value: Value, base_url: &str, dynamics: &Dynamics) -> Value {
    normalize_inner(value, base_url, dynamics, None)
}

fn normalize_inner(value: Value, base_url: &str, d: &Dynamics, parent_key: Option<&str>) -> Value {
    match value {
        Value::String(s) => {
            let s = normalize_string(&s, base_url, d);
            if parent_key == Some("x-ms-request-id") {
                Value::String(PH_REQUEST_ID.into())
            } else {
                Value::String(s)
            }
        }
        Value::Number(n) => {
            if let (Some(id), Some(n_val)) = (d.upload_id, n.as_i64())
                && id == n_val
            {
                Value::String(PH_UPLOAD_ID.into())
            } else {
                Value::Number(n)
            }
        }
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .map(|v| normalize_inner(v, base_url, d, None))
                .collect(),
        ),
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                let child = normalize_inner(v, base_url, d, Some(k.as_str()));
                out.insert(k, child);
            }
            Value::Object(out)
        }
        other => other,
    }
}

fn normalize_string(s: &str, base_url: &str, d: &Dynamics) -> String {
    let mut out = s.to_string();
    // Replace upload base URL paths first, then the raw base URL, then
    // the cache_entry id, so the more-specific replacements win.
    if let Some(id) = d.upload_id {
        let needle = format!("/devstoreaccount1/upload/{id}");
        let repl = format!("/devstoreaccount1/upload/{PH_UPLOAD_ID}");
        out = out.replace(&needle, &repl);
        let stringified = id.to_string();
        if out == stringified {
            return PH_UPLOAD_ID.to_string();
        }
    }
    if let Some(ref cid) = d.cache_entry_id {
        let needle = format!("/download/{cid}");
        let repl = format!("/download/{PH_CACHE_ENTRY_ID}");
        out = out.replace(&needle, &repl);
        if out == *cid {
            return PH_CACHE_ENTRY_ID.to_string();
        }
    }
    out = out.replace(base_url, PH_BASE_URL);
    out
}

// --- Extraction helpers ------------------------------------------------

/// Returns the `upload_id` embedded in a `signed_upload_url` under the
/// given base URL. Panics if the URL doesn't follow the expected shape.
pub fn extract_upload_id(create_entry_body: &Value, base_url: &str) -> i64 {
    let url = create_entry_body["signed_upload_url"]
        .as_str()
        .expect("signed_upload_url missing");
    let prefix = format!("{base_url}/devstoreaccount1/upload/");
    url.strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("expected {prefix}<id>, got {url}"))
        .parse()
        .unwrap()
}

pub fn extract_cache_entry_id(download_url_body: &Value, base_url: &str) -> String {
    let url = download_url_body["signed_download_url"]
        .as_str()
        .expect("signed_download_url missing");
    let prefix = format!("{base_url}/download/");
    url.strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("expected {prefix}<id>, got {url}"))
        .to_string()
}
