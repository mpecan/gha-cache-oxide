//! Forgejo runner cache-proxy authentication.
//!
//! The runner's cache proxy (`act/cacheproxy/handler.go`) stamps every
//! request it forwards with `Forgejo-Cache-*` headers, including
//! `Forgejo-Cache-MAC = hex(HMAC-SHA256(secret, repo ">" runNumber ">"
//! timestamp ">" writeIsolationKey))`. Ports `act/artifactcache/mac.go`:
//! the request is rejected with 403 when the MAC does not verify or the
//! timestamp is unparsable / in the future. The MAC comparison is
//! constant-time (`Mac::verify_slice`).

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use super::json_error;
use crate::state::AppState;

type HmacSha256 = Hmac<Sha256>;

/// Prefix applied to the MAC-validated repository name before it is
/// stored as `repoId`. The v2 surface stores numeric GitHub repository
/// ids there; the prefix keeps the two id spaces from ever colliding.
pub(super) const REPO_ID_PREFIX: &str = "forgejo:";

/// Validated run identity, attached as a request extension by
/// [`require_forgejo_mac`].
#[derive(Debug, Clone)]
pub(super) struct ForgejoRun {
    /// `repoId` column value: [`REPO_ID_PREFIX`] + `Forgejo-Cache-Repo`.
    pub repo_id: String,
    /// `Forgejo-Cache-WriteIsolationKey`, `""` when absent. Stored in
    /// the `scope` column.
    pub write_isolation_key: String,
    /// `Forgejo-Cache-Host` — the proxy's externally reachable base URL.
    pub proxy_host: String,
    /// `Forgejo-Cache-RunId` — the per-run path prefix the proxy strips.
    pub run_id: String,
}

impl ForgejoRun {
    /// The repository (`owner/name`) this run authenticated for, i.e.
    /// `repo_id` without the storage prefix. Used as a metrics label.
    pub(super) fn repo(&self) -> &str {
        self.repo_id
            .strip_prefix(REPO_ID_PREFIX)
            .unwrap_or(&self.repo_id)
    }
}

/// Computes the hex MAC exactly like act's `ComputeMac`.
///
/// Public so integration tests (and operators debugging a secret mismatch) can
/// mint the same value the runner does. `None` only if the HMAC
/// implementation rejects the key, which HMAC-SHA256 never does.
pub fn compute_mac(
    secret: &str,
    repo: &str,
    run_number: &str,
    timestamp: &str,
    write_isolation_key: &str,
) -> Option<String> {
    let parts = [repo, run_number, timestamp, write_isolation_key].map(str::as_bytes);
    let mac = mac_for(secret, parts)?;
    Some(hex::encode(mac.finalize().into_bytes()))
}

/// MAC over raw bytes: Go sends header values byte-for-byte, and a git
/// ref used as the write-isolation key need not be UTF-8.
fn mac_for(secret: &str, parts: [&[u8]; 4]) -> Option<HmacSha256> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            mac.update(b">");
        }
        mac.update(part);
    }
    Some(mac)
}

fn header_bytes<'a>(headers: &'a HeaderMap, name: &str) -> &'a [u8] {
    headers.get(name).map_or(&[], |v| v.as_bytes())
}

fn header(headers: &HeaderMap, name: &str) -> String {
    String::from_utf8_lossy(header_bytes(headers, name)).into_owned()
}

/// Validates the `Forgejo-Cache-*` headers against `secret` at
/// `now_secs`. Returns the run identity on success, `None` on any
/// validation failure (act collapses them all into one error, too).
pub(super) fn validate(headers: &HeaderMap, secret: &str, now_secs: i64) -> Option<ForgejoRun> {
    let repo = header_bytes(headers, "forgejo-cache-repo");
    let run_number = header_bytes(headers, "forgejo-cache-runnumber");
    let timestamp = header_bytes(headers, "forgejo-cache-timestamp");
    let wik = header_bytes(headers, "forgejo-cache-writeisolationkey");
    let mac_hex = header_bytes(headers, "forgejo-cache-mac");
    // act compares lowercase hex strings; `hex::decode` alone would also
    // accept uppercase.
    if mac_hex.iter().any(u8::is_ascii_uppercase) {
        return None;
    }
    let provided = hex::decode(mac_hex).ok()?;

    let ts: i64 = std::str::from_utf8(timestamp).ok()?.parse().ok()?;
    if ts > now_secs {
        return None;
    }
    mac_for(secret, [repo, run_number, timestamp, wik])?
        .verify_slice(&provided)
        .ok()?;

    Some(ForgejoRun {
        repo_id: format!("{REPO_ID_PREFIX}{}", String::from_utf8_lossy(repo)),
        write_isolation_key: String::from_utf8_lossy(wik).into_owned(),
        proxy_host: header(headers, "forgejo-cache-host"),
        run_id: header(headers, "forgejo-cache-runid"),
    })
}

/// Axum middleware: 403 `{"error":"validation error"}` unless the
/// request carries a valid MAC; otherwise attaches [`ForgejoRun`].
pub(super) async fn require_forgejo_mac(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(secret) = state.config.forgejo_cache_secret.as_ref() else {
        // The router is only mounted when the secret is configured.
        return json_error(StatusCode::FORBIDDEN, "validation error");
    };
    let now_secs = crate::db::id::now_ms() / 1000;
    let Some(run) = validate(req.headers(), secret.expose(), now_secs) else {
        state.metrics.forgejo.auth_failures.inc();
        tracing::info!(
            repo = %header(req.headers(), "forgejo-cache-repo"),
            path = %req.uri().path(),
            "forgejo cache: MAC validation failed",
        );
        return json_error(StatusCode::FORBIDDEN, "validation error");
    };
    req.extensions_mut().insert(run);
    next.run(req).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const SECRET: &str = "secret for testing";

    fn headers(repo: &str, run: &str, ts: &str, wik: &str, mac: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("forgejo-cache-repo", repo),
            ("forgejo-cache-runnumber", run),
            ("forgejo-cache-timestamp", ts),
            ("forgejo-cache-mac", mac),
            ("forgejo-cache-host", "http://proxy:1234"),
            ("forgejo-cache-runid", "abc"),
        ] {
            h.insert(k, HeaderValue::from_str(v).unwrap());
        }
        if !wik.is_empty() {
            h.insert(
                "forgejo-cache-writeisolationkey",
                HeaderValue::from_str(wik).unwrap(),
            );
        }
        h
    }

    // Vectors precomputed in act/artifactcache/mac_test.go ("compute correct mac").
    #[test]
    fn compute_mac_matches_act_vectors() {
        let secret = "this is my cool secret string :3";
        assert_eq!(
            compute_mac(secret, "org/reponame", "42", "1337", "").unwrap(),
            "4754474b21329e8beadd2b4054aa4be803965d66e710fa1fee091334ed804f29"
        );
        assert_eq!(
            compute_mac(secret, "org/reponame", "42", "1337", "refs/pull/12/head").unwrap(),
            "9ca8f4cb5e1b083ee8cd215215bc00f379b28511d3ef7930bf054767de34766d"
        );
        // handler_test.go's fixed `cacheMac`.
        assert_eq!(
            compute_mac("secret", "testuser/repo", "1", "0", "").unwrap(),
            "bc2e9167f9e310baebcead390937264e4c0b21d2fdd49f5b9470d54406099360"
        );
    }

    #[test]
    fn validate_correct_mac() {
        let mac = compute_mac(SECRET, "org/reponame", "1", "1000", "").unwrap();
        let run = validate(
            &headers("org/reponame", "1", "1000", "", &mac),
            SECRET,
            1000,
        )
        .unwrap();
        assert_eq!(run.repo_id, "forgejo:org/reponame");
        assert_eq!(run.write_isolation_key, "");
        assert_eq!(run.proxy_host, "http://proxy:1234");
        assert_eq!(run.run_id, "abc");
    }

    #[test]
    fn validate_carries_write_isolation_key() {
        let mac = compute_mac(SECRET, "o/r", "1", "1000", "refs/pull/1/head").unwrap();
        let run = validate(
            &headers("o/r", "1", "1000", "refs/pull/1/head", &mac),
            SECRET,
            2000,
        )
        .unwrap();
        assert_eq!(run.write_isolation_key, "refs/pull/1/head");
    }

    #[test]
    fn validate_rejects_future_timestamp() {
        let ts = "9223372036854775807";
        let mac = compute_mac(SECRET, "org/reponame", "1", ts, "").unwrap();
        assert!(validate(&headers("org/reponame", "1", ts, "", &mac), SECRET, 1000).is_none());
    }

    #[test]
    fn validate_rejects_unparsable_timestamp() {
        let mac = compute_mac(SECRET, "o/r", "1", "soon", "").unwrap();
        assert!(validate(&headers("o/r", "1", "soon", "", &mac), SECRET, 1000).is_none());
    }

    #[test]
    fn validate_rejects_incorrect_mac() {
        let h = headers(
            "org/reponame",
            "1",
            "1000",
            "",
            "this is not the right mac :D",
        );
        assert!(validate(&h, SECRET, 1000).is_none());
    }

    #[test]
    fn validate_rejects_mac_from_other_secret() {
        let mac = compute_mac("other", "o/r", "1", "1000", "").unwrap();
        assert!(validate(&headers("o/r", "1", "1000", "", &mac), SECRET, 1000).is_none());
    }

    #[test]
    fn validate_rejects_tampered_isolation_key() {
        // MAC minted without an isolation key must not authorise one.
        let mac = compute_mac(SECRET, "o/r", "1", "1000", "").unwrap();
        assert!(
            validate(
                &headers("o/r", "1", "1000", "refs/heads/main", &mac),
                SECRET,
                1000
            )
            .is_none()
        );
    }

    #[test]
    fn validate_rejects_uppercase_mac_like_act() {
        let mac = compute_mac(SECRET, "o/r", "1", "1000", "")
            .unwrap()
            .to_uppercase();
        assert!(validate(&headers("o/r", "1", "1000", "", &mac), SECRET, 1000).is_none());
    }

    #[test]
    fn validate_rejects_truncated_mac() {
        let mac = compute_mac(SECRET, "o/r", "1", "1000", "").unwrap();
        let h = headers("o/r", "1", "1000", "", &mac[..32]);
        assert!(validate(&h, SECRET, 1000).is_none());
    }

    #[test]
    fn validate_accepts_non_utf8_isolation_key() {
        let wik: &[u8] = b"refs/heads/caf\xe9";
        let mac = hex::encode(
            mac_for(SECRET, [b"o/r", b"1", b"1000", wik])
                .unwrap()
                .finalize()
                .into_bytes(),
        );
        let mut h = headers("o/r", "1", "1000", "", &mac);
        h.insert(
            "forgejo-cache-writeisolationkey",
            HeaderValue::from_bytes(wik).unwrap(),
        );
        let run = validate(&h, SECRET, 1000).unwrap();
        assert_eq!(run.write_isolation_key, "refs/heads/caf\u{fffd}");
    }

    #[test]
    fn validate_rejects_missing_headers() {
        assert!(validate(&HeaderMap::new(), SECRET, 1000).is_none());
    }
}
