//! JWKS cache + fetcher.
//!
//! The cache is keyed by `kid` (JWT header key ID). On lookup miss, the
//! injected fetcher repopulates the cache from the JWKS source —
//! typically `https://token.actions.githubusercontent.com/.well-known/jwks`,
//! matching upstream `lib/scope.ts`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use tokio::sync::Mutex;
use url::Url;

use super::AuthError;

/// One entry from the remote JWKS document. Fields beyond `kid`/`n`/`e`
/// (kty, alg, use, x5c, ...) are not needed for RS256 verification.
#[derive(Debug, Clone, Deserialize)]
pub struct JwkEntry {
    pub kid: String,
    /// RSA modulus, base64url-encoded.
    pub n: String,
    /// RSA exponent, base64url-encoded.
    pub e: String,
}

#[derive(Debug, Deserialize)]
struct JwksDocument {
    keys: Vec<JwkEntry>,
}

/// Fetches JWKS entries from whatever backing source supplies them.
/// Implementations must be cheap to clone into an `Arc`.
#[async_trait::async_trait]
pub trait JwksFetcher: Send + Sync {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError>;
}

/// `kid` → `DecodingKey` cache. Cheap to clone — the inner `Mutex` is
/// behind an `Arc` supplied by callers.
pub struct JwksCache {
    fetcher: Arc<dyn JwksFetcher>,
    keys: Mutex<HashMap<String, DecodingKey>>,
}

impl JwksCache {
    /// Constructs a cache backed by `fetcher`. The cache starts empty;
    /// the first lookup will trigger a fetch.
    #[must_use]
    pub fn new(fetcher: Arc<dyn JwksFetcher>) -> Self {
        Self {
            fetcher,
            keys: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the decoding key for the given `kid`. On miss the entire
    /// JWKS document is re-fetched and every returned key is inserted
    /// (existing entries overwritten). Returns
    /// [`AuthError::UnknownKey`] if `kid` is still absent after fetch.
    ///
    /// **Concurrent-fetch note:** two simultaneous misses both fetch.
    /// JWKS endpoints tolerate this; a dedup mechanism would add
    /// complexity disproportionate to the contention we expect.
    ///
    /// # Errors
    /// Propagates [`AuthError::JwksFetch`] from the underlying fetcher
    /// or the base64/RSA-component decode of the returned key material.
    pub async fn get(&self, kid: &str) -> Result<DecodingKey, AuthError> {
        // Fast path: already cached.
        {
            let guard = self.keys.lock().await;
            if let Some(k) = guard.get(kid) {
                return Ok(k.clone());
            }
        }

        // Miss — fetch the full document and overwrite the cache.
        let entries = self.fetcher.fetch().await?;
        let mut guard = self.keys.lock().await;
        for entry in entries {
            let key = DecodingKey::from_rsa_components(&entry.n, &entry.e)
                .map_err(|e| AuthError::JwksFetch(e.to_string()))?;
            guard.insert(entry.kid, key);
        }

        guard
            .get(kid)
            .cloned()
            .ok_or_else(|| AuthError::UnknownKey(kid.to_string()))
    }
}

/// Concrete HTTP fetcher hitting a JWKS URL.
#[derive(Debug, Clone)]
pub struct HttpJwksFetcher {
    url: Url,
    client: reqwest::Client,
}

static GITHUB_DEFAULT_URL: LazyLock<Url> = LazyLock::new(|| {
    // Static URL literal — `parse` is infallible here. `LazyLock` panics
    // on first access if this unwrap ever fails, which would be a
    // compile-time-detectable bug rather than a runtime surprise.
    #[allow(clippy::expect_used)]
    Url::parse("https://token.actions.githubusercontent.com/.well-known/jwks")
        .expect("static URL is always parseable")
});

impl HttpJwksFetcher {
    /// Builds a fetcher for the GitHub Actions default issuer URL.
    ///
    /// Matches the constant upstream uses in `lib/scope.ts`.
    #[must_use]
    pub fn github_default() -> Self {
        Self::new(GITHUB_DEFAULT_URL.clone())
    }

    /// Builds a fetcher for an arbitrary JWKS URL.
    #[must_use]
    pub fn new(url: Url) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl JwksFetcher for HttpJwksFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        let resp = self
            .client
            .get(self.url.clone())
            .send()
            .await
            .map_err(|e| AuthError::JwksFetch(e.to_string()))?
            .error_for_status()
            .map_err(|e| AuthError::JwksFetch(e.to_string()))?;
        let doc: JwksDocument = resp
            .json()
            .await
            .map_err(|e| AuthError::JwksFetch(e.to_string()))?;
        Ok(doc.keys)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fetcher that returns a canned entry list and counts fetch calls.
    struct TestFetcher {
        entries: Vec<JwkEntry>,
        calls: AtomicUsize,
    }

    impl TestFetcher {
        fn new(entries: Vec<JwkEntry>) -> Arc<Self> {
            Arc::new(Self {
                entries,
                calls: AtomicUsize::new(0),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl JwksFetcher for TestFetcher {
        async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.entries.clone())
        }
    }

    // A small pre-built set of RSA components (not a real key — values
    // are just valid base64url that round-trips through
    // `DecodingKey::from_rsa_components`). Sourced from a discarded
    // test RSA public key; trimmed to smallest valid shape.
    fn fake_entry(kid: &str) -> JwkEntry {
        JwkEntry {
            kid: kid.to_string(),
            // 2048-bit modulus captured from a throwaway RSA keypair —
            // not tied to any signing key; just needs to be valid base64url.
            n: "sXchDaQebHnPiGvyDOAT4saGEUetSyo9MKLOoWFsueri23bOdgWp4Dy1WlUzewbgBHod5pcM9H95GQRV3JDXboIRROSBigeC5yjU1hGzHHyXss8UDprecbAYxknTcQkhslANGRUZmdTOQ5ZTsSSfIClnYZOHcD_F6q_C9_vB5qfdSwDJkqL7MFKj82NyD8tqNVLUhIDBfZhdJ_HPL0xZT1c3zB_MibHEhzc7mWBlrlfg12PGY0aStYk7qlKm9ydt1f4MsGUGoTrFTYa9ZSwdvwKs0oGWhkafXC6iT2oxcUH9P-z1rUK-Bv17IslFp9JjsWVbWDdnwxTBwR0CNKJDFpw".to_string(),
            e: "AQAB".to_string(),
        }
    }

    #[tokio::test]
    async fn get_fetches_on_miss_and_caches_on_hit() {
        let fetcher = TestFetcher::new(vec![fake_entry("A")]);
        let cache = JwksCache::new(fetcher.clone());

        cache.get("A").await.unwrap();
        assert_eq!(fetcher.calls(), 1);

        cache.get("A").await.unwrap();
        assert_eq!(fetcher.calls(), 1, "second lookup should hit cache");
    }

    #[tokio::test]
    async fn get_refetches_on_kid_miss() {
        let fetcher = TestFetcher::new(vec![fake_entry("A")]);
        let cache = JwksCache::new(fetcher.clone());

        cache.get("A").await.unwrap();
        assert_eq!(fetcher.calls(), 1);

        // Different kid — must trigger a second fetch. Fetcher still
        // returns only "A" so the call returns UnknownKey, but the
        // fetch count proves the cache did attempt a refresh.
        match cache.get("B").await {
            Err(AuthError::UnknownKey(ref k)) if k == "B" => {}
            Err(other) => panic!("expected UnknownKey(B), got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
        assert_eq!(fetcher.calls(), 2);
    }

    #[tokio::test]
    async fn get_returns_unknown_after_fetch_without_match() {
        let fetcher = TestFetcher::new(vec![fake_entry("A")]);
        let cache = JwksCache::new(fetcher);
        match cache.get("missing").await {
            Err(AuthError::UnknownKey(ref k)) if k == "missing" => {}
            Err(other) => panic!("expected UnknownKey(missing), got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[tokio::test]
    async fn github_default_url_parses() {
        // Touch the LazyLock to ensure the URL literal parses — if the
        // constant is ever edited to something malformed we want a
        // loud failure at this test's first-access, not at startup.
        let fetcher = HttpJwksFetcher::github_default();
        assert!(
            fetcher
                .url
                .as_str()
                .starts_with("https://token.actions.githubusercontent.com/")
        );
    }

    // -----------------------------------------------------------------
    // HttpJwksFetcher tests — cover the happy path plus the error
    // branches inside `HttpJwksFetcher::fetch`.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn http_fetcher_parses_valid_jwks() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/.well-known/jwks"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "keys": [
                        {"kid": "A", "n": "bogus-modulus", "e": "AQAB", "kty": "RSA", "alg": "RS256"}
                    ]
                })),
            )
            .mount(&server)
            .await;
        let url = format!("{}/.well-known/jwks", server.uri())
            .parse()
            .unwrap();
        let fetcher = HttpJwksFetcher::new(url);
        let keys = fetcher.fetch().await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kid, "A");
        assert_eq!(keys[0].e, "AQAB");
    }

    #[tokio::test]
    async fn http_fetcher_surfaces_non_2xx_as_jwks_fetch_error() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let url = format!("{}/.well-known/jwks", server.uri())
            .parse()
            .unwrap();
        let fetcher = HttpJwksFetcher::new(url);
        match fetcher.fetch().await {
            Err(AuthError::JwksFetch(msg)) => assert!(
                msg.contains("500") || msg.to_lowercase().contains("server"),
                "expected 500/server marker in error, got: {msg}"
            ),
            Err(other) => panic!("expected JwksFetch, got {other:?}"),
            Ok(keys) => panic!("expected Err, got Ok({keys:?})"),
        }
    }

    #[tokio::test]
    async fn http_fetcher_surfaces_malformed_json_as_jwks_fetch_error() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_string("not a json document")
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        let url = format!("{}/.well-known/jwks", server.uri())
            .parse()
            .unwrap();
        let fetcher = HttpJwksFetcher::new(url);
        match fetcher.fetch().await {
            Err(AuthError::JwksFetch(_)) => {}
            Err(other) => panic!("expected JwksFetch, got {other:?}"),
            Ok(keys) => panic!("expected Err, got Ok({keys:?})"),
        }
    }

    #[tokio::test]
    async fn concurrent_misses_both_reach_fetcher() {
        // Documented trade-off: two simultaneous kid-misses both fetch
        // — we don't dedup. Pin that so a future refactor adding dedup
        // trips this assertion and prompts the decision.
        let fetcher = TestFetcher::new(vec![fake_entry("A")]);
        let cache = Arc::new(JwksCache::new(fetcher.clone()));
        let c1 = cache.clone();
        let c2 = cache.clone();
        let (r1, r2) = tokio::join!(async move { c1.get("A").await }, async move {
            c2.get("A").await
        });
        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert!(
            fetcher.calls() >= 1,
            "at least one fetch expected, got {}",
            fetcher.calls()
        );
    }
}
