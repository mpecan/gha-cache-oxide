//! OIDC JWT authentication for GitHub Actions cache clients.
//!
//! GitHub Actions runners present an OIDC token minted by
//! `https://token.actions.githubusercontent.com`. The middleware verifies
//! it against a cached JWKS, extracts the cache scopes (the `ac` claim —
//! itself a JSON-encoded array) and the `repository_id` claim, and
//! attaches a [`CacheScope`] to the request for downstream handlers.
//!
//! This module is not applied to any route in this PR — #8 is the first
//! consumer. Issue #6 lands the middleware + state.

mod jwks;
mod middleware;

pub use jwks::{HttpJwksFetcher, JwkEntry, JwksCache, JwksFetcher};
pub use middleware::require_github_token;

use serde::Deserialize;

/// One entry in the `ac` claim.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ScopeEntry {
    #[serde(rename = "Scope")]
    pub scope: String,
    #[serde(rename = "Permission")]
    pub permission: u8,
}

/// Cache scope context attached to the request by the middleware.
/// Handlers read this through `Extension<CacheScope>`.
#[derive(Debug, Clone)]
pub struct CacheScope {
    pub scopes: Vec<ScopeEntry>,
    pub repo_id: String,
}

/// Errors raised by the auth pipeline. Every variant maps to HTTP 401.
/// Message text ports upstream's `lib/scope.ts` verbatim so operators
/// debugging mixed upstream/port deployments see identical errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Authorization header missing or malformed")]
    MissingBearer,

    #[error("Invalid token")]
    InvalidToken,

    #[error("Token does not contain cache scopes")]
    MissingScopes,

    #[error("Invalid JSON in cache scopes")]
    InvalidScopesJson,

    #[error("Token does not contain any cache scopes")]
    EmptyScopes,

    #[error("Token does not contain repository id")]
    MissingRepoId,

    #[error("JWKS backend error: {0}")]
    JwksFetch(String),

    #[error("Unknown signing key kid={0}")]
    UnknownKey(String),
}

/// JWT payload fields we care about. Anything else is ignored.
#[derive(Debug, Deserialize)]
pub(crate) struct Claims {
    /// Cache scopes, JSON-string-encoded. Upstream parses this as a
    /// string, then `JSON.parse`s it — we replicate that.
    pub(crate) ac: Option<String>,
    pub(crate) repository_id: Option<String>,
}
