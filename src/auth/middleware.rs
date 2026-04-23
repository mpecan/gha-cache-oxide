//! `require_github_token` axum middleware.
//!
//! Extracts the `Authorization: Bearer` header, verifies the JWT against
//! the cached JWKS (unless `skip_token_validation` is on), parses the
//! `ac` claim (JSON-encoded array of scopes per upstream) and
//! `repository_id`, and attaches a [`CacheScope`] to the request
//! extensions for downstream handlers.

use std::sync::Once;

use axum::Json;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::json;

use super::{AuthError, CacheScope, Claims, ScopeEntry};
use crate::state::AppState;

/// GitHub Actions OIDC issuer. Matches upstream `lib/scope.ts`
/// (`jose.jwtVerify(..., { issuer: ... })`).
const GITHUB_ISSUER: &str = "https://token.actions.githubusercontent.com";

/// Axum middleware enforcing a valid GitHub Actions bearer token.
///
/// On success attaches [`CacheScope`] to the request extensions. On
/// failure responds with 401 and a JSON body matching upstream's
/// h3 `createError` shape (`{ "statusCode": 401, "message": "..." }`).
///
/// # Errors
/// The function signature is `Result`-shaped only to match axum's
/// middleware contract; failures are always conveyed as a 401 response,
/// never as a propagated error type.
pub async fn require_github_token(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    match authenticate(&state, req.headers()).await {
        Ok(scope) => {
            req.extensions_mut().insert(scope);
            next.run(req).await
        }
        Err(err) => unauthorized_response(&err),
    }
}

async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<CacheScope, AuthError> {
    let token = extract_bearer(headers)?;
    let claims = if state.config.skip_token_validation {
        log_skip_warning_once();
        decode_unverified(token)?
    } else {
        verify_signed(state, token).await?
    };
    build_cache_scope(claims)
}

/// Pulls the token out of the Authorization header. Matches upstream's
/// `Bearer ` prefix convention literally.
fn extract_bearer(headers: &HeaderMap) -> Result<&str, AuthError> {
    let header_value = headers
        .get(header::AUTHORIZATION)
        .ok_or(AuthError::MissingBearer)?
        .to_str()
        .map_err(|_| AuthError::MissingBearer)?;
    header_value
        .strip_prefix("Bearer ")
        .ok_or(AuthError::MissingBearer)
}

/// Decodes the payload without any signature check, and **also skips
/// expiry / not-before / audience / required-claims enforcement** —
/// used only when `skip_token_validation` is true, to pull the
/// scope/repo-id claims out of the payload. Do not use outside that
/// code path.
fn decode_unverified(token: &str) -> Result<Claims, AuthError> {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.insecure_disable_signature_validation();
    validation.validate_aud = false;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    // Empty `required_spec_claims` — we only want the payload.
    validation.required_spec_claims = std::collections::HashSet::new();
    let token_data = decode::<Claims>(token, &DecodingKey::from_secret(b""), &validation)
        .map_err(|_| AuthError::InvalidToken)?;
    Ok(token_data.claims)
}

/// Decodes and verifies a signed RS256 JWT. Resolves the key from the
/// JWKS cache via the `kid` header.
async fn verify_signed(state: &AppState, token: &str) -> Result<Claims, AuthError> {
    let header = decode_header(token).map_err(|_| AuthError::InvalidToken)?;
    let kid = header.kid.ok_or(AuthError::InvalidToken)?;
    let key = state.jwks.get(&kid).await?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[GITHUB_ISSUER]);
    // `aud` is not enforced upstream, and every deployment will mint with
    // its own audience via the `aud` / `audience` param to actions/core —
    // we trust the scope claim to gate access instead.
    validation.validate_aud = false;
    // `exp` is on by default. Also enable `nbf` so tokens whose
    // not-before claim is in the future are rejected — upstream's
    // `jose.jwtVerify` enforces `nbf` per RFC 7519 when present.
    validation.validate_nbf = true;

    let token_data =
        decode::<Claims>(token, &key, &validation).map_err(|_| AuthError::InvalidToken)?;
    Ok(token_data.claims)
}

/// Turns a `Claims` payload into the runtime `CacheScope` used by handlers.
fn build_cache_scope(claims: Claims) -> Result<CacheScope, AuthError> {
    let ac = claims.ac.ok_or(AuthError::MissingScopes)?;
    let scopes: Vec<ScopeEntry> =
        serde_json::from_str(&ac).map_err(|_| AuthError::InvalidScopesJson)?;
    if scopes.is_empty() {
        return Err(AuthError::EmptyScopes);
    }
    let repo_id = claims.repository_id.ok_or(AuthError::MissingRepoId)?;
    if repo_id.is_empty() {
        return Err(AuthError::MissingRepoId);
    }
    Ok(CacheScope { scopes, repo_id })
}

/// Builds a 401 response with a body shaped like upstream's h3
/// `createError` output so mixed-deployment operators see consistent
/// error payloads.
fn unauthorized_response(err: &AuthError) -> Response {
    let body = Json(json!({
        "statusCode": 401,
        "message": err.to_string(),
    }));
    (StatusCode::UNAUTHORIZED, body).into_response()
}

/// `std::sync::Once` plus a probe counter the tests read to assert the
/// warning emits exactly once per process lifetime.
static SKIP_WARNING: Once = Once::new();
static SKIP_WARNING_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn log_skip_warning_once() {
    SKIP_WARNING.call_once(|| {
        SKIP_WARNING_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::warn!(
            "SKIP_TOKEN_VALIDATION=true — tokens are decoded without signature \
             verification. Do not use in production."
        );
    });
}

#[cfg(test)]
fn skip_warning_count() -> usize {
    SKIP_WARNING_COUNT.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(test)]
#[path = "middleware_tests.rs"]
mod tests;
