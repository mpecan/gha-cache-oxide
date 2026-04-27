//! Catch-all fallback proxy for unmatched paths (issue #24).
//!
//! Mirrors upstream `routes/[...path].ts`:
//!
//! ```ts
//! return proxyRequest(event, `${env.DEFAULT_ACTIONS_RESULTS_URL}${event.path}`)
//! ```
//!
//! `actions/cache` clients reach for several endpoints beyond the cache
//! protocol surface (artifact summaries, session telemetry, etc.). Rather
//! than 404'ing them, upstream forwards the request to the receiver and
//! returns whatever the receiver answered. We do the same.
//!
//! # Behaviour
//!
//! - Method, path-and-query, body and most request headers are forwarded
//!   verbatim.
//! - Hop-by-hop headers (`Connection`, `Keep-Alive`, `Proxy-Authenticate`,
//!   `Proxy-Authorization`, `Te`, `Trailers`, `Transfer-Encoding`,
//!   `Upgrade`, `Host`) are stripped from both directions per RFC 7230 §6.1.
//! - Request bodies above [`MAX_PROXY_REQUEST_BODY`] are rejected with
//!   `413 Payload Too Large` — guards against accidental denial of
//!   service through the proxy.
//! - Upstream responses are streamed straight back via
//!   `axum::body::Body::from_stream` (reqwest `bytes_stream`).
//! - Reqwest transport errors map to `502 Bad Gateway` and are logged.

use std::sync::OnceLock;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::Response;
use futures::TryStreamExt;
use reqwest::Client;

use crate::routes::errors::error_response;
use crate::state::AppState;

/// Upper bound on a proxied request body (16 MiB). The fallback exists
/// for the small JSON RPCs `actions/cache` makes outside the cache
/// protocol; cache uploads use the explicit blob routes and never
/// reach this handler.
pub const MAX_PROXY_REQUEST_BODY: usize = 16 * 1024 * 1024;

/// Process-wide reqwest client for proxied requests. Reused across
/// invocations so connection pools / DNS caches stick around.
fn proxy_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            // Don't auto-follow redirects — the catch-all reflects upstream's
            // status verbatim, so a 302 should propagate to the client and
            // not become a transparent GET against `Location`.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|err| {
                // Surface the underlying TLS / DNS / config failure once
                // before degrading to defaults — the constitution forbids
                // silent fallbacks. `Client::new()` itself can't fail in
                // practice; if it ever did, reqwest would have already
                // panicked.
                tracing::error!(
                    error = %err,
                    "proxy: Client::builder() failed; falling back to Client::new() with library defaults",
                );
                Client::new()
            })
    })
}

pub async fn fallback(State(state): State<AppState>, req: Request<Body>) -> Response {
    let target_url = build_target_url(&state, &req);

    let (parts, body) = req.into_parts();

    let Ok(body_bytes) = axum::body::to_bytes(body, MAX_PROXY_REQUEST_BODY).await else {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds proxy size limit",
        );
    };

    // Pass `Bytes` straight through to reqwest — `body_bytes` already
    // owns one cheap reference; copying to a `Vec<u8>` would double the
    // peak allocation up to `MAX_PROXY_REQUEST_BODY` (16 MiB).
    let mut request_builder = proxy_client()
        .request(parts.method.clone(), &target_url)
        .body(body_bytes);
    for (name, value) in &parts.headers {
        if !is_hop_by_hop(name) {
            request_builder = request_builder.header(name, value);
        }
    }

    let upstream = match request_builder.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                target = %target_url,
                "proxy: upstream request failed",
            );
            return error_response(StatusCode::BAD_GATEWAY, "upstream unreachable");
        }
    };

    proxy_response(upstream)
}

fn build_target_url(state: &AppState, req: &Request<Body>) -> String {
    let base = state
        .config
        .default_actions_results_url
        .as_str()
        .trim_end_matches('/');
    let path_and_query = req
        .uri()
        .path_and_query()
        .map_or_else(|| req.uri().path().to_string(), ToString::to_string);
    format!("{base}{path_and_query}")
}

fn proxy_response(upstream: reqwest::Response) -> Response {
    let status = upstream.status();
    let mut response_headers = HeaderMap::new();
    for (name, value) in upstream.headers() {
        if !is_hop_by_hop(name) && name != reqwest::header::CONTENT_LENGTH {
            // axum recomputes Content-Length from the streamed body when
            // it has a fixed size; copying upstream's value can desync if
            // we ever decode/transform the body. Drop it for safety.
            response_headers.insert(name, value.clone());
        }
    }

    let mapped = upstream.bytes_stream().map_err(std::io::Error::other);
    let body = Body::from_stream(mapped);

    let mut builder = Response::builder().status(map_status(status));
    if let Some(headers) = builder.headers_mut() {
        *headers = response_headers;
    }
    builder.body(body).unwrap_or_else(|_| {
        tracing::error!("proxy: failed to assemble axum response");
        error_response(StatusCode::BAD_GATEWAY, "proxy response assembly failed")
    })
}

fn map_status(status: reqwest::StatusCode) -> StatusCode {
    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY)
}

/// Returns true for hop-by-hop headers per RFC 7230 §6.1, plus
/// `Host` (we set the URL ourselves; forwarding the inbound `Host`
/// would point reqwest at the wrong virtual host on the upstream
/// server).
fn is_hop_by_hop(name: &HeaderName) -> bool {
    static HOP_BY_HOP: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
    ];
    HOP_BY_HOP
        .iter()
        .any(|h| h.eq_ignore_ascii_case(name.as_str()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::is_hop_by_hop;
    use axum::http::HeaderName;

    #[test]
    fn hop_by_hop_recognises_rfc7230_set() {
        for h in [
            "Connection",
            "keep-alive",
            "Proxy-Authenticate",
            "TE",
            "Trailers",
            "Transfer-Encoding",
            "Upgrade",
            "Host",
        ] {
            let n = HeaderName::from_bytes(h.as_bytes()).unwrap();
            assert!(is_hop_by_hop(&n), "{h} must be classified hop-by-hop");
        }
    }

    #[test]
    fn end_to_end_headers_pass_through() {
        for h in [
            "content-type",
            "content-length",
            "user-agent",
            "x-custom-thing",
            "authorization",
        ] {
            let n = HeaderName::from_bytes(h.as_bytes()).unwrap();
            assert!(!is_hop_by_hop(&n), "{h} must NOT be hop-by-hop");
        }
    }
}
