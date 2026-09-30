//! Small helpers shared by the v1 handlers: byte-counting body
//! wrapper, `Content-Range` parsing, and act-shaped error responses.

use std::io;
use std::sync::Arc;

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use futures::{StreamExt, TryStreamExt};

use super::auth::ForgejoRun;
use super::json_error;
use crate::metrics::SourceCounters;
use crate::storage::ByteStream;

/// Parses act's supported `Content-Range` shape, `bytes <start>-<end>/*`
/// (the total may be anything, and is ignored).
pub(super) fn parse_content_range(s: &str) -> Option<(u64, u64)> {
    let s = s.strip_prefix("bytes ").unwrap_or(s);
    let range = s.split_once('/').map_or(s, |(r, _)| r);
    let (a, b) = range.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// Wraps `body` in a stream that reports every chunk's length to
/// `record` as it passes through.
pub(super) fn counted(
    body: Body,
    source: Arc<SourceCounters>,
    record: fn(&SourceCounters, u64),
) -> ByteStream {
    body.into_data_stream()
        .inspect_ok(move |chunk| record(&source, chunk.len() as u64))
        .map_err(io::Error::other)
        .boxed()
}

pub(super) fn not_reserved(id: impl std::fmt::Display) -> Response {
    json_error(StatusCode::NOT_FOUND, &format!("cache {id}: not reserved"))
}

pub(super) fn isolation_mismatch(run: &ForgejoRun, entry_scope: &str) -> Response {
    json_error(
        StatusCode::FORBIDDEN,
        &format!(
            "cache authorized for write isolation {:?}, but attempting to operate on {entry_scope:?}",
            run.write_isolation_key
        ),
    )
}

pub(super) fn internal(e: &dyn std::fmt::Display) -> Response {
    tracing::error!(error = %e, "forgejo cache: internal error");
    json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

#[cfg(test)]
mod tests {
    use super::parse_content_range;

    #[test]
    fn parses_act_content_range_shapes() {
        assert_eq!(parse_content_range("bytes 0-99/*"), Some((0, 99)));
        assert_eq!(parse_content_range("bytes 100-199/200"), Some((100, 199)));
        assert_eq!(parse_content_range("bytes 5-9"), Some((5, 9)));
    }

    #[test]
    fn rejects_malformed_content_range() {
        for s in [
            "",
            "bytes",
            "bytes -1/*",
            "bytes a-b/*",
            "bytes 0-/*",
            "bytes 1/*",
        ] {
            assert_eq!(parse_content_range(s), None, "{s:?}");
        }
    }
}
