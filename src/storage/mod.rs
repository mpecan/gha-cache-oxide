//! Object storage abstraction and drivers.
//!
//! The [`StorageAdapter`] trait abstracts over filesystem / S3 / GCS /
//! Azure backends — all served by the `object_store` crate. Filesystem
//! and S3 are wired up (#5, #12); GCS is deferred.
//!
//! # Dyn-compatibility
//!
//! We use the `#[async_trait]` macro rather than Rust 1.75+ native AFIT.
//! Native AFIT compiles but isn't `dyn`-compatible without extra scaffolding
//! (`trait-variant` or per-driver enum dispatch). `async-trait` desugars
//! every method to `Box<dyn Future>`, which lets `AppState` hold
//! `Arc<dyn StorageAdapter>` — the cost is one allocation per method call,
//! negligible next to filesystem / network round-trips.

mod filesystem;
mod s3;

pub use filesystem::FilesystemAdapter;
pub use s3::{S3Adapter, S3Config};

use bytes::Bytes;
use futures::stream::BoxStream;
use url::Url;

/// Stream of byte chunks handed to `upload_stream` or returned by
/// `download_stream`. The error type matches what `object_store` produces
/// natively to avoid a layer of wrapping in the hot path.
pub type ByteStream = BoxStream<'static, Result<Bytes, std::io::Error>>;

/// Errors produced by the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    ObjectNotFound(String),

    #[error("invalid object name {name:?}: {reason}")]
    InvalidObjectName { name: String, reason: &'static str },

    /// S3 bucket is unreachable at startup — missing, wrong region, or
    /// credential denied. Surfaced loudly during adapter construction so
    /// operators see the bucket name, not a generic "S3 error" on the
    /// first cache request.
    #[error("s3 bucket {bucket:?} is unavailable: {source}")]
    BucketUnavailable {
        bucket: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("storage backend error: {0}")]
    Backend(#[from] object_store::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Abstracts an object-storage backend.
///
/// Callers address objects by flat string paths (`"folder/file"`).
/// Implementations MUST reject traversal attempts (`".."` segments,
/// absolute paths) via the crate-internal `validate_object_name` helper.
#[async_trait::async_trait]
pub trait StorageAdapter: Send + Sync {
    /// Streams `body` into the object at `object_name`. Overwrites on
    /// collision. Parent "directories" are created implicitly.
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError>;

    /// Returns a byte stream for reading `object_name`. Errors with
    /// [`StorageError::ObjectNotFound`] if the object does not exist.
    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError>;

    /// Deletes every object under `folder_name/`. Missing folder is not
    /// an error (no-op).
    async fn delete_folder(&self, folder_name: &str) -> Result<(), StorageError>;

    /// Counts files directly under `folder_name/`. Returns 0 for a missing
    /// folder rather than an error.
    async fn count_files_in_folder(&self, folder_name: &str) -> Result<u64, StorageError>;

    /// Returns a time-limited download URL for direct-to-client download,
    /// or `Ok(None)` if the backend cannot sign URLs (filesystem).
    async fn signed_url(&self, object_name: &str) -> Result<Option<Url>, StorageError>;

    /// Removes every object this adapter manages. Used by tests and
    /// administrative clear operations.
    async fn clear(&self) -> Result<(), StorageError>;
}

/// Validates an object name before it's handed to the backend.
///
/// Rejects empty names, absolute paths, empty segments (double slash),
/// and `.`/`..` path components so no driver has to re-prove this
/// invariant. `object_store::path::Path` sanitises some of this too, but
/// we fail loud rather than silently normalise.
pub(crate) fn validate_object_name(name: &str) -> Result<(), StorageError> {
    if name.is_empty() {
        return Err(StorageError::InvalidObjectName {
            name: String::new(),
            reason: "empty",
        });
    }
    if name.starts_with('/') {
        return Err(StorageError::InvalidObjectName {
            name: name.to_string(),
            reason: "absolute path",
        });
    }
    for segment in name.split('/') {
        match segment {
            "" => {
                return Err(StorageError::InvalidObjectName {
                    name: name.to_string(),
                    reason: "empty segment (double slash)",
                });
            }
            "." | ".." => {
                return Err(StorageError::InvalidObjectName {
                    name: name.to_string(),
                    reason: "relative path component",
                });
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn accepts_normal_object_names() {
        assert!(validate_object_name("folder/file.bin").is_ok());
        assert!(validate_object_name("a/b/c/d/e").is_ok());
        assert!(validate_object_name("single-segment").is_ok());
        assert!(validate_object_name("with spaces and (punct)").is_ok());
    }

    #[test]
    fn rejects_empty() {
        let err = validate_object_name("").unwrap_err();
        assert!(matches!(
            err,
            StorageError::InvalidObjectName {
                reason: "empty",
                ..
            }
        ));
    }

    #[test]
    fn rejects_absolute_path() {
        let err = validate_object_name("/etc/passwd").unwrap_err();
        assert!(matches!(
            err,
            StorageError::InvalidObjectName {
                reason: "absolute path",
                ..
            }
        ));
    }

    #[test]
    fn rejects_dotdot_segments() {
        for s in ["..", "../etc", "foo/../bar", "a/b/c/../../../d"] {
            let err = validate_object_name(s).unwrap_err();
            assert!(
                matches!(
                    err,
                    StorageError::InvalidObjectName {
                        reason: "relative path component",
                        ..
                    }
                ),
                "expected relative-path rejection for {s:?}"
            );
        }
    }

    #[test]
    fn rejects_single_dot_segments() {
        let err = validate_object_name("foo/./bar").unwrap_err();
        assert!(matches!(
            err,
            StorageError::InvalidObjectName {
                reason: "relative path component",
                ..
            }
        ));
    }

    #[test]
    fn rejects_double_slash() {
        let err = validate_object_name("foo//bar").unwrap_err();
        assert!(matches!(
            err,
            StorageError::InvalidObjectName {
                reason: "empty segment (double slash)",
                ..
            }
        ));
    }
}
