//! S3-compatible storage adapter on top of `object_store::aws::AmazonS3`.
//!
//! # Upstream parity
//!
//! Upstream reference: `lib/storage.ts#S3Adapter`. The observable
//! on-the-wire contract matches:
//!
//! - key prefix `gh-actions-cache/` — a bucket populated by the upstream
//!   TypeScript server remains readable by this adapter (and vice-versa);
//! - 10-minute signed download-URL TTL;
//! - bucket probe at startup so a misconfigured bucket fails loudly
//!   before the first cache request;
//! - path-style addressing (`forcePathStyle` in upstream,
//!   `with_virtual_hosted_style_request(false)` here) so `MinIO` /
//!   Garage / Ceph work without per-host DNS juggling.
//!
//! # Deliberate deviations
//!
//! - **Multipart upload sizing.** Upstream caps per-part at 5 MiB with
//!   `queueSize: 1` (strictly serial). We use `BufWriter`'s 10 MiB
//!   default chunk with `with_max_concurrency(4)` so we saturate the
//!   link on fast paths while holding peak memory to ~40 MiB. The wire
//!   format is still S3 multipart, so objects remain drop-in readable
//!   by the upstream server.
//! - **Bucket probe.** Upstream issues `HeadBucketCommand`; we do
//!   `store.list(None).next().await` — one round-trip, carries the same
//!   missing / auth-denied signal with less AWS-specific API surface.
//!
//! Conditional-commit (If-Match / `PutMode`) support lands in issue #34;
//! no placeholder is reserved here — grep for `PutMode` when that work
//! starts.

use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ObjectStore, ObjectStoreExt};
use reqwest::Method;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::config::Secret;

use super::{ByteStream, StorageAdapter, StorageError, validate_object_name};

/// All object names are stored under this prefix so upstream-populated
/// buckets remain drop-in compatible.
const KEY_PREFIX: &str = "gh-actions-cache";

/// Signed-download-URL TTL. Upstream: `10 * 60 * 1000` ms in
/// `lib/storage.ts#createDownloadUrl`.
const SIGNED_URL_TTL: Duration = Duration::from_secs(10 * 60);

/// Inputs for constructing [`S3Adapter`]. Typically built from
/// `crate::config::StorageConfig::S3`, but tests can populate it directly.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<Url>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<Secret>,
    /// Override for the top-level key prefix. `None` uses the
    /// upstream-compatible default `"gh-actions-cache"`. Tests set this
    /// to a per-run UUID so parallel scenarios sharing a single `MinIO`
    /// bucket can't clobber each other's state.
    pub key_prefix: Option<String>,
}

/// S3-backed implementation of [`StorageAdapter`].
#[derive(Debug)]
pub struct S3Adapter {
    store: Arc<AmazonS3>,
    /// Every object name is stored under `{key_prefix}/{name}`.
    key_prefix: String,
}

impl S3Adapter {
    /// Builds an adapter and probes the bucket so a misconfigured
    /// endpoint / bucket / credential surfaces at startup, not on the
    /// first cache request.
    ///
    /// # Errors
    /// [`StorageError::Backend`] if the builder rejects the config
    /// (e.g. conflicting endpoint + virtual-hosted-style).
    /// [`StorageError::BucketUnavailable`] if the bucket does not exist
    /// or the credentials cannot reach it.
    pub async fn new(cfg: S3Config) -> Result<Self, StorageError> {
        let key_prefix = cfg
            .key_prefix
            .clone()
            .unwrap_or_else(|| KEY_PREFIX.to_string());
        let store = build_store(&cfg)?;
        probe_bucket(&store, &cfg.bucket).await?;
        Ok(Self {
            store: Arc::new(store),
            key_prefix,
        })
    }

    fn prefixed(&self, object_name: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{object_name}", self.key_prefix))
    }

    /// `ObjectStore::list` appends the path delimiter internally
    /// (see `object_store::client::list::list_paginated`), so
    /// `list(Some(Path::from("…/parts")))` already narrows to
    /// `parts/`-prefixed keys. The method stays for symmetry with
    /// `prefixed` and to keep `delete_folder` / `count_files_in_folder`
    /// reading the same way.
    fn prefixed_folder(&self, folder_name: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{folder_name}", self.key_prefix))
    }

    fn root_prefix(&self) -> ObjectPath {
        ObjectPath::from(self.key_prefix.clone())
    }
}

fn build_store(cfg: &S3Config) -> Result<AmazonS3, StorageError> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region)
        // Path-style addressing matches upstream's `forcePathStyle: true`
        // and is the only mode MinIO / Garage reliably support.
        .with_virtual_hosted_style_request(false);

    if let Some(endpoint) = &cfg.endpoint_url {
        builder = builder.with_endpoint(endpoint.as_str().trim_end_matches('/'));
        // Custom endpoints (MinIO) often run over plain HTTP in dev/CI.
        // Only enabled for http:// — https:// retains its default strict mode.
        if endpoint.scheme() == "http" {
            builder = builder.with_allow_http(true);
        }
    }

    if let (Some(key), Some(secret)) = (&cfg.access_key_id, &cfg.secret_access_key) {
        builder = builder
            .with_access_key_id(key)
            .with_secret_access_key(secret.expose());
    }
    // If one of the two is set, the builder errors at `.build()`; if
    // neither is set, the builder falls back to the default credential
    // provider chain (WebIdentity → Task → EKS → Instance metadata).

    builder.build().map_err(StorageError::Backend)
}

/// Probes the bucket by issuing a bounded list. Empty bucket → empty
/// stream (OK). Missing bucket / bad credentials → a concrete error we
/// surface as [`StorageError::BucketUnavailable`] so operators see the
/// bucket name, not a generic S3 500.
async fn probe_bucket(store: &AmazonS3, bucket: &str) -> Result<(), StorageError> {
    let mut stream = store.list(None);
    match stream.next().await {
        None | Some(Ok(_)) => Ok(()),
        Some(Err(e)) => Err(StorageError::BucketUnavailable {
            bucket: bucket.to_string(),
            source: Box::new(e),
        }),
    }
}

#[async_trait::async_trait]
impl StorageAdapter for S3Adapter {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        validate_object_name(object_name)?;
        let path = self.prefixed(object_name);
        // S3 benefits from concurrent part uploads: 4 × 10 MiB keeps peak
        // memory bounded (~40 MiB) while saturating typical links. The
        // filesystem driver pins 1 for the opposite reason (local writes
        // are already serial-fast); this is the diverging knob.
        let mut writer = BufWriter::new(self.store.clone(), path).with_max_concurrency(4);
        let mut stream = body;
        while let Some(chunk) = stream.next().await {
            writer.write_all(&chunk?).await?;
        }
        writer.shutdown().await?;
        Ok(())
    }

    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        validate_object_name(object_name)?;
        let path = self.prefixed(object_name);
        let got = self
            .store
            .get(&path)
            .await
            .map_err(|e| translate_not_found(e, object_name))?;
        let stream = got.into_stream().map_err(std::io::Error::from).boxed();
        Ok(stream)
    }

    async fn delete_folder(&self, folder_name: &str) -> Result<(), StorageError> {
        validate_object_name(folder_name)?;
        let prefix = self.prefixed_folder(folder_name);
        delete_under_prefix(self.store.as_ref(), &prefix).await
    }

    async fn count_files_in_folder(&self, folder_name: &str) -> Result<u64, StorageError> {
        validate_object_name(folder_name)?;
        let prefix = self.prefixed_folder(folder_name);
        let mut list = self.store.list(Some(&prefix));
        let mut count: u64 = 0;
        while let Some(item) = list.next().await {
            match item {
                Ok(_) => count += 1,
                Err(object_store::Error::NotFound { .. }) => return Ok(0),
                Err(e) => return Err(StorageError::Backend(e)),
            }
        }
        Ok(count)
    }

    async fn signed_url(&self, object_name: &str) -> Result<Option<Url>, StorageError> {
        validate_object_name(object_name)?;
        let path = self.prefixed(object_name);
        let url = self
            .store
            .signed_url(Method::GET, &path, SIGNED_URL_TTL)
            .await
            .map_err(StorageError::Backend)?;
        Ok(Some(url))
    }

    async fn clear(&self) -> Result<(), StorageError> {
        let prefix = self.root_prefix();
        delete_under_prefix(self.store.as_ref(), &prefix).await
    }
}

/// Collects every path under `prefix` and deletes them one by one.
/// Sequential matches the filesystem driver — simpler than batched
/// `DeleteObjects` and correct for the test / admin scales this is used
/// at. A future optimisation could hand the path stream to
/// `object_store::ObjectStore::delete_stream` for concurrent deletes.
async fn delete_under_prefix(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<(), StorageError> {
    let mut list = store.list(Some(prefix));
    let mut paths: Vec<ObjectPath> = Vec::new();
    while let Some(item) = list.next().await {
        match item {
            Ok(meta) => paths.push(meta.location),
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(StorageError::Backend(e)),
        }
    }
    for path in paths {
        store.delete(&path).await?;
    }
    Ok(())
}

fn translate_not_found(err: object_store::Error, name: &str) -> StorageError {
    if matches!(err, object_store::Error::NotFound { .. }) {
        StorageError::ObjectNotFound(name.to_string())
    } else {
        StorageError::Backend(err)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// Build-only — wraps `build_store` in a fake adapter so we can
    /// exercise the prefixing helpers without a live S3 endpoint.
    fn adapter_with_prefix(prefix: Option<&str>) -> S3Adapter {
        let cfg = S3Config {
            bucket: "bkt".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            key_prefix: prefix.map(str::to_string),
        };
        let store = build_store(&cfg).unwrap();
        let key_prefix = cfg.key_prefix.unwrap_or_else(|| KEY_PREFIX.to_string());
        S3Adapter {
            store: Arc::new(store),
            key_prefix,
        }
    }

    #[test]
    fn prefixed_prepends_gh_actions_cache_by_default() {
        let a = adapter_with_prefix(None);
        assert_eq!(
            a.prefixed("folder/file.bin").as_ref(),
            "gh-actions-cache/folder/file.bin"
        );
    }

    #[test]
    fn prefixed_folder_produces_segment_bounded_prefix() {
        // `object_store::client::list::list_paginated` appends the
        // delimiter internally, so we pin the raw `Path` form here and
        // trust the conformance suite's `prefix_matching_is_segment_aware`
        // scenario to prove segment-awareness end-to-end.
        let a = adapter_with_prefix(None);
        assert_eq!(
            a.prefixed_folder("parts").as_ref(),
            "gh-actions-cache/parts"
        );
    }

    #[test]
    fn custom_key_prefix_overrides_default() {
        let a = adapter_with_prefix(Some("test-run-abc"));
        assert_eq!(a.prefixed("obj").as_ref(), "test-run-abc/obj");
        assert_eq!(a.prefixed_folder("dir").as_ref(), "test-run-abc/dir");
        assert_eq!(a.root_prefix().as_ref(), "test-run-abc");
    }

    #[test]
    fn build_store_accepts_no_explicit_credentials() {
        // Empty creds → AWS default provider chain; builder must not
        // reject the combination.
        let cfg = S3Config {
            bucket: "bkt".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            key_prefix: None,
        };
        assert!(build_store(&cfg).is_ok());
    }

    #[test]
    fn build_store_accepts_http_endpoint() {
        // `with_allow_http(true)` is the knob MinIO needs — test that
        // it doesn't trip the builder.
        let cfg = S3Config {
            bucket: "bkt".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some(Url::parse("http://localhost:9000").unwrap()),
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some(Secret::new("minioadmin".to_string())),
            key_prefix: None,
        };
        assert!(build_store(&cfg).is_ok());
    }
}
