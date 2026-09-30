//! Google Cloud Storage adapter on top of `object_store::gcp::GoogleCloudStorage`.
//!
//! # Upstream parity
//!
//! Upstream reference: `lib/storage.ts#GcsAdapter`. The observable
//! on-the-wire contract matches:
//!
//! - key prefix `gh-actions-cache/` — a bucket populated by the upstream
//!   TypeScript server remains readable by this adapter (and vice-versa);
//! - 10-minute signed download-URL TTL;
//! - bucket probe at startup so a misconfigured bucket fails loudly
//!   before the first cache request;
//! - service-account-JSON authentication via `STORAGE_GCS_SERVICE_ACCOUNT_KEY`
//!   (omit for ambient ADC: GCE metadata, workload identity, gcloud
//!   default credentials).
//!
//! # Deliberate deviations
//!
//! - **Multipart upload sizing.** Upstream uses single-shot
//!   `createWriteStream({resumable: false, validation: false})` — no
//!   concurrency. We use `BufWriter`'s 10 MiB default chunk with
//!   `with_max_concurrency(4)` so we saturate the link on fast paths
//!   while holding peak memory to ~40 MiB. Same rationale as the S3
//!   driver (`src/storage/s3.rs:18-26`); the wire format is still GCS
//!   resumable upload, so objects remain drop-in readable by the
//!   upstream server.
//! - **Bucket probe.** Upstream issues `bucket.getMetadata()`; we do
//!   `store.list(None).next().await` — one round-trip, carries the same
//!   missing / auth-denied signal with less GCS-specific API surface.
//!   Mirrors what the S3 driver does (`src/storage/s3.rs:25-27`).
//! - **Segment-bounded `clear()` and `delete_folder`.** Upstream's
//!   `bucket.deleteFiles({ prefix: this.keyPrefix })` is a byte-prefix
//!   match, so a sibling key like `gh-actions-cache-other/...` would be
//!   swept up alongside `gh-actions-cache/...`. Our `root_prefix()`
//!   returns an `object_store::path::Path`, and `list(Some(&path))` is
//!   segment-aware — we won't touch sibling keys. Stricter behaviour by
//!   accident; documenting it so the divergence is traceable.

use std::path::Path as StdPath;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::client::ClientOptions;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ObjectStore, ObjectStoreExt};
use reqwest::Method;
use tokio::io::AsyncWriteExt;
use url::Url;

use super::{
    ByteStream, ObjectInfo, StorageAdapter, StorageError, list_under_prefix, validate_object_name,
};

/// All object names are stored under this prefix so upstream-populated
/// buckets remain drop-in compatible.
const KEY_PREFIX: &str = "gh-actions-cache";

/// Signed-download-URL TTL. Upstream: `10 * 60 * 1000` ms in
/// `lib/storage.ts#GcsAdapter::createDownloadUrl`.
const SIGNED_URL_TTL: Duration = Duration::from_mins(10);

/// Inputs for constructing [`GcsAdapter`]. Typically built from
/// `crate::config::StorageConfig::Gcs`, but tests can populate it directly.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    pub bucket: String,
    /// Path to a service-account JSON file. `None` falls back to the
    /// Application Default Credentials chain (GCE metadata, workload
    /// identity, gcloud default credentials).
    pub service_account_key: Option<std::path::PathBuf>,
    /// Override the GCS endpoint — required for `fake-gcs-server` and
    /// for any private-VPC / regional GCS proxy. Production leaves this
    /// at `None`.
    pub endpoint: Option<Url>,
    /// Override for the top-level key prefix. `None` uses the
    /// upstream-compatible default `"gh-actions-cache"`. Tests set this
    /// to a per-run UUID so parallel scenarios sharing a single bucket
    /// can't clobber each other's state.
    pub key_prefix: Option<String>,
}

/// GCS-backed implementation of [`StorageAdapter`].
#[derive(Debug)]
pub struct GcsAdapter {
    store: Arc<GoogleCloudStorage>,
    /// Every object name is stored under `{key_prefix}/{name}`.
    key_prefix: String,
}

impl GcsAdapter {
    /// Builds an adapter and probes the bucket so a misconfigured
    /// endpoint / bucket / credential surfaces at startup, not on the
    /// first cache request.
    ///
    /// # Errors
    /// [`StorageError::Backend`] if the builder rejects the config
    /// (e.g. unparseable service-account JSON).
    /// [`StorageError::BucketUnavailable`] if the bucket does not exist
    /// or the credentials cannot reach it.
    pub async fn new(cfg: GcsConfig) -> Result<Self, StorageError> {
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

fn build_store(cfg: &GcsConfig) -> Result<GoogleCloudStorage, StorageError> {
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&cfg.bucket);

    if let Some(path) = &cfg.service_account_key {
        builder = builder.with_service_account_path(path_to_string(path));
    }

    if let Some(endpoint) = &cfg.endpoint {
        // `with_base_url` is the override knob upstream's TypeScript
        // adapter exposes as `apiEndpoint`. Trim any trailing slash so
        // the builder doesn't double-up separators when it appends the
        // bucket / object path.
        builder = builder.with_base_url(endpoint.as_str().trim_end_matches('/'));
        // Custom endpoints (fake-gcs-server) often run over plain HTTP
        // in dev/CI. Only enabled for http:// — https:// retains its
        // default strict mode.
        if endpoint.scheme() == "http" {
            builder = builder.with_client_options(ClientOptions::new().with_allow_http(true));
        }
    }

    builder.build().map_err(StorageError::Backend)
}

/// Probes the bucket by issuing a bounded list. Empty bucket → empty
/// stream (OK). Missing bucket / bad credentials → a concrete error we
/// surface as [`StorageError::BucketUnavailable`] so operators see the
/// bucket name, not a generic GCS 500.
async fn probe_bucket(store: &GoogleCloudStorage, bucket: &str) -> Result<(), StorageError> {
    let mut stream = store.list(None);
    match stream.next().await {
        None | Some(Ok(_)) => Ok(()),
        Some(Err(e)) => Err(StorageError::BucketUnavailable {
            bucket: bucket.to_string(),
            source: Box::new(e),
        }),
    }
}

/// Converts a `Path` to a `String` for the builder. The builder takes an
/// `impl Into<String>`, so a lossy conversion is acceptable here — the
/// path is only ever read by `serde_json` against the file at that
/// location, and any encoding loss surfaces as a parse error there.
fn path_to_string(path: &StdPath) -> String {
    path.to_string_lossy().into_owned()
}

#[async_trait::async_trait]
impl StorageAdapter for GcsAdapter {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        validate_object_name(object_name)?;
        let path = self.prefixed(object_name);
        // Same concurrency rationale as the S3 driver: 4 × 10 MiB keeps
        // peak memory bounded (~40 MiB) while saturating typical cloud
        // links. Upstream's GCS adapter uses single-shot streaming;
        // this is a documented deviation in the file's module docs.
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

    async fn list_folder(&self, folder_name: &str) -> Result<Vec<ObjectInfo>, StorageError> {
        validate_object_name(folder_name)?;
        let prefix = self.prefixed_folder(folder_name);
        list_under_prefix(self.store.as_ref(), &prefix).await
    }

    async fn copy(&self, from: &str, to: &str) -> Result<(), StorageError> {
        validate_object_name(from)?;
        validate_object_name(to)?;
        let (src, dst) = (self.prefixed(from), self.prefixed(to));
        self.store
            .copy(&src, &dst)
            .await
            .map_err(|e| translate_not_found(e, from))
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
/// Sequential matches the S3 + filesystem drivers — simpler than
/// batched deletes and correct for the test / admin scales this is
/// used at. A future optimisation could hand the path stream to
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
    /// exercise the prefixing helpers without a live GCS endpoint.
    /// Empty service-account JSON is sufficient for `build()`; only the
    /// probe step touches the network.
    fn adapter_with_prefix(prefix: Option<&str>) -> GcsAdapter {
        let cfg = GcsConfig {
            bucket: "bkt".to_string(),
            service_account_key: None,
            endpoint: Some(Url::parse("http://localhost:4443").unwrap()),
            key_prefix: prefix.map(str::to_string),
        };
        let store = build_store(&cfg).unwrap();
        let key_prefix = cfg.key_prefix.unwrap_or_else(|| KEY_PREFIX.to_string());
        GcsAdapter {
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
        // Same invariant as the S3 driver — the segment-aware behaviour
        // is proven end-to-end by the conformance suite's
        // `prefix_matching_is_segment_aware` scenario.
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
    fn build_store_accepts_http_endpoint() {
        // `with_allow_http(true)` is the knob fake-gcs-server needs —
        // test that it doesn't trip the builder.
        let cfg = GcsConfig {
            bucket: "bkt".to_string(),
            service_account_key: None,
            endpoint: Some(Url::parse("http://localhost:4443").unwrap()),
            key_prefix: None,
        };
        assert!(build_store(&cfg).is_ok());
    }
}
