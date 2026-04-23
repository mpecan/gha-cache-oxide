//! Local filesystem storage adapter built on `object_store::local`.
//!
//! The configured root is used directly — no `gh-actions-cache/` key
//! prefix (that's an S3/GCS concern in upstream's `lib/storage.ts`).

use std::path::Path as StdPath;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use tokio::io::AsyncWriteExt;
use url::Url;

use super::{ByteStream, StorageAdapter, StorageError, validate_object_name};

/// Filesystem-backed implementation of [`StorageAdapter`].
///
/// Wraps an `object_store::local::LocalFileSystem` rooted at the path
/// supplied at construction time.
pub struct FilesystemAdapter {
    store: Arc<LocalFileSystem>,
}

impl FilesystemAdapter {
    /// Opens (or creates) a filesystem-backed storage adapter at `root`.
    ///
    /// `root` is created if missing. Subsequent object names are stored
    /// relative to it.
    ///
    /// # Errors
    /// Returns [`StorageError::Io`] on `create_dir_all` failure and
    /// [`StorageError::Backend`] if `object_store` cannot bind to the path.
    pub fn new(root: &StdPath) -> Result<Self, StorageError> {
        std::fs::create_dir_all(root)?;
        let store = LocalFileSystem::new_with_prefix(root)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Validates an object name and returns the corresponding
    /// `object_store` `Path`. Test-only helper — production code constructs
    /// the path inline inside each method.
    #[cfg(test)]
    fn object_path(object_name: &str) -> Result<ObjectPath, StorageError> {
        validate_object_name(object_name)?;
        Ok(ObjectPath::from(object_name))
    }
}

#[async_trait::async_trait]
impl StorageAdapter for FilesystemAdapter {
    async fn upload_stream(&self, object_name: &str, body: ByteStream) -> Result<(), StorageError> {
        validate_object_name(object_name)?;
        let path = ObjectPath::from(object_name);
        // BufWriter defaults to `capacity=10 MiB, max_concurrency=8`, which
        // holds up to 80 MiB of in-flight parts in memory. That's tuned for
        // cloud backends where concurrent part uploads win latency — on
        // LocalFileSystem the writer just renames a temp file, so serial
        // writes are equally fast and the concurrent buffers are pure
        // overhead. We cap at `max_concurrency=1` to keep peak memory
        // ≤ capacity, matching the bounded-memory streaming contract
        // asserted by `tests/streaming_memory.rs`.
        let mut writer = BufWriter::new(self.store.clone(), path).with_max_concurrency(1);
        let mut stream = body;
        while let Some(chunk) = stream.next().await {
            writer.write_all(&chunk?).await?;
        }
        writer.shutdown().await?;
        Ok(())
    }

    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        validate_object_name(object_name)?;
        let path = ObjectPath::from(object_name);
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
        let prefix = ObjectPath::from(folder_name);
        delete_under_prefix(self.store.as_ref(), &prefix).await
    }

    async fn count_files_in_folder(&self, folder_name: &str) -> Result<u64, StorageError> {
        validate_object_name(folder_name)?;
        let prefix = ObjectPath::from(folder_name);
        let mut list = self.store.list(Some(&prefix));
        let mut count: u64 = 0;
        while let Some(item) = list.next().await {
            match item {
                Ok(_) => count += 1,
                // Missing folder surfaces as NotFound on local backends;
                // treat that as "empty", matching upstream behaviour.
                Err(object_store::Error::NotFound { .. }) => return Ok(0),
                Err(e) => return Err(StorageError::Backend(e)),
            }
        }
        Ok(count)
    }

    async fn signed_url(&self, object_name: &str) -> Result<Option<Url>, StorageError> {
        // Validated for symmetry with the other methods — even though the
        // name is not used, a caller passing a traversal string should see
        // the same `InvalidObjectName` it would get from `upload_stream`.
        validate_object_name(object_name)?;
        // `LocalFileSystem` cannot issue URLs that anything outside this
        // process can dereference. Upstream's `FileSystemAdapter` doesn't
        // implement `createDownloadUrl` either; returning `None` lets
        // `GetCacheEntryDownloadURL` (#8) fall back to a server-proxied URL.
        Ok(None)
    }

    async fn clear(&self) -> Result<(), StorageError> {
        // No prefix → everything under the adapter root.
        let root = ObjectPath::default();
        delete_under_prefix(self.store.as_ref(), &root).await
    }
}

/// Lists everything under `prefix` and deletes each object. Collects into
/// a `Vec` first because the list stream borrows the store — holding it
/// open while issuing parallel `delete()` calls would race the same borrow.
async fn delete_under_prefix(
    store: &LocalFileSystem,
    prefix: &ObjectPath,
) -> Result<(), StorageError> {
    let mut list = store.list(Some(prefix));
    let mut paths: Vec<ObjectPath> = Vec::new();
    while let Some(item) = list.next().await {
        match item {
            Ok(meta) => paths.push(meta.location),
            // Empty / missing folder is a no-op.
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(StorageError::Backend(e)),
        }
    }
    for path in paths {
        store.delete(&path).await?;
    }
    Ok(())
}

/// Maps `object_store`'s `NotFound` error to our `ObjectNotFound` variant,
/// preserving the original-name context for diagnostics.
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

    use bytes::Bytes;
    use futures::stream;
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn bytes_stream(data: Vec<u8>) -> ByteStream {
        stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(data))]).boxed()
    }

    async fn collect(mut s: ByteStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = s.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    fn temp_adapter() -> (FilesystemAdapter, TempDir) {
        let tmp = TempDir::new().unwrap();
        let adapter = FilesystemAdapter::new(tmp.path()).unwrap();
        (adapter, tmp)
    }

    #[tokio::test]
    async fn round_trip_zero_bytes() {
        let (adapter, _tmp) = temp_adapter();
        adapter
            .upload_stream("empty.bin", bytes_stream(vec![]))
            .await
            .unwrap();
        let got = collect(adapter.download_stream("empty.bin").await.unwrap()).await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn round_trip_small_payload() {
        let (adapter, _tmp) = temp_adapter();
        let payload = b"hello, world".to_vec();
        adapter
            .upload_stream("folder/file.bin", bytes_stream(payload.clone()))
            .await
            .unwrap();
        let got = collect(adapter.download_stream("folder/file.bin").await.unwrap()).await;
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn download_missing_returns_object_not_found() {
        let (adapter, _tmp) = temp_adapter();
        let result = adapter.download_stream("does/not/exist").await;
        match result {
            Err(StorageError::ObjectNotFound(ref s)) if s == "does/not/exist" => {}
            Err(other) => panic!("expected ObjectNotFound, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[tokio::test]
    async fn upload_rejects_directory_traversal() {
        let (adapter, _tmp) = temp_adapter();
        let err = adapter
            .upload_stream("../etc/passwd", bytes_stream(b"bad".to_vec()))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::InvalidObjectName { .. }),
            "expected InvalidObjectName, got {err:?}"
        );
    }

    #[tokio::test]
    async fn download_rejects_traversal() {
        let (adapter, _tmp) = temp_adapter();
        match adapter.download_stream("/etc/passwd").await {
            Err(StorageError::InvalidObjectName { .. }) => {}
            Err(other) => panic!("expected InvalidObjectName, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[tokio::test]
    async fn count_files_in_missing_folder_returns_zero() {
        let (adapter, _tmp) = temp_adapter();
        assert_eq!(
            adapter.count_files_in_folder("no-such-dir").await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn count_files_counts_uploaded_files() {
        let (adapter, _tmp) = temp_adapter();
        for i in 0u8..3 {
            adapter
                .upload_stream(&format!("parts/{i}"), bytes_stream(vec![i]))
                .await
                .unwrap();
        }
        // Unrelated folder shouldn't be counted.
        adapter
            .upload_stream("other/z", bytes_stream(vec![0]))
            .await
            .unwrap();

        assert_eq!(adapter.count_files_in_folder("parts").await.unwrap(), 3);
        assert_eq!(adapter.count_files_in_folder("other").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn delete_folder_removes_all_children() {
        let (adapter, _tmp) = temp_adapter();
        for i in 0u8..3 {
            adapter
                .upload_stream(&format!("target/{i}"), bytes_stream(vec![i]))
                .await
                .unwrap();
        }
        adapter
            .upload_stream("sibling/x", bytes_stream(vec![0]))
            .await
            .unwrap();

        adapter.delete_folder("target").await.unwrap();

        assert_eq!(adapter.count_files_in_folder("target").await.unwrap(), 0);
        // Sibling unaffected.
        assert_eq!(adapter.count_files_in_folder("sibling").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn delete_missing_folder_is_noop() {
        let (adapter, _tmp) = temp_adapter();
        adapter.delete_folder("never-created").await.unwrap();
    }

    #[tokio::test]
    async fn clear_removes_everything() {
        let (adapter, _tmp) = temp_adapter();
        adapter
            .upload_stream("a/1", bytes_stream(vec![1]))
            .await
            .unwrap();
        adapter
            .upload_stream("b/2", bytes_stream(vec![2]))
            .await
            .unwrap();

        adapter.clear().await.unwrap();

        assert_eq!(adapter.count_files_in_folder("a").await.unwrap(), 0);
        assert_eq!(adapter.count_files_in_folder("b").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn signed_url_always_none_on_filesystem() {
        let (adapter, _tmp) = temp_adapter();
        assert!(adapter.signed_url("anything").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn signed_url_validates_object_name() {
        let (adapter, _tmp) = temp_adapter();
        match adapter.signed_url("../evil").await {
            Err(StorageError::InvalidObjectName { .. }) => {}
            Err(other) => panic!("expected InvalidObjectName, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok(None)"),
        }
    }

    #[tokio::test]
    async fn prefix_matching_is_segment_aware() {
        // Object_store's Path::prefix_match should treat "parts" and
        // "parts-foo" as disjoint top-level segments. Pin this contract
        // so a future backend switch can't silently leak sibling folders.
        let (adapter, _tmp) = temp_adapter();
        adapter
            .upload_stream("parts/a", bytes_stream(vec![1]))
            .await
            .unwrap();
        adapter
            .upload_stream("parts/b", bytes_stream(vec![2]))
            .await
            .unwrap();
        adapter
            .upload_stream("parts-foo/x", bytes_stream(vec![3]))
            .await
            .unwrap();

        assert_eq!(
            adapter.count_files_in_folder("parts").await.unwrap(),
            2,
            "count should match only the 'parts' folder, not 'parts-foo'"
        );

        // And delete_folder must not touch the sibling.
        adapter.delete_folder("parts").await.unwrap();
        assert_eq!(adapter.count_files_in_folder("parts").await.unwrap(), 0);
        assert_eq!(
            adapter.count_files_in_folder("parts-foo").await.unwrap(),
            1,
            "delete_folder(\"parts\") must not affect 'parts-foo'"
        );
    }

    #[tokio::test]
    async fn upload_overwrites_existing_object() {
        let (adapter, _tmp) = temp_adapter();
        adapter
            .upload_stream("obj", bytes_stream(b"first".to_vec()))
            .await
            .unwrap();
        adapter
            .upload_stream("obj", bytes_stream(b"second".to_vec()))
            .await
            .unwrap();
        let got = collect(adapter.download_stream("obj").await.unwrap()).await;
        assert_eq!(&got, b"second");
    }

    #[tokio::test]
    async fn delete_folder_is_recursive_across_nested_directories() {
        let (adapter, _tmp) = temp_adapter();
        adapter
            .upload_stream("root/a", bytes_stream(vec![1]))
            .await
            .unwrap();
        adapter
            .upload_stream("root/nested/b", bytes_stream(vec![2]))
            .await
            .unwrap();
        adapter
            .upload_stream("root/nested/deep/c", bytes_stream(vec![3]))
            .await
            .unwrap();

        adapter.delete_folder("root").await.unwrap();

        assert_eq!(adapter.count_files_in_folder("root").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn round_trip_large_payload_crosses_buffer_flush() {
        // Exercises `BufWriter` across its flush boundary. 12 MiB beats
        // the default 10 MiB threshold with margin; keeps test runtime
        // in single-digit seconds.
        let (adapter, _tmp) = temp_adapter();
        // Non-uniform payload so we'd catch accidental chunk reordering.
        let payload: Vec<u8> = (0..12 * 1024 * 1024)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        adapter
            .upload_stream("big.bin", bytes_stream(payload.clone()))
            .await
            .unwrap();
        let got = collect(adapter.download_stream("big.bin").await.unwrap()).await;
        assert_eq!(got.len(), payload.len());
        assert_eq!(got, payload);
    }

    #[test]
    fn object_path_round_trips_for_normal_names() {
        assert!(FilesystemAdapter::object_path("folder/file").is_ok());
        assert!(FilesystemAdapter::object_path("/absolute").is_err());
        assert!(FilesystemAdapter::object_path("../traversal").is_err());
    }

    // ------------------------------------------------------------------
    // Property test — round-trip byte equality under arbitrary payloads.
    // Proptest's default strategy would generate enormous payloads; cap at
    // 1 MiB so the test runs in reasonable wall time but still exercises
    // multi-chunk streaming through `BufWriter`.
    // ------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 16,
            .. ProptestConfig::default()
        })]

        #[test]
        fn round_trip_arbitrary_bytes(payload in proptest::collection::vec(any::<u8>(), 0..1_048_576)) {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let (adapter, _tmp) = temp_adapter();
                adapter
                    .upload_stream("prop/obj", bytes_stream(payload.clone()))
                    .await
                    .unwrap();
                let got = collect(adapter.download_stream("prop/obj").await.unwrap()).await;
                prop_assert_eq!(got, payload);
                Ok(())
            })?;
        }
    }
}
