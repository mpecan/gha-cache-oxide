//! Test-only fake [`StorageAdapter`] used by every per-task `tests`
//! submodule under `src/tasks/cleanup/`. Records every `delete_folder`
//! call so tests can assert the right keys were touched, and exposes a
//! `fail_next_delete` lever so the parts-cleanup test can drive the
//! storage-failure rollback path.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use crate::storage::{ByteStream, StorageAdapter, StorageError};

#[derive(Clone, Default)]
pub(super) struct FakeStorage {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    deleted: Vec<String>,
    next_delete_fails: bool,
}

impl FakeStorage {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn deleted_folders(&self) -> Vec<String> {
        self.inner.lock().unwrap().deleted.clone()
    }

    pub(super) fn fail_next_delete(&self) {
        self.inner.lock().unwrap().next_delete_fails = true;
    }
}

#[async_trait::async_trait]
impl StorageAdapter for FakeStorage {
    async fn upload_stream(
        &self,
        _object_name: &str,
        _body: ByteStream,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn download_stream(&self, object_name: &str) -> Result<ByteStream, StorageError> {
        Err(StorageError::ObjectNotFound(object_name.to_string()))
    }

    async fn delete_folder(&self, folder_name: &str) -> Result<(), StorageError> {
        let should_fail = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::take(&mut inner.next_delete_fails)
        };
        if should_fail {
            return Err(StorageError::Io(std::io::Error::other(
                "induced storage failure (FakeStorage::fail_next_delete)",
            )));
        }
        self.inner
            .lock()
            .unwrap()
            .deleted
            .push(folder_name.to_string());
        Ok(())
    }

    async fn count_files_in_folder(&self, _folder_name: &str) -> Result<u64, StorageError> {
        Ok(0)
    }

    async fn list_folder(
        &self,
        _folder_name: &str,
    ) -> Result<Vec<crate::storage::ObjectInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn copy(&self, from: &str, _to: &str) -> Result<(), StorageError> {
        Err(StorageError::ObjectNotFound(from.to_string()))
    }

    async fn signed_url(&self, _object_name: &str) -> Result<Option<url::Url>, StorageError> {
        Ok(None)
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.inner.lock().unwrap().deleted.clear();
        Ok(())
    }
}
