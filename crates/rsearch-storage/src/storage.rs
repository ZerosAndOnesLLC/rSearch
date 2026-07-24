use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::StorageResult;

/// Backend-agnostic object store. Keys are slash-separated relative paths
/// (e.g. `streams/app-logs/2026-07-24/split-abc.split`).
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Store an object, replacing any existing object at the key.
    /// Writes are atomic: readers never observe a partial object.
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()>;

    /// Upload a local file as an object (streaming; used for split files).
    async fn put_file(&self, key: &str, local: &Path) -> StorageResult<()>;

    /// Fetch an entire object.
    async fn get(&self, key: &str) -> StorageResult<Bytes>;

    /// Fetch a byte range of an object (end exclusive).
    async fn get_range(&self, key: &str, range: Range<u64>) -> StorageResult<Bytes>;

    /// Object size in bytes.
    async fn size(&self, key: &str) -> StorageResult<u64>;

    /// Delete an object. Deleting a missing object is not an error.
    async fn delete(&self, key: &str) -> StorageResult<()>;

    /// List all object keys under a prefix.
    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>>;

    /// Whether an object exists.
    async fn exists(&self, key: &str) -> StorageResult<bool> {
        match self.size(key).await {
            Ok(_) => Ok(true),
            Err(crate::StorageError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}
