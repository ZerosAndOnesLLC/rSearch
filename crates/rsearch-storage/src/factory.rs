use std::sync::Arc;

use rsearch_common::config::StorageConfig;

use crate::error::{StorageError, StorageResult};
use crate::fs::FsStorage;
use crate::s3::S3Storage;
use crate::storage::Storage;

/// Build the storage backend selected by configuration.
pub async fn from_config(cfg: &StorageConfig) -> StorageResult<Arc<dyn Storage>> {
    match cfg.backend.as_str() {
        "fs" => Ok(Arc::new(FsStorage::new(cfg.root.clone()))),
        "s3" => Ok(Arc::new(S3Storage::from_config(cfg).await?)),
        other => Err(StorageError::Backend {
            key: String::new(),
            message: format!("unknown storage backend '{other}' (expected fs or s3)"),
        }),
    }
}
