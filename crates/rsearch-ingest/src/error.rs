use thiserror::Error;

/// Errors returned by the ingest path.
#[derive(Debug, Error)]
pub enum IngestError {
    /// Bulk body too malformed to produce per-item responses.
    #[error("malformed bulk body: {0}")]
    MalformedBulk(String),

    /// The stream's bounded queue is full — callers report 429.
    #[error("ingest queue is full")]
    Saturated,

    /// WAL I/O failure (append, fsync, or replay).
    #[error("wal error: {0}")]
    Wal(#[from] std::io::Error),

    /// Split build/packaging failure.
    #[error("index error: {0}")]
    Index(#[from] rsearch_index::IndexError),

    /// Metastore operation failure.
    #[error("metastore error: {0}")]
    Metastore(#[from] rsearch_metastore::MetastoreError),

    /// Object storage failure (split upload).
    #[error("storage error: {0}")]
    Storage(#[from] rsearch_storage::StorageError),
}

/// Convenience alias for fallible ingest operations.
pub type IngestResult<T> = Result<T, IngestError>;
