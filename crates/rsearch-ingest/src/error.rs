use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("malformed bulk body: {0}")]
    MalformedBulk(String),

    #[error("ingest queue is full")]
    Saturated,

    #[error("wal error: {0}")]
    Wal(#[from] std::io::Error),

    #[error("index error: {0}")]
    Index(#[from] rsearch_index::IndexError),

    #[error("metastore error: {0}")]
    Metastore(#[from] rsearch_metastore::MetastoreError),

    #[error("storage error: {0}")]
    Storage(#[from] rsearch_storage::StorageError),
}

pub type IngestResult<T> = Result<T, IngestError>;
