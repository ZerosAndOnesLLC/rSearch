use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("invalid object key: {0}")]
    InvalidKey(String),

    #[error("io error on {key}: {source}")]
    Io {
        key: String,
        #[source]
        source: std::io::Error,
    },

    #[error("backend error on {key}: {message}")]
    Backend { key: String, message: String },
}

pub type StorageResult<T> = Result<T, StorageError>;
