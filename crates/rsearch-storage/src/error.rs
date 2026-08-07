use thiserror::Error;

/// Errors returned by the object storage backends.
#[derive(Debug, Error)]
pub enum StorageError {
    /// The requested object key does not exist.
    #[error("object not found: {0}")]
    NotFound(String),

    /// Key rejected before any I/O (traversal, absolute path, or
    /// otherwise malformed).
    #[error("invalid object key: {0}")]
    InvalidKey(String),

    /// Filesystem or network I/O failed while operating on a key.
    #[error("io error on {key}: {source}")]
    Io {
        /// Object key the operation was acting on.
        key: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Backend-specific failure (S3 API error, peer refusal, quorum
    /// shortfall, misconfiguration).
    #[error("backend error on {key}: {message}")]
    Backend {
        /// Object key the operation was acting on (may be empty for
        /// configuration errors).
        key: String,
        /// Backend-provided failure description.
        message: String,
    },
}

/// Convenience alias for results carrying [`StorageError`].
pub type StorageResult<T> = Result<T, StorageError>;
