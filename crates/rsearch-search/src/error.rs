use thiserror::Error;

/// Errors returned by the search path.
#[derive(Debug, Error)]
pub enum SearchError {
    /// Client error — maps to HTTP 400 with the reason.
    #[error("{0}")]
    BadRequest(String),

    /// Split open/read failure.
    #[error("index error: {0}")]
    Index(#[from] rsearch_index::IndexError),

    /// Metastore operation failure (split pruning, stream lookup).
    #[error("metastore error: {0}")]
    Metastore(#[from] rsearch_metastore::MetastoreError),

    /// Query execution failure inside Tantivy.
    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    /// Internal failure (e.g. a search task panicked or was dropped).
    #[error("search task failed: {0}")]
    Internal(String),
}

/// Convenience alias for fallible search operations.
pub type SearchResult<T> = Result<T, SearchError>;
