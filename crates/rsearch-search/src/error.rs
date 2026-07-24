use thiserror::Error;

#[derive(Debug, Error)]
pub enum SearchError {
    /// Client error — maps to HTTP 400 with the reason.
    #[error("{0}")]
    BadRequest(String),

    #[error("index error: {0}")]
    Index(#[from] rsearch_index::IndexError),

    #[error("metastore error: {0}")]
    Metastore(#[from] rsearch_metastore::MetastoreError),

    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("search task failed: {0}")]
    Internal(String),
}

pub type SearchResult<T> = Result<T, SearchError>;
