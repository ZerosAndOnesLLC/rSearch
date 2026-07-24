use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("invalid mapping: {0}")]
    InvalidMapping(String),

    #[error("invalid document: {0}")]
    InvalidDocument(String),

    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type IndexResult<T> = Result<T, IndexError>;
