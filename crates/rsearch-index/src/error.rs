use thiserror::Error;

/// Errors from mapping parsing, split building, and split reading.
#[derive(Debug, Error)]
pub enum IndexError {
    /// The ES-style mapping JSON is malformed or uses an unsupported
    /// field type / reserved name.
    #[error("invalid mapping: {0}")]
    InvalidMapping(String),

    /// A document or split object could not be parsed or read.
    #[error("invalid document: {0}")]
    InvalidDocument(String),

    /// Underlying Tantivy index operation failed.
    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    /// Underlying filesystem I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias for results carrying [`IndexError`].
pub type IndexResult<T> = Result<T, IndexError>;
