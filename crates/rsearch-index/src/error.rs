use thiserror::Error;

/// Errors from mapping parsing, split building, and split reading.
#[derive(Debug, Error)]
pub enum IndexError {
    /// The ES-style mapping JSON is malformed or uses an unsupported
    /// field type / reserved name.
    #[error("invalid mapping: {0}")]
    InvalidMapping(String),

    /// A mapping was refused the way OpenSearch refuses it; the message
    /// is the client-facing `reason` verbatim (`mapper_parsing_exception`).
    #[error("{0}")]
    MapperParsing(String),

    /// A mapping request was refused with an `illegal_argument_exception`
    /// (an unusable analysis setting); the message is the `reason`.
    #[error("{0}")]
    IllegalArgument(String),

    /// A document could not be indexed because a mapped field could not
    /// parse its value and `ignore_malformed` is off (issue #86).
    #[error("failed to parse field [{field}] of type [{ty}]")]
    MalformedField {
        /// Full dotted path of the field that failed.
        field: String,
        /// The field's declared type.
        ty: &'static str,
        /// Why the value could not be parsed, in OpenSearch's wording.
        reason: String,
        /// The offending value, for the error preview.
        preview: String,
    },

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
