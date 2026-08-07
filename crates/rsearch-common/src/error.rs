use thiserror::Error;

/// Workspace-wide error type shared by all rSearch crates.
#[derive(Debug, Error)]
pub enum RsearchError {
    /// Invalid or unloadable configuration (bad TOML, env vars, TLS
    /// material).
    #[error("configuration error: {0}")]
    Config(String),

    /// Object storage operation failed.
    #[error("storage error: {0}")]
    Storage(String),

    /// Postgres metastore query or connection failed.
    #[error("metastore error: {0}")]
    Metastore(String),

    /// Index build, open, or search failed.
    #[error("index error: {0}")]
    Index(String),

    /// Underlying filesystem/network I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias for results carrying [`RsearchError`].
pub type Result<T> = std::result::Result<T, RsearchError>;
