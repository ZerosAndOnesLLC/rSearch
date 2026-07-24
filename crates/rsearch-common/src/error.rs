use thiserror::Error;

#[derive(Debug, Error)]
pub enum RsearchError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("metastore error: {0}")]
    Metastore(String),

    #[error("index error: {0}")]
    Index(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, RsearchError>;
