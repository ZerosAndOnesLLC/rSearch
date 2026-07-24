use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetastoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("stream not found: {0}")]
    StreamNotFound(String),

    #[error("split not found or not in expected state: {0}")]
    SplitStateConflict(String),
}

pub type MetastoreResult<T> = Result<T, MetastoreError>;
