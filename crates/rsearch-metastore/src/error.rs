use thiserror::Error;

/// Errors returned by metastore operations.
#[derive(Debug, Error)]
pub enum MetastoreError {
    /// Underlying Postgres/sqlx failure (also wraps invalid-input
    /// configuration errors raised before a query runs).
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Embedded schema migration failed at startup.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// No stream with the given name (or id).
    #[error("stream not found: {0}")]
    StreamNotFound(String),

    /// The stream already holds data, so its mode can no longer change.
    #[error("stream '{0}' already holds data; its mode cannot be changed")]
    StreamModeFixed(String),

    /// Split missing, or not in the state a transition requires.
    #[error("split not found or not in expected state: {0}")]
    SplitStateConflict(String),
}

/// Convenience alias for fallible metastore operations.
pub type MetastoreResult<T> = Result<T, MetastoreError>;
