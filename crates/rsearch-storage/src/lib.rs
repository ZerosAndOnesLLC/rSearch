//! Object storage abstraction. Local filesystem, S3, and S3-compatible
//! (MinIO) backends are equal citizens: splits and other artifacts only
//! ever go through the [`Storage`] trait.

mod error;
mod fs;
mod storage;

pub use error::{StorageError, StorageResult};
pub use fs::FsStorage;
pub use storage::Storage;
