//! Object storage abstraction. Local filesystem, S3, and S3-compatible
//! (MinIO) backends are equal citizens: splits and other artifacts only
//! ever go through the [`Storage`] trait.

mod error;
mod factory;
mod fs;
mod peer;
mod s3;
mod storage;

pub use error::{StorageError, StorageResult};
pub use factory::from_config;
pub use fs::FsStorage;
pub use peer::{INTERNAL_TOKEN_HEADER, PeerClient};
pub use s3::S3Storage;
pub use storage::Storage;
