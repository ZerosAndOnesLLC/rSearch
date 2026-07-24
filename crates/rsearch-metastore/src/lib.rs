//! Postgres metastore: the cluster's coordination layer. Tracks streams,
//! split lifecycle (staged → published → marked_for_delete), and node
//! liveness. All cross-node state lives here or in object storage.

mod error;
mod metastore;
mod nodes;
mod types;

pub use error::{MetastoreError, MetastoreResult};
pub use metastore::Metastore;
pub use types::{NodeRecord, SplitRecord, SplitState, StreamRecord};
