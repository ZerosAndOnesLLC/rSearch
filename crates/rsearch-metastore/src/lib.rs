//! Postgres metastore: the cluster's coordination layer. Tracks streams,
//! split lifecycle (staged → published → marked_for_delete), and node
//! liveness. All cross-node state lives here or in object storage.

mod alerts;
mod auth;
mod control;
mod error;
mod metastore;
mod nodes;
mod routing;
mod types;

pub use alerts::AlertRecord;
pub use auth::{ApiKeyRecord, UserRecord};
pub use control::CONTROL_LEADER_LOCK;
pub use routing::RoutingRuleRecord;

pub use error::{MetastoreError, MetastoreResult};
pub use metastore::Metastore;
pub use types::{NodeRecord, SplitRecord, SplitState, StreamRecord, StreamStats};
