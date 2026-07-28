use async_trait::async_trait;

use crate::error::StorageResult;

/// A peer node that can hold object copies.
#[derive(Debug, Clone)]
pub struct PeerNode {
    pub id: String,
    /// Dialable base URL from the node's heartbeat (advertise_url).
    pub address: Option<String>,
}

/// Where object copies live. Implemented over the metastore by the server
/// so the storage crate stays free of any database dependency.
#[async_trait]
pub trait Placement: Send + Sync + 'static {
    /// Record that `node_id` durably holds `key`.
    async fn record(&self, key: &str, node_id: &str, size_bytes: i64) -> StorageResult<()>;

    /// Drop every placement row for `key` (object deleted cluster-wide).
    async fn remove_all(&self, key: &str) -> StorageResult<()>;

    /// Live nodes holding a copy of `key`.
    async fn live_holders(&self, key: &str) -> StorageResult<Vec<PeerNode>>;

    /// Up to `count` live nodes to receive new copies, excluding `exclude`,
    /// preferring nodes holding the fewest total bytes.
    async fn write_targets(&self, exclude: &[String], count: usize)
    -> StorageResult<Vec<PeerNode>>;

    /// Recorded size of `key`, if any copy is registered.
    async fn size_of(&self, key: &str) -> StorageResult<Option<i64>>;

    /// All keys under a prefix (the placement table is authoritative).
    async fn list_keys(&self, prefix: &str) -> StorageResult<Vec<String>>;
}
