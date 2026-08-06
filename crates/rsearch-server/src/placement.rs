//! Metastore-backed [`Placement`] implementation for the replicated
//! storage backend (the storage crate stays database-free).

use async_trait::async_trait;
use rsearch_metastore::Metastore;
use rsearch_storage::{PeerNode, Placement, StorageError, StorageResult};

/// Holders/targets must have heartbeated this recently to count as
/// reachable for reads and writes (heartbeats fire every 5s).
const LIVE_AFTER_SECS: f64 = 30.0;

pub struct MetastorePlacement {
    metastore: Metastore,
}

impl MetastorePlacement {
    pub fn new(metastore: Metastore) -> Self {
        Self { metastore }
    }
}

fn placement_err(key: &str, e: rsearch_metastore::MetastoreError) -> StorageError {
    StorageError::Backend {
        key: key.to_string(),
        message: format!("placement query failed: {e}"),
    }
}

#[async_trait]
impl Placement for MetastorePlacement {
    async fn record(&self, key: &str, node_id: &str, size_bytes: i64) -> StorageResult<()> {
        self.metastore
            .record_object_location(key, node_id, size_bytes)
            .await
            .map_err(|e| placement_err(key, e))
    }

    async fn remove_all(&self, key: &str) -> StorageResult<()> {
        self.metastore
            .remove_object_locations(key)
            .await
            .map_err(|e| placement_err(key, e))
    }

    async fn live_holders(&self, key: &str) -> StorageResult<Vec<PeerNode>> {
        Ok(self
            .metastore
            .live_holders_of(key, LIVE_AFTER_SECS)
            .await
            .map_err(|e| placement_err(key, e))?
            .into_iter()
            .map(|n| PeerNode {
                id: n.id,
                address: n.address,
            })
            .collect())
    }

    async fn write_targets(
        &self,
        exclude: &[String],
        count: usize,
    ) -> StorageResult<Vec<PeerNode>> {
        Ok(self
            .metastore
            .replication_targets(LIVE_AFTER_SECS, exclude, count as i64, false)
            .await
            .map_err(|e| placement_err("", e))?
            .into_iter()
            .map(|n| PeerNode {
                id: n.id,
                address: n.address,
            })
            .collect())
    }

    async fn size_of(&self, key: &str) -> StorageResult<Option<i64>> {
        self.metastore
            .object_size(key)
            .await
            .map_err(|e| placement_err(key, e))
    }

    async fn list_keys(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.metastore
            .object_keys_with_prefix(prefix)
            .await
            .map_err(|e| placement_err(prefix, e))
    }
}
