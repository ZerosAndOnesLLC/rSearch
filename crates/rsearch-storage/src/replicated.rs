//! Replicated storage backend: HA on plain block storage with no external
//! object store. Every node keeps a local fs object root; writes push
//! copies to peers until a quorum holds them, reads fall back to a live
//! holder over HTTP, deletes fan out. Placement is tracked in the
//! metastore via the [`Placement`] trait; the control leader repairs
//! under-replication in the background.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::warn;

use crate::error::{StorageError, StorageResult};
use crate::fs::FsStorage;
use crate::peer::PeerClient;
use crate::placement::{PeerNode, Placement};
use crate::storage::Storage;

pub struct ReplicatedStorage {
    local: FsStorage,
    placement: Arc<dyn Placement>,
    client: PeerClient,
    node_id: String,
    replication_factor: usize,
    write_quorum: usize,
}

impl ReplicatedStorage {
    pub fn new(
        local: FsStorage,
        placement: Arc<dyn Placement>,
        client: PeerClient,
        node_id: String,
        replication_factor: usize,
        write_quorum: usize,
    ) -> Self {
        Self {
            local,
            placement,
            client,
            node_id,
            replication_factor,
            write_quorum,
        }
    }

    /// Replicate a just-written local object to peers. Counts the local
    /// copy toward the quorum; receivers record their own placement rows.
    /// Under quorum the local copy and row are rolled back so a failed
    /// write leaves nothing behind (the caller's WAL retry re-drives it).
    async fn fan_out<'a, F, Fut>(&'a self, key: &'a str, push: F) -> StorageResult<()>
    where
        F: Fn(&'a PeerClient, String) -> Fut,
        Fut: Future<Output = StorageResult<()>> + Send,
    {
        let needed = self.replication_factor.saturating_sub(1);
        let targets = if needed == 0 {
            Vec::new()
        } else {
            self.placement
                .write_targets(std::slice::from_ref(&self.node_id), needed)
                .await?
        };
        let pushes = targets.iter().filter_map(|t| {
            let address = t.address.clone()?;
            Some(async {
                match push(&self.client, address).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(key, peer = %t.id, error = %e, "replica push failed");
                        false
                    }
                }
            })
        });
        let copies = 1 + futures::future::join_all(pushes)
            .await
            .into_iter()
            .filter(|ok| *ok)
            .count();
        if copies < self.write_quorum.min(self.replication_factor).max(1) {
            // Roll back the local copy so a retried batch (new split id)
            // doesn't strand an orphan object outside split GC.
            let _ = self.local.delete(key).await;
            let _ = self.placement.remove_all(key).await;
            return Err(StorageError::Backend {
                key: key.to_string(),
                message: format!(
                    "write quorum not met: {copies} of {} copies (factor {})",
                    self.write_quorum, self.replication_factor
                ),
            });
        }
        Ok(())
    }

    /// Live holders other than this node, for read fallback.
    async fn remote_holders(&self, key: &str) -> StorageResult<Vec<PeerNode>> {
        Ok(self
            .placement
            .live_holders(key)
            .await?
            .into_iter()
            .filter(|n| n.id != self.node_id)
            .collect())
    }
}

#[async_trait]
impl Storage for ReplicatedStorage {
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()> {
        self.local.put(key, data.clone()).await?;
        self.placement
            .record(key, &self.node_id, data.len() as i64)
            .await?;
        self.fan_out(key, |client, address| {
            let data = data.clone();
            async move { client.push_bytes(&address, key, data).await }
        })
        .await
    }

    async fn put_file(&self, key: &str, local: &Path) -> StorageResult<()> {
        self.local.put_file(key, local).await?;
        let size = self.local.size(key).await?;
        self.placement.record(key, &self.node_id, size as i64).await?;
        self.fan_out(key, |client, address| async move {
            client.push_file(&address, key, local).await
        })
        .await
    }

    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        match self.local.get(key).await {
            Ok(bytes) => return Ok(bytes),
            Err(StorageError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let holders = self.remote_holders(key).await?;
        let mut last = StorageError::NotFound(key.to_string());
        for holder in &holders {
            let Some(address) = &holder.address else { continue };
            match self.client.get(address, key).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    warn!(key, peer = %holder.id, error = %e, "peer read failed");
                    last = e;
                }
            }
        }
        Err(last)
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> StorageResult<Bytes> {
        match self.local.get_range(key, range.clone()).await {
            Ok(bytes) => return Ok(bytes),
            Err(StorageError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let holders = self.remote_holders(key).await?;
        let mut last = StorageError::NotFound(key.to_string());
        for holder in &holders {
            let Some(address) = &holder.address else { continue };
            match self.client.get_range(address, key, range.clone()).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    warn!(key, peer = %holder.id, error = %e, "peer range read failed");
                    last = e;
                }
            }
        }
        Err(last)
    }

    async fn size(&self, key: &str) -> StorageResult<u64> {
        match self.local.size(key).await {
            Ok(size) => return Ok(size),
            Err(StorageError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        match self.placement.size_of(key).await? {
            Some(size) => Ok(size as u64),
            None => Err(StorageError::NotFound(key.to_string())),
        }
    }

    /// Cluster-wide delete: every live holder drops its copy, then all
    /// placement rows go. Copies on currently-dead nodes are orphaned on
    /// that node's disk (documented; the object is unreachable once the
    /// rows are gone).
    async fn delete(&self, key: &str) -> StorageResult<()> {
        for holder in self.remote_holders(key).await? {
            let Some(address) = &holder.address else { continue };
            if let Err(e) = self.client.delete(address, key).await {
                warn!(key, peer = %holder.id, error = %e, "peer delete failed; orphaning copy");
            }
        }
        self.local.delete(key).await?;
        self.placement.remove_all(key).await
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.placement.list_keys(prefix).await
    }
}
