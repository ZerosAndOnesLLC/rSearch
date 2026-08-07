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

/// What a detached peer push reads from: small objects travel as bytes,
/// splits stream from the durable copy in the local root (never the
/// caller's staging file, which may be deleted once the write is acked).
#[derive(Clone)]
enum PushSource {
    Bytes(Bytes),
    File(std::path::PathBuf),
}

/// Storage backend that replicates every object across peer nodes,
/// backed by a local [`FsStorage`] root plus HTTP pushes/reads to peers.
pub struct ReplicatedStorage {
    local: FsStorage,
    placement: Arc<dyn Placement>,
    client: PeerClient,
    node_id: String,
    replication_factor: usize,
    write_quorum: usize,
}

impl ReplicatedStorage {
    /// Build a replicated backend over a local root, a placement tracker,
    /// and a peer HTTP client. `write_quorum` is clamped to
    /// `replication_factor` (floor 1) at write time.
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
    /// copy toward the quorum and acks as soon as the quorum holds —
    /// remaining pushes finish detached (they read from the durable root
    /// copy) and the repair job closes any gap to the full factor. Under
    /// quorum, the local copy, the placement rows, AND any copies that
    /// did land on peers are rolled back, so a failed write leaves
    /// nothing behind for the caller's WAL retry to collide with.
    async fn fan_out(&self, key: &str, source: PushSource) -> StorageResult<()> {
        let needed = self.replication_factor.saturating_sub(1);
        let quorum = self.write_quorum.min(self.replication_factor).max(1);
        let targets = if needed == 0 {
            Vec::new()
        } else {
            self.placement
                .write_targets(std::slice::from_ref(&self.node_id), needed)
                .await?
        };
        let mut pushes = futures::stream::FuturesUnordered::new();
        for target in targets {
            let Some(address) = target.address.clone() else { continue };
            let client = self.client.clone();
            let key = key.to_string();
            let source = source.clone();
            let id = target.id.clone();
            pushes.push(tokio::spawn(async move {
                let result = match &source {
                    PushSource::Bytes(data) => {
                        client.push_bytes(&address, &key, data.clone()).await
                    }
                    PushSource::File(path) => client.push_file(&address, &key, path).await,
                };
                match result {
                    Ok(()) => (id, address, true),
                    Err(e) => {
                        warn!(key, peer = %id, error = %e, "replica push failed");
                        (id, address, false)
                    }
                }
            }));
        }

        use futures::StreamExt;
        let mut copies = 1usize; // the local copy
        let mut pushed: Vec<(String, String)> = Vec::new();
        while copies < quorum {
            match pushes.next().await {
                Some(Ok((id, address, true))) => {
                    copies += 1;
                    pushed.push((id, address));
                }
                Some(Ok((_, _, false))) | Some(Err(_)) => {}
                None => break,
            }
        }
        if copies >= quorum {
            // Stragglers keep running toward the full factor; their
            // receivers record their own rows on completion.
            drop(pushes);
            return Ok(());
        }
        // Quorum failed: every push has resolved, so `pushed` is the
        // complete set of peers holding a copy — delete those too (their
        // DELETE handler drops the file and its placement row).
        let _ = self.local.delete(key).await;
        for (id, address) in &pushed {
            if let Err(e) = self.client.delete(address, key).await {
                warn!(key, peer = %id, error = %e, "rollback delete failed; copy orphaned");
            }
        }
        let _ = self.placement.remove_all(key).await;
        Err(StorageError::Backend {
            key: key.to_string(),
            message: format!(
                "write quorum not met: {copies} of {quorum} copies (factor {})",
                self.replication_factor
            ),
        })
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
        if let Err(e) = self
            .placement
            .record(key, &self.node_id, data.len() as i64)
            .await
        {
            // Unrecorded local files are invisible to GC — don't keep one.
            let _ = self.local.delete(key).await;
            return Err(e);
        }
        self.fan_out(key, PushSource::Bytes(data)).await
    }

    async fn put_file(&self, key: &str, local: &Path) -> StorageResult<()> {
        self.local.put_file(key, local).await?;
        let size = self.local.size(key).await?;
        if let Err(e) = self.placement.record(key, &self.node_id, size as i64).await {
            let _ = self.local.delete(key).await;
            return Err(e);
        }
        self.fan_out(key, PushSource::File(self.local.object_path(key)?))
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
