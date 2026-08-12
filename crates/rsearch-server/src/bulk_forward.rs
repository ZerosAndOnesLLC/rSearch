//! Server-side bulk ingest balancing (#19): shippers hold keep-alive
//! connections, so without help every batch lands on whichever node the
//! client happened to connect to. When enabled, the receiving node hands
//! whole `/_bulk` bodies round-robin to live ingest peers over the
//! internal API (cluster-token auth), and relays the peer's response.
//! A handed-off batch is durable in the *target's* WAL before the ack.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use hyper::body::Bytes;
use rsearch_metastore::Metastore;
use rsearch_storage::PeerClient;
use tracing::warn;

/// A peer is a handoff candidate only if it heartbeated this recently.
const PEER_STALE_SECS: f64 = 30.0;
/// How long a fetched peer list is reused before the registry is re-read.
const PEER_LIST_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// Round-robin bulk handoff across the cluster's live ingest nodes.
pub struct BulkForwarder {
    metastore: Metastore,
    client: PeerClient,
    node_id: String,
    /// Digest of the cluster token; inbound handoffs are checked against
    /// it (digest-to-digest, same scheme as the internal object API).
    pub token_digest: String,
    /// `ingest.balance_bulk` — when false this node never hands off, but
    /// still accepts handoffs from peers that do.
    balance: bool,
    counter: AtomicU64,
    /// (targets sorted by id — self included so rotation is fair, fetch time)
    peers: std::sync::Mutex<Option<(Arc<Vec<(String, String)>>, Instant)>>,
    /// Batches handed to a peer / handoffs that fell back to local.
    pub forwarded: AtomicU64,
    pub forward_fallbacks: AtomicU64,
    /// Batches accepted from a peer via the internal endpoint.
    pub received: AtomicU64,
}

impl BulkForwarder {
    pub fn new(
        metastore: Metastore,
        client: PeerClient,
        node_id: String,
        token_digest: String,
        balance: bool,
    ) -> Self {
        Self {
            metastore,
            client,
            node_id,
            token_digest,
            balance,
            counter: AtomicU64::new(0),
            peers: std::sync::Mutex::new(None),
            forwarded: AtomicU64::new(0),
            forward_fallbacks: AtomicU64::new(0),
            received: AtomicU64::new(0),
        }
    }

    /// Live, non-draining ingest nodes with an advertised address, sorted
    /// by id so every node walks the same rotation order.
    async fn candidates(&self) -> Arc<Vec<(String, String)>> {
        if let Some((peers, at)) = self.peers.lock().unwrap().as_ref()
            && at.elapsed() < PEER_LIST_TTL
        {
            return peers.clone();
        }
        let peers: Arc<Vec<(String, String)>> = match self.metastore.list_nodes().await {
            Ok(nodes) => Arc::new(
                nodes
                    .into_iter()
                    .filter(|n| {
                        n.heartbeat_age_secs < PEER_STALE_SECS
                            && !n.draining
                            && n.roles.iter().any(|r| r == "ingest")
                    })
                    .filter_map(|n| n.address.map(|addr| (n.id, addr)))
                    .collect(),
            ),
            Err(e) => {
                // Registry unreachable: keep ingesting locally rather than
                // fail batches over a balancing nicety.
                warn!(error = %e, "bulk balance: listing ingest nodes failed");
                Arc::new(Vec::new())
            }
        };
        *self.peers.lock().unwrap() = Some((peers.clone(), Instant::now()));
        peers
    }

    /// The peer the next batch should go to, or `None` to index locally
    /// (balancing off, no live peers, or the rotation landed on self).
    pub async fn pick_target(&self) -> Option<(String, String)> {
        if !self.balance {
            return None;
        }
        let peers = self.candidates().await;
        if peers.len() < 2 {
            return None;
        }
        let slot = self.counter.fetch_add(1, Ordering::Relaxed) as usize % peers.len();
        let (id, addr) = &peers[slot];
        if *id == self.node_id {
            return None;
        }
        Some((id.clone(), addr.clone()))
    }

    /// Hand a raw bulk body to `addr` and return the peer's verbatim
    /// `(status, response body)`.
    pub async fn forward(
        &self,
        addr: &str,
        default_index: Option<&str>,
        body: Bytes,
    ) -> Result<(u16, Bytes), String> {
        let mut url = url::Url::parse(addr).map_err(|e| format!("peer address '{addr}': {e}"))?;
        url.set_path("/_rsearch/internal/bulk");
        if let Some(index) = default_index {
            url.query_pairs_mut().append_pair("index", index);
        }
        self.client
            .post_raw(url.as_str(), body)
            .await
            .map_err(|e| e.to_string())
    }
}
