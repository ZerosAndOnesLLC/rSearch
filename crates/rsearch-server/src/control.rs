//! Control plane: one node at a time (elected via a Postgres advisory
//! lock) runs the background jobs — split merging, retention enforcement,
//! garbage collection, and dead-node expiry. Leadership is just holding
//! the lock connection; kill the leader and another control node takes
//! over on its next attempt.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rsearch_common::config::{ControlConfig, RsearchConfig};
use rsearch_index::{IndexMapping, MappedSchema, SplitBuilder, SplitCache, SplitReader};
use rsearch_metastore::{Metastore, NodeRecord, SplitRecord, UnderReplicatedKey};
use rsearch_storage::Storage;
use tracing::{error, info, warn};

/// Peer transfers per repair/drain pass that run concurrently. Each key's
/// metastore lookups and replicate calls are independent; running them
/// serially made drain throughput a function of per-key latency.
const REPAIR_DRAIN_CONCURRENCY: usize = 8;
/// Ceiling on drain batches processed per node per tick — bounds how long
/// one tick can run while still draining far faster than one batch/tick.
const DRAIN_BATCHES_PER_TICK: usize = 10;
/// How often the full under-replication scan must run even with no
/// membership change — catches writes that never reached the factor.
const REPAIR_FULL_SCAN_SECS: u64 = 300;

pub struct ControlPlane {
    metastore: Metastore,
    storage: Arc<dyn Storage>,
    cache: Arc<SplitCache>,
    config: ControlConfig,
    node_id: String,
    work_dir: std::path::PathBuf,
    memory_budget: usize,
    /// Alert queries execute locally on the control leader.
    search: rsearch_search::SearchService,
    webhook: crate::webhook::WebhookClient,
    /// Present when the replicated storage backend is active: the leader
    /// re-replicates under-held objects between peers.
    replication: Option<ReplicationCtl>,
    /// Shared with the /metrics handler.
    metrics: Arc<crate::metrics::ControlMetrics>,
    /// Gates the O(object_locations) under-replication aggregation so
    /// healthy steady-state ticks don't pay a full-table scan every 15s.
    repair_scan: std::sync::Mutex<RepairScanState>,
}

#[derive(Default)]
struct RepairScanState {
    /// Live-node set at the last scan; a change (death/join) triggers one.
    last_live: std::collections::BTreeSet<String>,
    last_scan: Option<std::time::Instant>,
    /// The previous scan found under-replicated keys — keep scanning every
    /// tick until a scan comes back clean.
    last_found_work: bool,
}

struct ReplicationCtl {
    client: rsearch_storage::PeerClient,
    replication_factor: i64,
    repair_stale_secs: f64,
}

impl ControlPlane {
    pub fn new(
        config: &RsearchConfig,
        metastore: Metastore,
        storage: Arc<dyn Storage>,
        cache: Arc<SplitCache>,
        metrics: Arc<crate::metrics::ControlMetrics>,
    ) -> anyhow::Result<Self> {
        let data_dir = std::path::PathBuf::from(&config.node.data_dir);
        let search =
            rsearch_search::SearchService::new(metastore.clone(), storage.clone(), cache.clone());
        let replication = if config.storage.backend == "replicated" {
            let ca = (!config.cluster.peer_ca_file.is_empty())
                .then_some(config.cluster.peer_ca_file.as_str());
            Some(ReplicationCtl {
                client: rsearch_storage::PeerClient::new(&config.cluster.internal_token, ca)
                    .map_err(|e| anyhow::anyhow!("building repair peer client: {e}"))?,
                replication_factor: config.storage.replication_factor as i64,
                repair_stale_secs: config.control.repair_stale_secs,
            })
        } else {
            None
        };
        Ok(Self {
            metastore,
            storage,
            cache,
            config: config.control.clone(),
            node_id: config.node_id(),
            work_dir: data_dir.join("merge"),
            memory_budget: config.ingest.memory_budget_mb << 20,
            search,
            webhook: crate::webhook::WebhookClient::new(config.control.allow_insecure_webhooks)?,
            replication,
            metrics,
            repair_scan: std::sync::Mutex::new(RepairScanState::default()),
        })
    }

    /// Run forever: contend for leadership, run jobs while leader.
    pub async fn run(self) {
        loop {
            let mut leader_conn = match self.metastore.try_acquire_leadership().await {
                Ok(Some(conn)) => conn,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_secs(self.config.interval_secs)).await;
                    continue;
                }
                Err(e) => {
                    warn!(error = %e, "leadership attempt failed");
                    tokio::time::sleep(Duration::from_secs(self.config.interval_secs)).await;
                    continue;
                }
            };
            info!(node = %self.node_id, "acquired control leadership");
            self.metrics.leader.store(true, std::sync::atomic::Ordering::Relaxed);
            loop {
                self.tick().await;
                tokio::time::sleep(Duration::from_secs(self.config.interval_secs)).await;
                if !Metastore::leadership_alive(&mut leader_conn).await {
                    warn!("leadership connection lost; abdicating");
                    // Dead connection: the lock is already freed server-side,
                    // just drop it.
                    break;
                }
            }
            self.metrics.leader.store(false, std::sync::atomic::Ordering::Relaxed);
            // Explicitly release the advisory lock so a healthy connection
            // doesn't keep leadership pinned in the pool (M7).
            Metastore::release_leadership(leader_conn).await;
        }
    }

    async fn tick(&self) {
        if let Err(e) = self.merge_job().await {
            error!(error = %e, "merge job failed");
        }
        if let Err(e) = self.retention_job().await {
            error!(error = %e, "retention job failed");
        }
        if let Err(e) = self.staged_orphan_job().await {
            error!(error = %e, "staged-orphan sweep failed");
        }
        if let Err(e) = self.gc_job().await {
            error!(error = %e, "gc job failed");
        }
        if let Err(e) = self.metastore.expire_dead_nodes(3600.0).await {
            error!(error = %e, "node expiry failed");
        }
        if let Err(e) = self.repair_job().await {
            error!(error = %e, "repair job failed");
        }
        if let Err(e) = self.drain_job().await {
            error!(error = %e, "drain job failed");
        }
        if let Err(e) = self.alert_job().await {
            error!(error = %e, "alert job failed");
        }
    }

    /// Re-replicate objects held by fewer than replication_factor live
    /// nodes (replicated backend only). Most-endangered keys first; each
    /// copy is a leader-instructed pull on the target so the transfer
    /// streams peer-to-peer, never through the leader.
    async fn repair_job(&self) -> anyhow::Result<()> {
        use futures::stream::{self, StreamExt};
        let Some(ctl) = &self.replication else {
            return Ok(());
        };
        // The under-replication query aggregates the whole placement
        // table, so a healthy cluster shouldn't pay it every tick: scan
        // when the live-node set changed (a node died or joined), while
        // the previous scan still found work, and on a periodic deadline
        // (catches writes that never reached the factor with no
        // membership change at all).
        let nodes = self.metastore.list_nodes().await?;
        let live: std::collections::BTreeSet<String> = nodes
            .into_iter()
            .filter(|n| n.heartbeat_age_secs < ctl.repair_stale_secs)
            .map(|n| n.id)
            .collect();
        {
            let mut scan = self.repair_scan.lock().unwrap();
            let deadline_due = scan
                .last_scan
                .map(|at| at.elapsed().as_secs() >= REPAIR_FULL_SCAN_SECS)
                .unwrap_or(true);
            if live == scan.last_live && !scan.last_found_work && !deadline_due {
                return Ok(());
            }
            scan.last_live = live;
            scan.last_scan = Some(std::time::Instant::now());
        }
        // repair_stale_secs doubles as the fresh-write grace: a key whose
        // newest row is younger may still be mid-push (quorum acks detach
        // the remaining replica pushes).
        let under = self
            .metastore
            .under_replicated_keys(
                ctl.replication_factor,
                ctl.repair_stale_secs,
                ctl.repair_stale_secs,
                20,
            )
            .await?;
        self.metrics
            .repair_pending_keys
            .store(under.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.repair_scan.lock().unwrap().last_found_work = !under.is_empty();
        // Keys repair independently; bounded concurrency overlaps their
        // metastore lookups and peer transfers instead of running the
        // whole batch serially.
        let futs: Vec<_> = under
            .iter()
            .map(|entry| async move {
                if let Err(e) = self.repair_one(ctl, entry).await {
                    warn!(key = %entry.storage_key, error = %e, "repair: key failed");
                }
            })
            .collect();
        stream::iter(futs)
            .buffer_unordered(REPAIR_DRAIN_CONCURRENCY)
            .collect::<Vec<()>>()
            .await;
        Ok(())
    }

    async fn repair_one(
        &self,
        ctl: &ReplicationCtl,
        entry: &UnderReplicatedKey,
    ) -> anyhow::Result<()> {
        let key = &entry.storage_key;
        // Holders must be reachable *now* to serve as copy sources.
        let holders = self.metastore.live_holders_of(key, 30.0).await?;
        let Some(source) = holders.iter().find_map(|h| h.address.as_deref()) else {
            warn!(key, "repair: no live holder — object currently unavailable");
            return Ok(());
        };
        let missing = (ctl.replication_factor as usize).saturating_sub(holders.len());
        if missing == 0 {
            return Ok(());
        }
        let exclude: Vec<String> = holders.iter().map(|h| h.id.clone()).collect();
        let mut targets = self
            .metastore
            .replication_targets(30.0, &exclude, missing as i64, false)
            .await?;
        if targets.len() < missing {
            // Last resort: a draining node beats losing the last copy.
            // The drain job moves the copy off again once a real
            // target exists (#4).
            let mut exclude_more = exclude.clone();
            exclude_more.extend(targets.iter().map(|t| t.id.clone()));
            let fallback = self
                .metastore
                .replication_targets(
                    30.0,
                    &exclude_more,
                    (missing - targets.len()) as i64,
                    true,
                )
                .await?;
            for target in fallback {
                warn!(
                    key,
                    target = %target.id,
                    "repair: using draining node as last-resort copy target"
                );
                targets.push(target);
            }
        }
        if targets.is_empty() {
            // Even draining nodes were considered above, so this means
            // every live node already holds a copy — the cluster is
            // simply short of nodes for the factor.
            warn!(
                key,
                live_holders = holders.len(),
                missing,
                "repair: no eligible target nodes — every live node \
                 already holds a copy"
            );
            return Ok(());
        }
        for target in targets {
            let Some(target_addr) = target.address.as_deref() else { continue };
            match tokio::time::timeout(
                Duration::from_secs(600),
                ctl.client.replicate(target_addr, key, source),
            )
            .await
            {
                Ok(Ok(())) => {
                    self.metrics
                        .repair_copies_restored
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    info!(key, target = %target.id, "repair: copy restored");
                }
                Ok(Err(e)) => {
                    self.metrics
                        .repair_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!(key, target = %target.id, error = %e, "repair: replicate failed");
                }
                Err(_) => {
                    self.metrics
                        .repair_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!(key, target = %target.id, "repair: replicate timed out");
                }
            }
        }
        Ok(())
    }

    /// Run due alerts: count hits over the window, compare, fire webhook.
    async fn alert_job(&self) -> anyhow::Result<()> {
        let due = self.metastore.due_alerts().await?;
        for alert in due {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let window_start = now_ms - alert.window_secs * 1000;
            let mut filters = vec![serde_json::json!({
                "range": {"@timestamp": {"gte": window_start, "lte": now_ms}}
            })];
            if alert.query.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                filters.push(alert.query.clone());
            }
            let body = serde_json::json!({
                "query": {"bool": {"filter": filters}},
                "size": 0,
            });
            let count = match rsearch_search::SearchRequest::parse(&alert.stream, &body) {
                Ok(request) => match self.search.search(request).await {
                    Ok(result) => result["hits"]["total"]["value"].as_i64().unwrap_or(0),
                    Err(e) => {
                        self.metastore
                            .record_alert_run(&alert.name, &format!("error: {e}"), None)
                            .await?;
                        continue;
                    }
                },
                Err(e) => {
                    self.metastore
                        .record_alert_run(&alert.name, &format!("error: {e}"), None)
                        .await?;
                    continue;
                }
            };
            let triggered = match alert.condition_op.as_str() {
                "lt" => count < alert.threshold,
                _ => count > alert.threshold,
            };
            let status = if triggered {
                let payload = serde_json::json!({
                    "alert": alert.name,
                    "stream": alert.stream,
                    "count": count,
                    "condition": format!("count {} {}", alert.condition_op, alert.threshold),
                    "window_secs": alert.window_secs,
                    "timestamp_millis": now_ms,
                    "node": self.node_id,
                });
                match self.webhook.post_json(&alert.webhook_url, &payload).await {
                    Ok(status_code) => {
                        info!(alert = %alert.name, count, status_code, "alert fired");
                        "fired".to_string()
                    }
                    Err(e) => {
                        warn!(alert = %alert.name, error = %e, "webhook delivery failed");
                        format!("webhook_error: {e}")
                    }
                }
            } else {
                "ok".to_string()
            };
            self.metastore
                .record_alert_run(&alert.name, &status, Some(count))
                .await?;
        }
        Ok(())
    }

    /// Copy objects off draining nodes (replicated backend only). Each
    /// key gets `replication_factor` copies on non-draining live nodes,
    /// then the draining node's placement row is dropped; once a node has
    /// no rows left it can be shut down. The node keeps serving reads the
    /// whole time, so there is no availability dip.
    async fn drain_job(&self) -> anyhow::Result<()> {
        use futures::stream::{self, StreamExt};
        let nodes = self.metastore.list_nodes().await?;
        // A draining flag outliving the warn window is likely a forgotten
        // DELETE: the node looks healthy but silently takes no writes and
        // no new copies (#4). Warn every tick until cleared.
        for node in nodes.iter().filter(|n| n.draining) {
            if let Some(age) = node.draining_since_secs
                && age > self.config.drain_warn_secs
            {
                warn!(
                    node = %node.id,
                    draining_hours = format!("{:.1}", age / 3600.0),
                    "node has been draining for a long time — it takes no \
                     writes or repair copies; if unintended, clear with \
                     DELETE /_rsearch/nodes/{{id}}/drain"
                );
            }
        }
        let Some(ctl) = &self.replication else {
            return Ok(());
        };
        let factor = ctl.replication_factor as usize;
        for node in nodes.iter().filter(|n| n.draining) {
            // Multiple batches per tick with concurrent per-key transfers;
            // one serial 50-key batch per 15s tick made draining a large
            // node take days. A batch with any stuck key stops until the
            // next tick rather than re-fetching the same keys in a loop.
            for _ in 0..DRAIN_BATCHES_PER_TICK {
                let keys = self.metastore.locations_on_node(&node.id, 50).await?;
                if keys.is_empty() {
                    info!(node = %node.id, "drain complete: node holds no objects and can be shut down");
                    break;
                }
                let futs: Vec<_> = keys
                    .iter()
                    .map(|key| async move {
                        match self.drain_one(ctl, node, factor, key).await {
                            Ok(moved) => moved,
                            Err(e) => {
                                warn!(key = %key, error = %e, "drain: key failed");
                                false
                            }
                        }
                    })
                    .collect();
                let results: Vec<bool> = stream::iter(futs)
                    .buffer_unordered(REPAIR_DRAIN_CONCURRENCY)
                    .collect()
                    .await;
                if results.iter().any(|moved| !moved) {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Drain a single key off `node`: ensure `factor` copies exist on
    /// non-draining nodes, then drop the draining node's copy. Returns
    /// whether the node's placement row for the key is gone.
    async fn drain_one(
        &self,
        ctl: &ReplicationCtl,
        node: &NodeRecord,
        factor: usize,
        key: &str,
    ) -> anyhow::Result<bool> {
        let holders = self.metastore.live_holders_of(key, 30.0).await?;
        let Some(source) = holders.iter().find_map(|h| h.address.as_deref()) else {
            warn!(key, "drain: no live holder to copy from; skipping");
            return Ok(false);
        };
        // Copies already safe on nodes that are staying.
        let mut safe = holders.iter().filter(|h| !h.draining).count();
        if safe < factor {
            let exclude: Vec<String> = holders.iter().map(|h| h.id.clone()).collect();
            let targets = self
                .metastore
                .replication_targets(30.0, &exclude, (factor - safe) as i64, false)
                .await?;
            for target in targets {
                let Some(addr) = target.address.as_deref() else { continue };
                match tokio::time::timeout(
                    Duration::from_secs(600),
                    ctl.client.replicate(addr, key, source),
                )
                .await
                {
                    Ok(Ok(())) => {
                        safe += 1;
                        self.metrics
                            .drain_copies_moved
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        info!(key, target = %target.id, "drain: copy moved");
                    }
                    Ok(Err(e)) => {
                        self.metrics
                            .drain_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        warn!(key, target = %target.id, error = %e, "drain: replicate failed");
                    }
                    Err(_) => {
                        self.metrics
                            .drain_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        warn!(key, target = %target.id, "drain: replicate timed out");
                    }
                }
            }
        }
        if safe >= factor {
            // Prefer a peer DELETE: the draining node drops the
            // file AND its own row, so the disk space returns and
            // an undrained node can't later serve cluster-deleted
            // objects from leftovers. Fall back to just the row if
            // the node is unreachable.
            let deleted = match node.address.as_deref() {
                Some(addr) => ctl.client.delete(addr, key).await.is_ok(),
                None => false,
            };
            if !deleted {
                self.metastore.remove_object_location(key, &node.id).await?;
            }
            Ok(true)
        } else {
            warn!(
                key,
                node = %node.id,
                safe,
                factor,
                "drain: not enough non-draining nodes to hold the factor"
            );
            Ok(false)
        }
    }

    /// Combine the first group of >= 2 small published splits in one
    /// stream into a single split. One merge per tick bounds the work.
    async fn merge_job(&self) -> anyhow::Result<()> {
        let small = self
            .metastore
            .small_published_splits(self.config.merge_min_mb << 20, 200)
            .await?;
        let mut by_stream: BTreeMap<i64, Vec<&SplitRecord>> = BTreeMap::new();
        for split in &small {
            by_stream.entry(split.stream_id).or_default().push(split);
        }
        let Some((stream_id, group)) = by_stream
            .into_iter()
            .find_map(|(id, group)| {
                let group = merge_candidates(group);
                (group.len() >= 2).then(|| {
                    let take = group.len().min(self.config.merge_max_group);
                    (id, group[..take].to_vec())
                })
            })
        else {
            return Ok(());
        };

        let stream = self.metastore.get_stream_by_id(stream_id).await?;
        let mapping = IndexMapping::from_json(&stream.mapping).unwrap_or_default();
        let schema = MappedSchema::build(mapping);
        info!(
            stream = %stream.name,
            splits = group.len(),
            docs = group.iter().map(|s| s.doc_count).sum::<i64>(),
            "merging small splits"
        );

        // Stream every source split's docs straight into the new builder,
        // one split at a time on blocking threads. Peak memory is one doc
        // plus the Tantivy writer budget — the merge group's parsed corpus
        // (GBs at default config) is never materialized.
        let stream_name = stream.name.clone();
        let work_dir = self.work_dir.clone();
        let budget = self.memory_budget;
        let mut builder = tokio::task::spawn_blocking(move || {
            SplitBuilder::new(stream_name, schema, &work_dir, budget)
        })
        .await??;
        for split in &group {
            let reader = Arc::new(
                SplitReader::open(self.storage.clone(), &split.storage_key, self.cache.clone())
                    .await?,
            );
            builder = tokio::task::spawn_blocking(move || {
                reader.for_each_doc(|doc, ts_millis| {
                    // Docs lacking their own timestamp keep the original
                    // one via the fallback.
                    builder.add_json(
                        doc,
                        rsearch_index::DateTime::from_timestamp_millis(ts_millis),
                    )
                })?;
                Ok::<_, rsearch_index::IndexError>(builder)
            })
            .await??;
        }
        let packaged = tokio::task::spawn_blocking(move || builder.finish()).await??;

        let key = format!("streams/{}/{}.split", stream.name, packaged.meta.split_id);
        self.storage.put_file(&key, &packaged.file_path).await?;
        self.metastore
            .stage_split(
                &packaged.meta.split_id,
                stream_id,
                &key,
                packaged.meta.doc_count as i64,
                packaged.size_bytes as i64,
                packaged.meta.time_start_millis,
                packaged.meta.time_end_millis,
                packaged.footer_len as i64,
                Some(&self.node_id),
            )
            .await?;
        let old_ids: Vec<String> = group.iter().map(|s| s.split_id.clone()).collect();
        self.metastore
            .swap_splits(&old_ids, &packaged.meta.split_id)
            .await?;
        info!(
            merged_into = %packaged.meta.split_id,
            sources = old_ids.len(),
            docs = packaged.meta.doc_count,
            "merge complete"
        );
        Ok(())
    }

    /// Sweep splits stuck in `staged` (crash between stage and publish):
    /// mark them for deletion so the GC job removes the orphaned object
    /// and row (M5).
    async fn staged_orphan_job(&self) -> anyhow::Result<()> {
        let stale = self
            .metastore
            .stale_staged_splits(self.config.staged_orphan_secs, 100)
            .await?;
        if stale.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = stale.iter().map(|s| s.split_id.clone()).collect();
        let marked = self.metastore.mark_splits_for_delete(&ids).await?;
        info!(marked, "swept orphaned staged splits for deletion");
        Ok(())
    }

    async fn retention_job(&self) -> anyhow::Result<()> {
        const BATCH: i64 = 10_000;
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        // Batched: enabling retention on a large old stream marks its
        // whole backlog without ever holding millions of ids in memory.
        loop {
            let expired = self.metastore.expired_splits(now_millis, BATCH).await?;
            if expired.is_empty() {
                break;
            }
            let finished = (expired.len() as i64) < BATCH;
            let marked = self.metastore.mark_splits_for_delete(&expired).await?;
            info!(marked, "retention: expired splits marked for delete");
            if finished {
                break;
            }
        }
        Ok(())
    }

    async fn gc_job(&self) -> anyhow::Result<()> {
        let candidates = self
            .metastore
            .gc_candidates(self.config.gc_grace_secs, 50)
            .await?;
        for split in candidates {
            match self.storage.delete(&split.storage_key).await {
                Ok(()) => {
                    self.metastore.delete_split_row(&split.split_id).await?;
                    info!(split_id = %split.split_id, "gc: split deleted");
                }
                Err(e) => warn!(split_id = %split.split_id, error = %e, "gc: delete failed"),
            }
        }
        Ok(())
    }
}

/// A split whose size exceeds this many times the combined size of every
/// smaller merge candidate in its stream is left out of the merge group.
const MERGE_SKEW_FACTOR: i64 = 10;

/// Drop dominant splits from a stream's merge group (#15). Merging is a
/// full rewrite of every source, so folding a trickle of tiny splits into
/// one dominant under-threshold split re-wrote its bytes every tick —
/// quadratic write amplification (plus re-replication, GC churn, and
/// cache pressure) until it finally crossed `merge_min_mb`. Excluding a
/// split that is more than [`MERGE_SKEW_FACTOR`]× the sum of its smaller
/// peers lets the small splits merge among themselves; the dominant one
/// rejoins once they have accumulated to comparable size, so each byte is
/// rewritten O(log) times instead of every tick. Relative (time) order of
/// the survivors is preserved to keep merged time ranges tight.
fn merge_candidates(group: Vec<&SplitRecord>) -> Vec<&SplitRecord> {
    let mut by_size: Vec<i64> = group.iter().map(|s| s.size_bytes).collect();
    by_size.sort_unstable_by_key(|size| std::cmp::Reverse(*size));
    // Find the largest suffix of the size-ranked list where no split
    // dominates the rest; everything ranked above it is excluded. The
    // last-ranked split is never tested — any size "dominates" an empty
    // rest, and a lone survivor is already too small a group to merge.
    let mut cut = 0;
    for i in 0..by_size.len().saturating_sub(1) {
        let rest: i64 = by_size[i + 1..].iter().sum();
        if by_size[i] > MERGE_SKEW_FACTOR.saturating_mul(rest) {
            cut = i + 1;
        } else {
            break;
        }
    }
    if cut == 0 {
        return group;
    }
    let threshold = by_size[cut - 1];
    // Exclude by size threshold: `cut` splits are >= threshold in rank
    // order, and ties at the threshold can only make the group more
    // balanced, so a strict comparison keeps exactly the survivors.
    group
        .into_iter()
        .filter(|s| s.size_bytes < threshold)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(id: i64, size_bytes: i64) -> SplitRecord {
        SplitRecord {
            id,
            split_id: format!("split-{id}"),
            stream_id: 1,
            state: "published".to_string(),
            storage_key: format!("streams/test/{id}.split"),
            doc_count: size_bytes / 100,
            size_bytes,
            time_start_millis: id * 1_000,
            time_end_millis: id * 1_000 + 999,
            footer_len: 0,
            created_by: None,
        }
    }

    fn candidate_ids(splits: &[SplitRecord]) -> Vec<i64> {
        merge_candidates(splits.iter().collect())
            .into_iter()
            .map(|s| s.id)
            .collect()
    }

    #[test]
    fn dominant_split_is_left_out() {
        // The #15 pathology: a 52 MB split plus one trickle split must
        // not merge — the survivor group is too small to act on.
        let splits = [split(1, 52 << 20), split(2, 4 << 10)];
        assert_eq!(candidate_ids(&splits), vec![2]);
    }

    #[test]
    fn tiny_splits_merge_among_themselves() {
        let splits = [
            split(1, 52 << 20),
            split(2, 4 << 10),
            split(3, 6 << 10),
            split(4, 5 << 10),
        ];
        // The giant stays out; the trickle splits keep their time order.
        assert_eq!(candidate_ids(&splits), vec![2, 3, 4]);
    }

    #[test]
    fn comparable_splits_all_merge() {
        let splits = [split(1, 10 << 20), split(2, 8 << 20), split(3, 52 << 20)];
        assert_eq!(candidate_ids(&splits), vec![1, 2, 3]);
    }

    #[test]
    fn dominant_rejoins_once_peers_accumulate() {
        // 5 MB of accumulated smalls x 10 >= 50 MB giant: full merge.
        let splits = [split(1, 50 << 20), split(2, 3 << 20), split(3, 2 << 20)];
        assert_eq!(candidate_ids(&splits), vec![1, 2, 3]);
    }

    #[test]
    fn cascading_dominance_is_cut_at_the_right_rank() {
        // 100 MB dominates (1 MB + 1 KB); 1 MB dominates 1 KB alone —
        // both are excluded, leaving too few to merge.
        let splits = [split(1, 100 << 20), split(2, 1 << 20), split(3, 1 << 10)];
        assert_eq!(candidate_ids(&splits), vec![3]);
    }

    #[test]
    fn single_split_passes_through() {
        // A lone split survives the filter; the caller's >= 2 group-size
        // check is what keeps it from merging with itself.
        let splits = [split(1, 1 << 20)];
        assert_eq!(candidate_ids(&splits), vec![1]);
    }
}
