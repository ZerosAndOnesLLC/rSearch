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
use rsearch_metastore::{Metastore, SplitRecord};
use rsearch_storage::Storage;
use tracing::{error, info, warn};

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
        metrics: Arc<crate::metrics::ControlMetrics>,
    ) -> anyhow::Result<Self> {
        let data_dir = std::path::PathBuf::from(&config.node.data_dir);
        let cache = Arc::new(SplitCache::new(data_dir.join("cache/control"), 1 << 30)?);
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
        let Some(ctl) = &self.replication else {
            return Ok(());
        };
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
        for entry in under {
            let key = &entry.storage_key;
            // Holders must be reachable *now* to serve as copy sources.
            let holders = self.metastore.live_holders_of(key, 30.0).await?;
            let Some(source) = holders.iter().find_map(|h| h.address.as_deref()) else {
                warn!(key, "repair: no live holder — object currently unavailable");
                continue;
            };
            let missing = (ctl.replication_factor as usize).saturating_sub(holders.len());
            if missing == 0 {
                continue;
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
                continue;
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
            let keys = self.metastore.locations_on_node(&node.id, 50).await?;
            if keys.is_empty() {
                info!(node = %node.id, "drain complete: node holds no objects and can be shut down");
                continue;
            }
            for key in &keys {
                let holders = self.metastore.live_holders_of(key, 30.0).await?;
                let Some(source) = holders.iter().find_map(|h| h.address.as_deref()) else {
                    warn!(key, "drain: no live holder to copy from; skipping");
                    continue;
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
                } else {
                    warn!(
                        key,
                        node = %node.id,
                        safe,
                        factor,
                        "drain: not enough non-draining nodes to hold the factor"
                    );
                }
            }
        }
        Ok(())
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
            .find(|(_, group)| group.len() >= 2)
            .map(|(id, group)| {
                let take = group.len().min(self.config.merge_max_group);
                (id, group[..take].to_vec())
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

        // Export docs from every source split (blocking reads).
        let mut all_docs: Vec<(serde_json::Value, i64)> = Vec::new();
        for split in &group {
            let reader = Arc::new(
                SplitReader::open(self.storage.clone(), &split.storage_key, self.cache.clone())
                    .await?,
            );
            let docs =
                tokio::task::spawn_blocking(move || reader.export_docs()).await??;
            all_docs.extend(docs);
        }

        // Re-index into one split.
        let stream_name = stream.name.clone();
        let work_dir = self.work_dir.clone();
        let budget = self.memory_budget;
        let packaged = tokio::task::spawn_blocking(move || {
            let mut builder = SplitBuilder::new(stream_name, schema, &work_dir, budget)?;
            for (doc, ts_millis) in &all_docs {
                // Docs lacking their own timestamp keep the original one
                // via the fallback.
                builder.add_json(doc, rsearch_index::DateTime::from_timestamp_millis(*ts_millis))?;
            }
            builder.finish()
        })
        .await??;

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
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let expired = self.metastore.expired_splits(now_millis).await?;
        if !expired.is_empty() {
            let marked = self.metastore.mark_splits_for_delete(&expired).await?;
            info!(marked, "retention: expired splits marked for delete");
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
