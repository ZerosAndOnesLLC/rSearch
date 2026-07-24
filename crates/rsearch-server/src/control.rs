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
}

impl ControlPlane {
    pub fn new(
        config: &RsearchConfig,
        metastore: Metastore,
        storage: Arc<dyn Storage>,
    ) -> anyhow::Result<Self> {
        let data_dir = std::path::PathBuf::from(&config.node.data_dir);
        let cache = Arc::new(SplitCache::new(data_dir.join("cache/control"), 1 << 30)?);
        let search =
            rsearch_search::SearchService::new(metastore.clone(), storage.clone(), cache.clone());
        Ok(Self {
            metastore,
            storage,
            cache,
            config: config.control.clone(),
            node_id: config.node_id(),
            work_dir: data_dir.join("merge"),
            memory_budget: config.ingest.memory_budget_mb << 20,
            search,
            webhook: crate::webhook::WebhookClient::new()?,
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
        if let Err(e) = self.alert_job().await {
            error!(error = %e, "alert job failed");
        }
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
