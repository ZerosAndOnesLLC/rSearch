//! Per-stream indexer pipeline: bounded queues feed one worker per
//! stream; workers batch by count/age, build splits on blocking threads,
//! upload, publish in the metastore, then confirm the WAL.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use rsearch_index::{IndexMapping, MappedSchema, SplitBuilder};
use rsearch_metastore::Metastore;
use rsearch_storage::Storage;

use crate::error::{IngestError, IngestResult};
use crate::wal::{Wal, WalPos, WalRecord};

#[derive(Clone)]
pub struct PipelineConfig {
    pub max_batch_docs: usize,
    pub max_batch_secs: u64,
    pub queue_capacity: usize,
    pub work_dir: PathBuf,
    pub memory_budget: usize,
    pub node_id: String,
}

struct WorkItem {
    doc: Value,
    /// Original document bytes, stored verbatim as `_source`.
    source: Arc<str>,
    pos: WalPos,
}

/// Ingest metrics counters (monotonic).
#[derive(Default)]
pub struct IngestMetrics {
    pub docs_enqueued: AtomicU64,
    pub bytes_enqueued: AtomicU64,
    pub docs_indexed: AtomicU64,
    pub splits_published: AtomicU64,
    pub flush_failures: AtomicU64,
    pub queue_depth: AtomicU64,
}

struct PipelineInner {
    config: PipelineConfig,
    storage: Arc<dyn Storage>,
    metastore: Metastore,
    wal: Arc<Wal>,
    workers: tokio::sync::Mutex<HashMap<String, mpsc::Sender<WorkItem>>>,
    metrics: IngestMetrics,
    /// Routing rules, refreshed periodically from the metastore.
    rules: std::sync::RwLock<Arc<Vec<rsearch_metastore::RoutingRuleRecord>>>,
}

#[derive(Clone)]
pub struct IngestPipeline {
    inner: Arc<PipelineInner>,
}

fn now_datetime() -> rsearch_index::DateTime {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    rsearch_index::DateTime::from_timestamp_millis(millis)
}

impl IngestPipeline {
    pub fn new(
        config: PipelineConfig,
        storage: Arc<dyn Storage>,
        metastore: Metastore,
        wal: Arc<Wal>,
    ) -> Self {
        let pipeline = Self {
            inner: Arc::new(PipelineInner {
                config,
                storage,
                metastore,
                wal,
                workers: tokio::sync::Mutex::new(HashMap::new()),
                metrics: IngestMetrics::default(),
                rules: std::sync::RwLock::new(Arc::new(Vec::new())),
            }),
        };
        // Keep the routing-rule cache warm.
        let inner = pipeline.inner.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                match inner.metastore.list_routing_rules().await {
                    Ok(rules) => *inner.rules.write().unwrap() = Arc::new(rules),
                    Err(e) => warn!(error = %e, "routing rule refresh failed"),
                }
            }
        });
        pipeline
    }

    /// Load routing rules synchronously. Call before the ingest endpoints
    /// start serving so documents in the startup window are never routed
    /// with an empty rule set (M8).
    pub async fn warm_routing_rules(&self) -> IngestResult<()> {
        let rules = self.inner.metastore.list_routing_rules().await?;
        *self.inner.rules.write().unwrap() = Arc::new(rules);
        Ok(())
    }

    /// Resolve where a document should go: the default stream unless a
    /// `move` rule matches; `copy` rules add extra destinations.
    pub fn expand_routes(&self, default_stream: &str, doc: &Value) -> Vec<String> {
        let rules = self.inner.rules.read().unwrap().clone();
        let mut primary = default_stream.to_string();
        let mut extra: Vec<String> = Vec::new();
        for rule in rules.iter() {
            let matched = match (rule.op.as_str(), doc.get(&rule.field)) {
                (_, None) => false,
                ("exists", Some(_)) => true,
                ("eq", Some(v)) => {
                    v.as_str().map(|s| s == rule.value).unwrap_or_else(|| {
                        v.to_string() == rule.value
                    })
                }
                ("contains", Some(v)) => {
                    v.as_str().map(|s| s.contains(&rule.value)).unwrap_or(false)
                }
                _ => false,
            };
            if matched {
                if rule.copy {
                    extra.push(rule.target_stream.clone());
                } else {
                    primary = rule.target_stream.clone();
                }
            }
        }
        let mut routes = vec![primary];
        for stream in extra {
            if !routes.contains(&stream) {
                routes.push(stream);
            }
        }
        routes
    }

    /// Ingest documents from a non-HTTP input (syslog, GELF): apply
    /// routing, WAL-append the batch, enqueue. Saturated docs are dropped
    /// with a warning (datagram sources have no backpressure channel).
    /// Returns (accepted, dropped).
    pub async fn ingest_external(
        &self,
        default_stream: &str,
        docs: Vec<Value>,
    ) -> IngestResult<(usize, usize)> {
        // Programmatic inputs have no original line, so serialize once and
        // share the Arc across every route.
        let mut pairs: Vec<(String, Value, Arc<str>)> = Vec::new();
        for doc in docs {
            let source: Arc<str> = Arc::from(doc.to_string());
            for stream in self.expand_routes(default_stream, &doc) {
                pairs.push((stream, doc.clone(), source.clone()));
            }
        }
        if pairs.is_empty() {
            return Ok((0, 0));
        }
        let wal = self.inner.wal.clone();
        let wal_items: Vec<(String, Vec<u8>)> = pairs
            .iter()
            .map(|(stream, _, source)| (stream.clone(), source.as_bytes().to_vec()))
            .collect();
        let positions = tokio::task::spawn_blocking(move || wal.append_batch(&wal_items))
            .await
            .map_err(|e| IngestError::Wal(std::io::Error::other(e.to_string())))??;

        let mut accepted = 0;
        let mut dropped = 0;
        for ((stream, doc, source), pos) in pairs.into_iter().zip(positions) {
            match self.enqueue(&stream, doc, source, pos).await {
                Ok(()) => accepted += 1,
                Err(_) => {
                    self.inner.wal.confirm(&[pos]);
                    dropped += 1;
                }
            }
        }
        if dropped > 0 {
            warn!(dropped, "input documents dropped (ingest saturated)");
        }
        Ok((accepted, dropped))
    }

    pub fn metrics(&self) -> &IngestMetrics {
        &self.inner.metrics
    }

    pub fn wal(&self) -> &Arc<Wal> {
        &self.inner.wal
    }

    /// Enqueue a WAL-durable document for indexing. `source` is the exact
    /// bytes stored as `_source` (the client's original line — no
    /// re-serialization). Fails with [`IngestError::Saturated`] when the
    /// stream's queue is full — the caller reports a per-item 429 and
    /// confirms the WAL position.
    pub async fn enqueue(
        &self,
        stream: &str,
        doc: Value,
        source: Arc<str>,
        pos: WalPos,
    ) -> IngestResult<()> {
        let tx = self.worker_for(stream).await?;
        let size = source.len() as u64;
        match tx.try_send(WorkItem { doc, source, pos }) {
            Ok(()) => {
                self.inner.metrics.docs_enqueued.fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .bytes_enqueued
                    .fetch_add(size, Ordering::Relaxed);
                self.inner.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(IngestError::Saturated),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(IngestError::Saturated),
        }
    }

    /// Feed WAL records recovered at startup back through the pipeline.
    /// Uses blocking sends — replay must not be dropped for backpressure.
    pub async fn replay(&self, records: Vec<WalRecord>) -> IngestResult<usize> {
        let count = records.len();
        for record in records {
            match serde_json::from_slice::<Value>(&record.doc) {
                Ok(doc) => {
                    // The WAL payload IS the original source bytes.
                    let source: Arc<str> =
                        Arc::from(String::from_utf8_lossy(&record.doc).into_owned());
                    let tx = self.worker_for(&record.stream).await?;
                    self.inner.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
                    if tx
                        .send(WorkItem {
                            doc,
                            source,
                            pos: record.pos,
                        })
                        .await
                        .is_err()
                    {
                        warn!(stream = %record.stream, "worker gone during replay");
                    }
                }
                Err(e) => {
                    // Unparseable WAL payload: confirm so it doesn't pin
                    // the segment forever.
                    warn!(stream = %record.stream, error = %e, "dropping corrupt WAL doc");
                    self.inner.wal.confirm(&[record.pos]);
                }
            }
        }
        if count > 0 {
            info!(count, "replayed WAL records into pipeline");
        }
        Ok(count)
    }

    async fn worker_for(&self, stream: &str) -> IngestResult<mpsc::Sender<WorkItem>> {
        let mut workers = self.inner.workers.lock().await;
        if let Some(tx) = workers.get(stream) {
            return Ok(tx.clone());
        }
        // First document for this stream: resolve it in the metastore and
        // spin up its worker.
        let record = self.inner.metastore.ensure_stream(stream).await?;
        let mapping = IndexMapping::from_json(&record.mapping).unwrap_or_default();
        let schema = MappedSchema::build(mapping);
        let (tx, rx) = mpsc::channel(self.inner.config.queue_capacity);
        tokio::spawn(stream_worker(
            self.inner.clone(),
            stream.to_string(),
            record.id,
            schema,
            rx,
        ));
        workers.insert(stream.to_string(), tx.clone());
        Ok(tx)
    }
}

async fn stream_worker(
    inner: Arc<PipelineInner>,
    stream: String,
    stream_id: i64,
    schema: MappedSchema,
    mut rx: mpsc::Receiver<WorkItem>,
) {
    let max_docs = inner.config.max_batch_docs.max(1);
    let max_age = Duration::from_secs(inner.config.max_batch_secs.max(1));
    let mut buffer: Vec<WorkItem> = Vec::new();
    let far_future = Duration::from_secs(3600 * 24 * 365);
    let mut deadline = tokio::time::Instant::now() + far_future;
    // Re-resolved before each flush so a mapping change (PUT /{index})
    // takes effect on the next split, not only after a restart (L7).
    let mut schema = schema;
    let mut mapping_json = schema.mapping.to_json();

    loop {
        let flush_now = tokio::select! {
            item = rx.recv() => match item {
                Some(item) => {
                    if buffer.is_empty() {
                        deadline = tokio::time::Instant::now() + max_age;
                    }
                    buffer.push(item);
                    buffer.len() >= max_docs
                }
                None => {
                    // Channel closed: final flush then exit.
                    if !buffer.is_empty() {
                        flush(&inner, &stream, stream_id, &schema, &mut buffer).await;
                    }
                    return;
                }
            },
            _ = tokio::time::sleep_until(deadline) => !buffer.is_empty(),
        };
        if flush_now {
            // Pick up mapping changes before building the split.
            if let Ok(record) = inner.metastore.get_stream(&stream).await
                && record.mapping != mapping_json
            {
                let mapping = IndexMapping::from_json(&record.mapping).unwrap_or_default();
                schema = MappedSchema::build(mapping);
                mapping_json = record.mapping;
            }
            flush(&inner, &stream, stream_id, &schema, &mut buffer).await;
            deadline = tokio::time::Instant::now() + far_future;
        }
    }
}

async fn flush(
    inner: &Arc<PipelineInner>,
    stream: &str,
    stream_id: i64,
    schema: &MappedSchema,
    buffer: &mut Vec<WorkItem>,
) {
    let mut batch = std::mem::take(buffer);
    let count = batch.len() as u64;
    inner.metrics.queue_depth.fetch_sub(count, Ordering::Relaxed);
    let positions: Vec<WalPos> = batch.iter().map(|item| item.pos).collect();

    // Retry the flush with capped backoff on transient errors (S3/DB
    // blips). Data is already WAL-durable, so an in-process retry recovers
    // without a restart; the WAL still backstops a hard crash.
    let mut backoff = Duration::from_millis(200);
    let max_attempts = 8;
    for attempt in 1..=max_attempts {
        match flush_inner(inner, stream, stream_id, schema, batch).await {
            Ok(split_id) => {
                inner.wal.confirm(&positions);
                inner.metrics.docs_indexed.fetch_add(count, Ordering::Relaxed);
                inner.metrics.splits_published.fetch_add(1, Ordering::Relaxed);
                info!(stream, split_id = %split_id, docs = count, "split published");
                return;
            }
            Err((e, returned)) => {
                batch = returned;
                inner.metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                if attempt == max_attempts {
                    error!(
                        stream, docs = count, error = %e,
                        "flush failed after retries; docs retained in WAL for replay on restart"
                    );
                    return;
                }
                warn!(stream, attempt, error = %e, "flush failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Build → upload → stage → publish a batch. On error, returns the batch
/// back alongside the error so the caller can retry (the split-building
/// step is deterministic, so re-running it is safe).
async fn flush_inner(
    inner: &Arc<PipelineInner>,
    stream: &str,
    stream_id: i64,
    schema: &MappedSchema,
    batch: Vec<WorkItem>,
) -> Result<String, (IngestError, Vec<WorkItem>)> {
    let schema = schema.clone();
    let stream_name = stream.to_string();
    let work_dir = inner.config.work_dir.clone();
    let budget = inner.config.memory_budget;

    // The blocking task always hands the batch back so a failed flush can
    // be retried without having lost the documents.
    let (result, batch) = tokio::task::spawn_blocking(move || {
        let build = (|| {
            let mut builder = SplitBuilder::new(stream_name, schema, &work_dir, budget)?;
            let fallback = now_datetime();
            for item in &batch {
                builder.add_json_with_source(&item.doc, Some(&item.source), fallback)?;
            }
            builder.finish()
        })();
        (build, batch)
    })
    .await
    .map_err(|e| {
        (
            IngestError::Index(rsearch_index::IndexError::InvalidDocument(format!(
                "indexing task panicked: {e}"
            ))),
            Vec::new(),
        )
    })?;

    let packaged = match result {
        Ok(packaged) => packaged,
        Err(e) => return Err((IngestError::Index(e), batch)),
    };

    let key = format!("streams/{stream}/{}.split", packaged.meta.split_id);
    if let Err(e) = inner.storage.put_file(&key, &packaged.file_path).await {
        return Err((IngestError::Storage(e), batch));
    }
    if let Err(e) = inner
        .metastore
        .stage_split(
            &packaged.meta.split_id,
            stream_id,
            &key,
            packaged.meta.doc_count as i64,
            packaged.size_bytes as i64,
            packaged.meta.time_start_millis,
            packaged.meta.time_end_millis,
            packaged.footer_len as i64,
            Some(&inner.config.node_id),
        )
        .await
    {
        return Err((IngestError::Metastore(e), batch));
    }
    if let Err(e) = inner.metastore.publish_split(&packaged.meta.split_id).await {
        return Err((IngestError::Metastore(e), batch));
    }
    Ok(packaged.meta.split_id)
}
