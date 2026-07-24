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
        Self {
            inner: Arc::new(PipelineInner {
                config,
                storage,
                metastore,
                wal,
                workers: tokio::sync::Mutex::new(HashMap::new()),
                metrics: IngestMetrics::default(),
            }),
        }
    }

    pub fn metrics(&self) -> &IngestMetrics {
        &self.inner.metrics
    }

    pub fn wal(&self) -> &Arc<Wal> {
        &self.inner.wal
    }

    /// Enqueue a WAL-durable document for indexing. Fails with
    /// [`IngestError::Saturated`] when the stream's queue is full — the
    /// caller reports a per-item 429 and confirms the WAL position.
    pub async fn enqueue(&self, stream: &str, doc: Value, pos: WalPos) -> IngestResult<()> {
        let tx = self.worker_for(stream).await?;
        let size = doc.to_string().len() as u64; // approximate
        match tx.try_send(WorkItem { doc, pos }) {
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
                    let tx = self.worker_for(&record.stream).await?;
                    self.inner.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
                    if tx.send(WorkItem { doc, pos: record.pos }).await.is_err() {
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
    let batch = std::mem::take(buffer);
    let count = batch.len() as u64;
    inner.metrics.queue_depth.fetch_sub(count, Ordering::Relaxed);
    let positions: Vec<WalPos> = batch.iter().map(|item| item.pos).collect();

    match flush_inner(inner, stream, stream_id, schema, batch).await {
        Ok(split_id) => {
            inner.wal.confirm(&positions);
            inner.metrics.docs_indexed.fetch_add(count, Ordering::Relaxed);
            inner.metrics.splits_published.fetch_add(1, Ordering::Relaxed);
            info!(stream, split_id = %split_id, docs = count, "split published");
        }
        Err(e) => {
            // Documents remain in the WAL and will be replayed on restart.
            inner.metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
            error!(stream, docs = count, error = %e, "flush failed; docs retained in WAL for replay");
        }
    }
}

async fn flush_inner(
    inner: &Arc<PipelineInner>,
    stream: &str,
    stream_id: i64,
    schema: &MappedSchema,
    batch: Vec<WorkItem>,
) -> IngestResult<String> {
    let schema = schema.clone();
    let stream_name = stream.to_string();
    let work_dir = inner.config.work_dir.clone();
    let budget = inner.config.memory_budget;

    let packaged = tokio::task::spawn_blocking(move || {
        let mut builder = SplitBuilder::new(stream_name, schema, &work_dir, budget)?;
        let fallback = now_datetime();
        for item in &batch {
            builder.add_json(&item.doc, fallback)?;
        }
        builder.finish()
    })
    .await
    .map_err(|e| {
        IngestError::Index(rsearch_index::IndexError::InvalidDocument(format!(
            "indexing task panicked: {e}"
        )))
    })??;

    let key = format!("streams/{stream}/{}.split", packaged.meta.split_id);
    inner.storage.put_file(&key, &packaged.file_path).await?;
    inner
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
        .await?;
    inner.metastore.publish_split(&packaged.meta.split_id).await?;
    Ok(packaged.meta.split_id)
}
