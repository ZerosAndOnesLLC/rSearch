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

use rsearch_index::{DocIdentity, IndexMapping, MappedSchema, SplitBuilder};
use rsearch_metastore::Metastore;
use rsearch_storage::Storage;

use crate::error::{IngestError, IngestResult};
use crate::wal::{Wal, WalItem, WalPos, WalReplay};

/// Node-local monotonic write-sequence source: micros since epoch, forced
/// strictly increasing across calls so two writes to the same `_id` on
/// one node always order, even within the same microsecond. Across nodes
/// ordering relies on wall clocks (NTP) — the same assumption retention
/// already makes.
#[derive(Default)]
pub struct SeqClock {
    last: std::sync::atomic::AtomicI64,
}

impl SeqClock {
    /// Next sequence stamp.
    pub fn next(&self) -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        // fetch_update: `last = max(last + 1, now)` atomically.
        self.last
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                Some(now.max(last.saturating_add(1)))
            })
            .map(|prev| now.max(prev.saturating_add(1)))
            .unwrap_or(now)
    }
}

/// Tuning knobs for the ingest pipeline.
#[derive(Clone)]
pub struct PipelineConfig {
    /// Flush a batch into a split once it holds this many documents.
    pub max_batch_docs: usize,
    /// Flush a non-empty batch after this many seconds regardless of
    /// size (bounds time-to-searchable).
    pub max_batch_secs: u64,
    /// Per-stream bounded queue depth in documents; a full queue
    /// rejects with [`IngestError::Saturated`].
    pub queue_capacity: usize,
    /// Local scratch directory splits are built in before upload.
    pub work_dir: PathBuf,
    /// Indexer memory budget in bytes for each split build.
    pub memory_budget: usize,
    /// This node's id, recorded as each split's creator.
    pub node_id: String,
}

struct WorkItem {
    /// Original document bytes. The queue holds only these (not a parsed
    /// Value, which is several times larger in memory) — the indexer
    /// re-parses on the blocking thread. Keeps ingest RSS low, which is
    /// the whole point of the engine.
    source: Arc<str>,
    /// Document identity (`_id`, `_seq`).
    identity: DocIdentity,
    pos: WalPos,
}

/// Ingest metrics counters (monotonic).
#[derive(Default)]
pub struct IngestMetrics {
    /// Documents accepted onto worker queues.
    pub docs_enqueued: AtomicU64,
    /// Source bytes accepted onto worker queues.
    pub bytes_enqueued: AtomicU64,
    /// Documents indexed into built splits.
    pub docs_indexed: AtomicU64,
    /// Splits published in the metastore.
    pub splits_published: AtomicU64,
    /// Failed split flushes (build/upload/publish errors).
    pub flush_failures: AtomicU64,
    /// Current documents queued across all streams (gauge).
    pub queue_depth: AtomicU64,
}

struct PipelineInner {
    config: PipelineConfig,
    storage: Arc<dyn Storage>,
    metastore: Metastore,
    wal: Arc<Wal>,
    /// Stream → worker sender. Read-locked (sync, never across an await)
    /// on the per-document hot path; the write lock is only ever taken to
    /// insert a newly created worker.
    workers: std::sync::RwLock<HashMap<String, mpsc::Sender<WorkItem>>>,
    /// Single-flights first-time worker creation so the metastore resolve
    /// never runs under `workers` — enqueues to existing streams are not
    /// blocked while a new stream's row is being created.
    worker_create: tokio::sync::Mutex<()>,
    metrics: IngestMetrics,
    /// Routing rules, refreshed periodically from the metastore.
    rules: std::sync::RwLock<Arc<Vec<rsearch_metastore::RoutingRuleRecord>>>,
    /// Write-sequence source shared by every ingest entry point.
    seq: SeqClock,
}

/// Cloneable handle to the shared ingest pipeline: routes documents,
/// appends them to the WAL, and feeds per-stream indexer workers.
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
    /// Build the pipeline and spawn the periodic routing-rule refresh
    /// task. Workers are created lazily per stream on first enqueue.
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
                workers: std::sync::RwLock::new(HashMap::new()),
                worker_create: tokio::sync::Mutex::new(()),
                metrics: IngestMetrics::default(),
                rules: std::sync::RwLock::new(Arc::new(Vec::new())),
                seq: SeqClock::default(),
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
        let mut items: Vec<WalItem> = Vec::new();
        for doc in docs {
            let source: Arc<str> = Arc::from(doc.to_string());
            let id = uuid::Uuid::new_v4().simple().to_string();
            let seq = self.next_seq();
            for stream in self.expand_routes(default_stream, &doc) {
                items.push(WalItem {
                    stream,
                    id: id.clone(),
                    seq,
                    doc: source.clone(),
                });
            }
        }
        if items.is_empty() {
            return Ok((0, 0));
        }
        // The items move into the blocking task and back so the WAL append
        // works on the existing strings — no per-document byte copies.
        let wal = self.inner.wal.clone();
        let (items, positions) = tokio::task::spawn_blocking(move || {
            let positions = wal.append_batch(&items);
            (items, positions)
        })
        .await
        .map_err(|e| IngestError::Wal(std::io::Error::other(e.to_string())))?;
        let positions = positions?;

        let mut accepted = 0;
        let mut dropped = 0;
        for (item, pos) in items.into_iter().zip(positions) {
            let identity = DocIdentity::new(item.id, item.seq);
            match self.enqueue(&item.stream, item.doc, identity, pos).await {
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

    /// The pipeline's metrics counters.
    pub fn metrics(&self) -> &IngestMetrics {
        &self.inner.metrics
    }

    /// The shared WAL handle.
    pub fn wal(&self) -> &Arc<Wal> {
        &self.inner.wal
    }

    /// Next write-sequence stamp (`_seq`) for a document accepted by this
    /// node. Take it before the WAL append so the stamp is what gets
    /// persisted and replayed.
    pub fn next_seq(&self) -> i64 {
        self.inner.seq.next()
    }

    /// Enqueue a WAL-durable document for indexing. `source` is the exact
    /// document bytes — stored as `_source` and re-parsed by the indexer.
    /// Fails with [`IngestError::Saturated`] when the stream's queue is
    /// full — the caller reports a per-item 429 and confirms the WAL
    /// position.
    pub async fn enqueue(
        &self,
        stream: &str,
        source: Arc<str>,
        identity: DocIdentity,
        pos: WalPos,
    ) -> IngestResult<()> {
        let tx = self.worker_for(stream).await?;
        let size = source.len() as u64;
        match tx.try_send(WorkItem {
            source,
            identity,
            pos,
        }) {
            Ok(()) => {
                self.note_enqueued(size);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(IngestError::Saturated),
            Err(mpsc::error::TrySendError::Closed(item)) => {
                // The worker idled out between the lookup and the send; its
                // map entry is already removed, so one retry resolves a
                // fresh worker.
                let tx = self.worker_for(stream).await?;
                match tx.try_send(item) {
                    Ok(()) => {
                        self.note_enqueued(size);
                        Ok(())
                    }
                    Err(_) => Err(IngestError::Saturated),
                }
            }
        }
    }

    fn note_enqueued(&self, size: u64) {
        self.inner.metrics.docs_enqueued.fetch_add(1, Ordering::Relaxed);
        self.inner
            .metrics
            .bytes_enqueued
            .fetch_add(size, Ordering::Relaxed);
        self.inner.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    /// Feed WAL records recovered at startup back through the pipeline.
    /// Records are streamed from the segment files one at a time and the
    /// worker queues are bounded, so replaying a large backlog holds at
    /// most O(queue depth) in memory. Uses blocking sends — replay must
    /// not be dropped for backpressure.
    pub async fn replay(&self, records: WalReplay) -> IngestResult<usize> {
        let mut count = 0usize;
        for record in records {
            let record = record.map_err(IngestError::Wal)?;
            count += 1;
            // Validate the payload is parseable JSON; the WAL payload IS
            // the source bytes, so we don't keep the parsed value.
            if serde_json::from_slice::<serde::de::IgnoredAny>(&record.doc).is_err() {
                warn!(stream = %record.stream, "dropping corrupt WAL doc");
                self.inner.wal.confirm(&[record.pos]);
                continue;
            }
            // Valid JSON is valid UTF-8, so the common path builds the Arc
            // straight from the record bytes (one copy, no String detour).
            let source: Arc<str> = match std::str::from_utf8(&record.doc) {
                Ok(text) => Arc::from(text),
                Err(_) => Arc::from(String::from_utf8_lossy(&record.doc).into_owned()),
            };
            let tx = self.worker_for(&record.stream).await?;
            self.inner.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
            if tx
                .send(WorkItem {
                    source,
                    identity: DocIdentity::new(record.id, record.seq),
                    pos: record.pos,
                })
                .await
                .is_err()
            {
                // The item was dropped, so undo the gauge and confirm the
                // position — otherwise the depth drifts and the segment
                // stays pinned forever.
                self.inner.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
                self.inner.wal.confirm(&[record.pos]);
                warn!(stream = %record.stream, "worker gone during replay; record dropped");
            }
        }
        if count > 0 {
            info!(count, "replayed WAL records into pipeline");
        }
        Ok(count)
    }

    async fn worker_for(&self, stream: &str) -> IngestResult<mpsc::Sender<WorkItem>> {
        if let Some(tx) = self.inner.workers.read().unwrap().get(stream) {
            return Ok(tx.clone());
        }
        // First document for this stream: resolve it in the metastore and
        // spin up its worker. Serialized behind `worker_create` (re-checking
        // after acquiring it) so concurrent first-docs create one worker.
        let _create = self.inner.worker_create.lock().await;
        if let Some(tx) = self.inner.workers.read().unwrap().get(stream) {
            return Ok(tx.clone());
        }
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
        self.inner
            .workers
            .write()
            .unwrap()
            .insert(stream.to_string(), tx.clone());
        Ok(tx)
    }
}

/// How long a stream worker sits idle (empty buffer, no incoming docs)
/// before retiring itself. Stream names are client-controlled (bulk
/// `_index` values), so without an exit path every name ever seen pins a
/// task + bounded queue + schema for the life of the process.
const WORKER_IDLE_EXIT_SECS: u64 = 600;

async fn stream_worker(
    inner: Arc<PipelineInner>,
    stream: String,
    stream_id: i64,
    schema: MappedSchema,
    mut rx: mpsc::Receiver<WorkItem>,
) {
    let max_docs = inner.config.max_batch_docs.max(1);
    let max_age = Duration::from_secs(inner.config.max_batch_secs.max(1));
    let idle_exit = Duration::from_secs(WORKER_IDLE_EXIT_SECS);
    let mut buffer: Vec<WorkItem> = Vec::new();
    let mut deadline = tokio::time::Instant::now() + idle_exit;
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
            _ = tokio::time::sleep_until(deadline) => {
                if buffer.is_empty() {
                    // Idle: retire this worker so per-stream state doesn't
                    // accumulate forever for streams that stopped writing.
                    // Order matters — unpublish, close, drain — so no
                    // in-flight send can slip a doc into a dropped queue:
                    // after close() a racing try_send fails and the sender
                    // re-creates a fresh worker via worker_for.
                    inner.workers.write().unwrap().remove(&stream);
                    rx.close();
                    while let Ok(item) = rx.try_recv() {
                        buffer.push(item);
                    }
                    if !buffer.is_empty() {
                        flush(&inner, &stream, stream_id, &schema, &mut buffer).await;
                    }
                    info!(stream, "idle stream worker retired");
                    return;
                }
                true
            },
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
            deadline = tokio::time::Instant::now() + idle_exit;
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
    // without a restart; the WAL still backstops a hard crash. Retries
    // never give up: abandoning the batch would strand its docs (invisible
    // to search) and pin its WAL segments until an operator restarts the
    // process. Backpressure is the bounded queue behind this worker.
    let mut backoff = Duration::from_millis(200);
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        match flush_inner(inner, stream, stream_id, schema, batch).await {
            Ok(Some((split_id, indexed))) => {
                inner.wal.confirm(&positions);
                inner.metrics.docs_indexed.fetch_add(indexed, Ordering::Relaxed);
                inner.metrics.splits_published.fetch_add(1, Ordering::Relaxed);
                if indexed < count {
                    warn!(
                        stream,
                        skipped = count - indexed,
                        "batch published without its invalid docs"
                    );
                }
                info!(stream, split_id = %split_id, docs = indexed, "split published");
                return;
            }
            Ok(None) => {
                // Nothing in the batch was indexable. The docs are still
                // processed: confirm their WAL entries so replay doesn't
                // resurrect (and re-skip) them forever.
                inner.wal.confirm(&positions);
                warn!(stream, docs = count, "batch contained no indexable docs; dropped");
                return;
            }
            Err((e, returned)) => {
                batch = returned;
                inner.metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
                // Escalate periodically so a long outage is loud without
                // logging every attempt at error level.
                if attempt % 8 == 0 {
                    error!(
                        stream, docs = count, attempt, error = %e,
                        "flush still failing; docs held in WAL and retried until the backend recovers"
                    );
                } else {
                    warn!(stream, attempt, error = %e, "flush failed; retrying");
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Render a caught panic payload as a log-friendly message.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// Build → upload → stage → publish a batch. On error, returns the batch
/// back alongside the error so the caller can retry (the split-building
/// step is deterministic, so re-running it is safe). `Ok(None)` means the
/// whole batch was skipped as invalid — nothing to publish, but the docs
/// are processed and their WAL positions may confirm.
async fn flush_inner(
    inner: &Arc<PipelineInner>,
    stream: &str,
    stream_id: i64,
    schema: &MappedSchema,
    batch: Vec<WorkItem>,
) -> Result<Option<(String, u64)>, (IngestError, Vec<WorkItem>)> {
    let schema = schema.clone();
    let stream_name = stream.to_string();
    let work_dir = inner.config.work_dir.clone();
    let budget = inner.config.memory_budget;

    // The blocking task always hands the batch back so a failed flush can
    // be retried without having lost the documents — including when the
    // build panics (#23: the old JoinError path returned an empty batch,
    // so the retry loop spun on "refusing to build an empty split"
    // forever while the docs sat unconfirmed in the WAL).
    let (result, batch) = tokio::task::spawn_blocking(move || {
        let build = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut builder = SplitBuilder::new(stream_name, schema, &work_dir, budget)?;
            let fallback = now_datetime();
            for item in &batch {
                // Parse the source here, on the indexer thread — the queue
                // only ever held the raw bytes.
                let doc = match serde_json::from_str::<Value>(&item.source) {
                    Ok(doc) => doc,
                    Err(e) => {
                        // Enqueued docs were validated, so this is unexpected;
                        // skip the bad doc rather than fail the whole batch.
                        tracing::warn!(error = %e, "skipping unparseable buffered doc");
                        continue;
                    }
                };
                // A poison doc must cost itself, not its stream (#23):
                // doc-shaped failures — invalid-document errors and
                // conversion panics — skip the doc; backend errors still
                // fail the batch for retry. AssertUnwindSafe: on the
                // batch-failure paths the builder is discarded and every
                // retry starts from a fresh one, so a builder corrupted
                // by an unwound panic can at worst fail this attempt.
                let added = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    builder.add_document(doc, Some(&item.source), &item.identity, fallback)
                }));
                match added {
                    Ok(Ok(())) => {}
                    Ok(Err(rsearch_index::IndexError::InvalidDocument(reason))) => {
                        tracing::warn!(reason, "skipping invalid buffered doc");
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(panic) => {
                        tracing::warn!(
                            panic = panic_message(panic.as_ref()),
                            "skipping doc that panicked the indexer"
                        );
                    }
                }
            }
            if builder.doc_count() == 0 {
                // Every doc was skipped: nothing to publish, but the batch
                // is done — it must not retry (finish() on an empty
                // builder is an error) and its WAL entries must confirm.
                return Ok(None);
            }
            let indexed = builder.doc_count();
            builder.finish().map(|packaged| Some((packaged, indexed)))
        }));
        let result = match build {
            Ok(result) => result,
            Err(panic) => Err(rsearch_index::IndexError::InvalidDocument(format!(
                "indexing task panicked: {}",
                panic_message(panic.as_ref())
            ))),
        };
        (result, batch)
    })
    .await
    .map_err(|e| {
        (
            IngestError::Index(rsearch_index::IndexError::InvalidDocument(format!(
                "indexing task aborted: {e}"
            ))),
            Vec::new(),
        )
    })?;

    let (packaged, indexed) = match result {
        Ok(Some((packaged, indexed))) => (packaged, indexed),
        Ok(None) => return Ok(None),
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
    Ok(Some((packaged.meta.split_id, indexed)))
}
