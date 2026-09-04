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
use rsearch_metastore::{Metastore, StreamMode};
use rsearch_storage::Storage;

use crate::error::{IngestError, IngestResult};
use crate::wal::{Wal, WalItem, WalPos, WalReplay};

/// What the write path needs to know about a stream per request: its id
/// (tombstones are keyed by it) and its mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamInfo {
    /// The stream's metastore id.
    pub id: i64,
    /// Log or document mode.
    pub mode: StreamMode,
}

/// How long a resolved [`StreamInfo`] is reused before the metastore is
/// asked again. A mode can only change while a stream is empty, so
/// staleness is bounded and harmless; the node that changes it forgets
/// its own entry immediately.
const STREAM_INFO_TTL: Duration = Duration::from_secs(10);

/// Bits every stamp reserves for a per-node salt: `search_after` cursors
/// use `(timestamp, _seq)` as their paging position, so two nodes must
/// not be able to mint the same `_seq` in the same microsecond.
const SEQ_SALT_BITS: u32 = 10;
const SEQ_SALT_MASK: i64 = (1 << SEQ_SALT_BITS) - 1;

/// Write-sequence source: a hybrid logical clock. Stamps are micros
/// since epoch shifted left by [`SEQ_SALT_BITS`] with a node salt in the
/// low bits, forced strictly increasing across calls on one node, and
/// pushed past every sequence this node has *observed* from the cluster
/// (tombstone bounds of the ids being written, the stream's highest
/// published `_seq`) — so a replacement written on a node whose wall
/// clock lags the previous writer's still orders after it. Wall clocks
/// only set the pace; causality comes from the observations. The salt
/// (a hash of the node id) keeps stamps distinct across nodes even at
/// the same microsecond, and every stamp — including the increment
/// fallback — carries it, so cross-node collisions need a salt collision
/// too. Pre-salt stamps (plain micros) are ~1000× smaller, so all new
/// writes order after all old ones and `observe` folds them in as
/// before.
#[derive(Default)]
pub struct SeqClock {
    last: std::sync::atomic::AtomicI64,
    salt: i64,
}

impl SeqClock {
    /// A clock whose stamps carry a salt derived from `node_id` in their
    /// low bits. (Non-cryptographic hash: disambiguation, not security.)
    pub fn new(node_id: &str) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        node_id.hash(&mut hasher);
        Self {
            last: std::sync::atomic::AtomicI64::new(0),
            salt: (hasher.finish() as i64) & SEQ_SALT_MASK,
        }
    }

    /// Fold a sequence seen elsewhere into the clock: later stamps exceed it.
    pub fn observe(&self, seen: i64) {
        self.last.fetch_max(seen, Ordering::AcqRel);
    }

    /// The lowest stamp carrying this clock's salt that is strictly
    /// greater than `last` (saturating at the top of the range).
    fn stamp_after(&self, last: i64) -> i64 {
        let slot = (last >> SEQ_SALT_BITS).saturating_add(1);
        if slot > (i64::MAX >> SEQ_SALT_BITS) {
            i64::MAX
        } else {
            (slot << SEQ_SALT_BITS) | self.salt
        }
    }

    /// Next sequence stamp.
    pub fn next(&self) -> i64 {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let now = (micros.clamp(0, i64::MAX >> SEQ_SALT_BITS) << SEQ_SALT_BITS) | self.salt;
        // fetch_update: `last = max(first salted stamp past last, now)`
        // atomically.
        self.last
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                Some(now.max(self.stamp_after(last)))
            })
            .map(|prev| now.max(self.stamp_after(prev)))
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
    /// The same bound for document-mode streams.
    pub document_max_batch_secs: u64,
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

/// What a stream worker receives: documents to index, or a request to cut
/// the current batch now (`?refresh=wait_for`) and say when it's published.
enum WorkerMsg {
    Doc(WorkItem),
    Flush(tokio::sync::oneshot::Sender<()>),
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
    workers: std::sync::RwLock<HashMap<String, mpsc::Sender<WorkerMsg>>>,
    /// Single-flights first-time worker creation so the metastore resolve
    /// never runs under `workers` — enqueues to existing streams are not
    /// blocked while a new stream's row is being created.
    worker_create: tokio::sync::Mutex<()>,
    metrics: IngestMetrics,
    /// Routing rules, refreshed periodically from the metastore.
    rules: std::sync::RwLock<Arc<Vec<rsearch_metastore::RoutingRuleRecord>>>,
    /// Write-sequence source shared by every ingest entry point.
    seq: SeqClock,
    /// Stream name → (info, resolved at). Read-locked per request.
    stream_info: std::sync::RwLock<HashMap<String, (StreamInfo, std::time::Instant)>>,
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
        let seq = SeqClock::new(&config.node_id);
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
                seq,
                stream_info: std::sync::RwLock::new(HashMap::new()),
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
                    tombstone: false,
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

    /// Resolve a stream's id and mode (creating the stream, log mode, if it
    /// does not exist — the same implicit creation `_bulk` has always
    /// done), through a short TTL cache.
    pub async fn stream_info(&self, name: &str) -> IngestResult<StreamInfo> {
        if let Some((info, at)) = self.inner.stream_info.read().unwrap().get(name)
            && at.elapsed() < STREAM_INFO_TTL
        {
            return Ok(*info);
        }
        let record = self.inner.metastore.ensure_stream(name).await?;
        let info = StreamInfo {
            id: record.id,
            mode: record.mode(),
        };
        if info.mode == StreamMode::Document {
            // Order this node's next writes after everything already
            // published for the stream (cheap lower bound; the per-id
            // tombstone bounds the bulk handler observes are exact).
            if let Some(max) = self.inner.metastore.stream_max_seq(info.id).await? {
                self.inner.seq.observe(max);
            }
        }
        let mut cache = self.inner.stream_info.write().unwrap();
        // Bounded: stream names are client-controlled.
        if cache.len() > 10_000 {
            cache.clear();
        }
        cache.insert(name.to_string(), (info, std::time::Instant::now()));
        Ok(info)
    }

    /// Like [`IngestPipeline::stream_info`] but never creates the stream:
    /// `Ok(None)` when it does not exist.
    pub async fn stream_info_if_exists(&self, name: &str) -> IngestResult<Option<StreamInfo>> {
        if let Some((info, at)) = self.inner.stream_info.read().unwrap().get(name)
            && at.elapsed() < STREAM_INFO_TTL
        {
            return Ok(Some(*info));
        }
        match self.inner.metastore.get_stream(name).await {
            Ok(_) => self.stream_info(name).await.map(Some),
            Err(rsearch_metastore::MetastoreError::StreamNotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Drop a cached [`StreamInfo`] (after this node changed the stream's
    /// mode) so the next write re-reads it.
    pub fn forget_stream(&self, name: &str) {
        self.inner.stream_info.write().unwrap().remove(name);
    }

    /// Next write-sequence stamp (`_seq`) for a document accepted by this
    /// node. Take it before the WAL append so the stamp is what gets
    /// persisted and replayed.
    pub fn next_seq(&self) -> i64 {
        self.inner.seq.next()
    }

    /// Push the sequence clock past a sequence observed elsewhere (see
    /// [`SeqClock::observe`]).
    pub fn observe_seq(&self, seen: i64) {
        self.inner.seq.observe(seen);
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
        match tx.try_send(WorkerMsg::Doc(WorkItem {
            source,
            identity,
            pos,
        })) {
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
        // Records flagged as replacements re-issue their tombstone before
        // the document is re-indexed (upserts only raise, so doing it
        // twice is harmless; skipping it would resurrect the old version).
        let mut pending_tombstones: Vec<rsearch_metastore::NewTombstone> = Vec::new();
        for record in records {
            let record = record.map_err(IngestError::Wal)?;
            count += 1;
            if record.tombstone {
                let info = self.stream_info(&record.stream).await?;
                pending_tombstones.push(rsearch_metastore::NewTombstone {
                    stream_id: info.id,
                    doc_id: record.id.clone(),
                    before_seq: record.seq,
                });
                self.inner.seq.observe(record.seq);
                if pending_tombstones.len() >= 1_000 {
                    self.inner
                        .metastore
                        .upsert_tombstones(&pending_tombstones)
                        .await?;
                    pending_tombstones.clear();
                }
            }
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
                .send(WorkerMsg::Doc(WorkItem {
                    source,
                    identity: DocIdentity::new(record.id, record.seq),
                    pos: record.pos,
                }))
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
        if !pending_tombstones.is_empty() {
            self.inner
                .metastore
                .upsert_tombstones(&pending_tombstones)
                .await?;
        }
        if count > 0 {
            info!(count, "replayed WAL records into pipeline");
        }
        Ok(count)
    }

    /// Cut the stream's current batch into a split now and resolve once it
    /// is published (or immediately when nothing is buffered on this
    /// node). Backs `?refresh=true|wait_for`. Only this node's buffer is
    /// flushed — with bulk handoff the flag travels to the node that
    /// indexed the batch, which is the one whose buffer matters.
    pub async fn flush_stream(&self, stream: &str) -> IngestResult<()> {
        let tx = match self.inner.workers.read().unwrap().get(stream) {
            Some(tx) => tx.clone(),
            // No worker: nothing buffered here.
            None => return Ok(()),
        };
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        if tx.send(WorkerMsg::Flush(done_tx)).await.is_err() {
            // Worker retired between the lookup and the send; its buffer
            // was flushed on the way out.
            return Ok(());
        }
        // A dropped sender means the worker exited after flushing.
        let _ = done_rx.await;
        Ok(())
    }

    async fn worker_for(&self, stream: &str) -> IngestResult<mpsc::Sender<WorkerMsg>> {
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
            record.is_document_mode(),
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
    mut stream_id: i64,
    document_mode: bool,
    schema: MappedSchema,
    mut rx: mpsc::Receiver<WorkerMsg>,
) {
    let max_docs = inner.config.max_batch_docs.max(1);
    let age_secs = if document_mode {
        inner.config.document_max_batch_secs
    } else {
        inner.config.max_batch_secs
    };
    let max_age = Duration::from_secs(age_secs.max(1));
    let idle_exit = Duration::from_secs(WORKER_IDLE_EXIT_SECS);
    let mut buffer: Vec<WorkItem> = Vec::new();
    // Refresh waiters, answered after the next flush completes.
    let mut waiters: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
    let mut deadline = tokio::time::Instant::now() + idle_exit;
    // Re-resolved before each flush so a mapping change (PUT /{index})
    // takes effect on the next split, not only after a restart (L7).
    let mut schema = schema;
    let mut mapping_json = schema.mapping.to_json();

    loop {
        let flush_now = tokio::select! {
            msg = rx.recv() => match msg {
                Some(WorkerMsg::Doc(item)) => {
                    if buffer.is_empty() {
                        deadline = tokio::time::Instant::now() + max_age;
                    }
                    buffer.push(item);
                    buffer.len() >= max_docs
                }
                Some(WorkerMsg::Flush(done)) => {
                    if buffer.is_empty() {
                        // Nothing to cut: the waiter is satisfied now.
                        let _ = done.send(());
                        false
                    } else {
                        waiters.push(done);
                        true
                    }
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
                    while let Ok(msg) = rx.try_recv() {
                        match msg {
                            WorkerMsg::Doc(item) => buffer.push(item),
                            WorkerMsg::Flush(done) => waiters.push(done),
                        }
                    }
                    if !buffer.is_empty() {
                        flush(&inner, &stream, stream_id, &schema, &mut buffer).await;
                    }
                    for done in waiters.drain(..) {
                        let _ = done.send(());
                    }
                    info!(stream, "idle stream worker retired");
                    return;
                }
                true
            },
        };
        if flush_now {
            // Re-resolve the stream before building the split: picks up
            // mapping changes (PUT /{index}, PUT /{index}/_mapping), and a
            // stream deleted and re-created under the same name (#71) gets
            // its new id — a split published under the retired id would
            // be swept away with it. A stream that is gone is re-created,
            // the implicit creation `_bulk` always did for a first write.
            let resolved = match inner.metastore.get_stream(&stream).await {
                Ok(record) => Some(record),
                Err(rsearch_metastore::MetastoreError::StreamNotFound(_)) => {
                    inner.metastore.ensure_stream(&stream).await.ok()
                }
                Err(_) => None,
            };
            if let Some(record) = resolved {
                if record.id != stream_id {
                    info!(stream, old_id = stream_id, new_id = record.id, "stream re-created; worker rebound");
                    stream_id = record.id;
                    inner.stream_info.write().unwrap().remove(&stream);
                }
                if record.mapping != mapping_json {
                    let mapping = IndexMapping::from_json(&record.mapping).unwrap_or_default();
                    schema = MappedSchema::build(mapping);
                    mapping_json = record.mapping;
                }
            }
            flush(&inner, &stream, stream_id, &schema, &mut buffer).await;
            for done in waiters.drain(..) {
                let _ = done.send(());
            }
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
        .stage_split(&rsearch_metastore::NewSplit {
            split_id: &packaged.meta.split_id,
            stream_id,
            storage_key: &key,
            doc_count: packaged.meta.doc_count as i64,
            size_bytes: packaged.size_bytes as i64,
            time_start_millis: packaged.meta.time_start_millis,
            time_end_millis: packaged.meta.time_end_millis,
            footer_len: packaged.footer_len as i64,
            created_by: Some(&inner.config.node_id),
            seq_min: packaged.meta.seq_min,
            seq_max: packaged.meta.seq_max,
            tombstone_seq_applied: 0,
            schema_version: packaged.meta.schema_version as i32,
            dynamic_fields: packaged
                .meta
                .dynamic_fields
                .as_ref()
                .map(|f| serde_json::to_value(f).unwrap_or(serde_json::Value::Null)),
        })
        .await
    {
        return Err((IngestError::Metastore(e), batch));
    }
    if let Err(e) = inner.metastore.publish_split(&packaged.meta.split_id).await {
        return Err((IngestError::Metastore(e), batch));
    }
    Ok(Some((packaged.meta.split_id, indexed)))
}

#[cfg(test)]
mod tests {
    use super::SeqClock;

    #[test]
    fn seq_clock_is_monotonic_and_observes() {
        let clock = SeqClock::default();
        let a = clock.next();
        let b = clock.next();
        assert!(b > a);
        // Observing a sequence from the future (a peer whose clock is
        // ahead) pushes the next stamp past it.
        let far = a + 10_000_000_000;
        clock.observe(far);
        assert!(clock.next() > far);
        // Observing the past changes nothing.
        let c = clock.next();
        clock.observe(a);
        assert!(clock.next() > c);
    }

    /// Two nodes minting stamps at the same wall-clock instant must never
    /// collide: `search_after` cursors rely on `(timestamp, _seq)` being
    /// unique cluster-wide, and each node's salt rides in the low bits of
    /// every stamp — including increment-fallback stamps minted while a
    /// clock is pushed ahead of wall time by an observation.
    #[test]
    fn seq_clock_salts_keep_nodes_distinct() {
        let a = SeqClock::new("node-a");
        let b = SeqClock::new("node-b");
        let salt = |s: i64| s & super::SEQ_SALT_MASK;
        let first_a = a.next();
        let first_b = b.next();
        assert_ne!(salt(first_a), salt(first_b), "test node ids must hash apart");
        // Drive both clocks into the increment fallback from the same
        // observed stamp (a peer far in the future) — the classic
        // collision path for an unsalted hybrid clock.
        let far = first_a.max(first_b) + (10_000_000_000 << super::SEQ_SALT_BITS);
        a.observe(far);
        b.observe(far);
        let mut seen = std::collections::HashSet::new();
        let mut last_a = 0;
        let mut last_b = 0;
        for _ in 0..1000 {
            let sa = a.next();
            let sb = b.next();
            assert!(sa > last_a && sb > last_b, "stamps must stay monotonic");
            (last_a, last_b) = (sa, sb);
            assert_eq!(salt(sa), salt(first_a));
            assert_eq!(salt(sb), salt(first_b));
            assert!(seen.insert(sa) && seen.insert(sb), "cross-node stamp collision");
        }
    }
}
