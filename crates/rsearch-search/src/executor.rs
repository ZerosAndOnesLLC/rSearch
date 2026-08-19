//! Search execution: prune splits via the metastore, search splits
//! concurrently on blocking threads, merge hits and aggregations, fetch
//! `_source` only for the final page, shape the ES response.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tantivy::aggregation::AggregationLimitsGuard;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, Query};
use tantivy::schema::Value as _;
use tantivy::{DocAddress, Order, TantivyDocument};
use tokio::sync::Mutex;
use tracing::warn;

use rsearch_index::{
    ExcludeDocsQuery, ExclusionSet, IndexMapping, MappedSchema, SplitCache, SplitReader, Tombstone,
};
use rsearch_metastore::Metastore;
use rsearch_storage::Storage;

use crate::error::{SearchError, SearchResult};
use crate::query_dsl::{
    extract_time_bounds, fix_date_histogram_keys, rewrite_agg_fields, translate_query,
};

const TIMESTAMP_ALIASES: [&str; 3] = ["@timestamp", "timestamp", "_timestamp"];
/// Cap on cached open split readers (LRU), across all shards.
const READER_CACHE_CAP: usize = 256;
/// Reader-cache shards: every split touch on every query locks the cache
/// (LRU `get` mutates), so one global lock serializes the whole search
/// path. Sharding by split id keeps that contention local.
const READER_CACHE_SHARDS: usize = 8;
/// Max concurrent split searches per query.
const SPLIT_SEARCH_CONCURRENCY: usize = 16;
/// How long a resolved stream record + built schema may be reused before
/// re-checking the metastore. Bounds mapping-change staleness the same way
/// the ingest side's per-flush re-check does.
const STREAM_CACHE_TTL: Duration = Duration::from_secs(10);
/// ES-compatible default exact-count ceiling; beyond this the reported
/// total is a lower bound (`"relation": "gte"`).
const DEFAULT_TRACK_TOTAL_HITS: usize = 10_000;
/// How long a document-mode stream's tombstone list may be reused before
/// the metastore is asked for newer rows. A node that wrote a tombstone
/// invalidates its own cache immediately; other search nodes see it
/// within this window.
const TOMBSTONE_CACHE_TTL: Duration = Duration::from_secs(1);
/// Tombstone rows fetched per metastore round trip.
const TOMBSTONE_PAGE: i64 = 10_000;
/// How often a stream's cached tombstone list is rebuilt from scratch so
/// rows the control plane purged drop out of memory (incremental refreshes
/// only ever append).
const TOMBSTONE_REBUILD_EVERY: Duration = Duration::from_secs(300);
/// Hard ceiling on from+size (ES max_result_window).
const MAX_RESULT_WINDOW: usize = 10_000;
/// Max splits one query may touch. A query over more than this (an
/// unbounded time range on a long-retention stream) is rejected with a
/// clear error instead of silently materializing every split row and
/// scheduling a search on each.
const MAX_QUERY_SPLITS: usize = 10_000;

/// A document found by `_id` (see `SearchService::get_document`).
#[derive(Debug, Clone)]
pub struct FoundDocument {
    /// The document's `_seq` (reported as `_version`).
    pub version: i64,
    /// Its `_source`.
    pub source: Value,
}

/// A parsed `_search` request body.
pub struct SearchRequest {
    /// Stream (index) the search runs against.
    pub stream: String,
    /// ES query DSL clause; defaults to `match_all`.
    pub query: Value,
    /// Result offset (pagination); from+size is capped at 10k.
    pub from: usize,
    /// Page size; defaults to 10.
    pub size: usize,
    /// Timestamp sort direction; true (default) = newest first.
    pub sort_desc: bool,
    /// ES `aggs`/`aggregations` body, if present.
    pub aggs: Option<Value>,
    /// Whether hits include `_source` (`"_source": false` disables).
    pub include_source: bool,
    /// Exact-count ceiling; None = unbounded (always exact).
    pub track_total_hits: Option<usize>,
}

impl SearchRequest {
    /// Parse an ES search body for `stream`.
    pub fn parse(stream: &str, body: &Value) -> SearchResult<Self> {
        let query = body
            .get("query")
            .cloned()
            .unwrap_or_else(|| json!({"match_all": {}}));
        let from = body.get("from").and_then(Value::as_u64).unwrap_or(0) as usize;
        let size = body.get("size").and_then(Value::as_u64).unwrap_or(10) as usize;
        // Cap from+size (ES max_result_window): guards against usize
        // overflow and an oversized per-split TopDocs allocation from a
        // single request (M6).
        if size > MAX_RESULT_WINDOW || from > MAX_RESULT_WINDOW || from + size > MAX_RESULT_WINDOW {
            return Err(SearchError::BadRequest(format!(
                "from + size must be <= {MAX_RESULT_WINDOW}"
            )));
        }
        // track_total_hits: true = exact (None), false = don't count
        // beyond the page, N = exact up to N.
        let track_total_hits = match body.get("track_total_hits") {
            Some(Value::Bool(true)) | None => Some(DEFAULT_TRACK_TOTAL_HITS),
            Some(Value::Bool(false)) => Some(0),
            Some(Value::Number(n)) => n.as_u64().map(|v| v as usize),
            _ => Some(DEFAULT_TRACK_TOTAL_HITS),
        };
        // Sort: timestamp desc default; only timestamp sorts supported in v1.
        let mut sort_desc = true;
        if let Some(sorts) = body.get("sort") {
            let entries: Vec<&Value> = match sorts {
                Value::Array(items) => items.iter().collect(),
                single => vec![single],
            };
            for entry in entries {
                match entry {
                    Value::String(s) if TIMESTAMP_ALIASES.contains(&s.as_str()) => {
                        sort_desc = false;
                    }
                    Value::Object(map) => {
                        for (field, spec) in map {
                            if TIMESTAMP_ALIASES.contains(&field.as_str()) {
                                let order = spec
                                    .get("order")
                                    .and_then(Value::as_str)
                                    .unwrap_or_else(|| spec.as_str().unwrap_or("desc"));
                                sort_desc = order != "asc";
                            } else if field != "_score" && field != "_doc" {
                                warn!(field, "ignoring unsupported sort field");
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        let aggs = body
            .get("aggs")
            .or_else(|| body.get("aggregations"))
            .cloned();
        let include_source = body
            .get("_source")
            .map(|s| !matches!(s, Value::Bool(false)))
            .unwrap_or(true);
        Ok(Self {
            stream: stream.to_string(),
            query,
            from,
            size,
            sort_desc,
            aggs,
            include_source,
            track_total_hits,
        })
    }
}

/// A fetched page document: (page position, stored `_id`, `_seq`,
/// `_source` text).
type FetchedDoc = (usize, Option<String>, Option<i64>, Option<String>);

/// A hit reference — no `_source` yet; it's fetched only for the final
/// page after the global merge, and includes a stable tiebreaker.
#[derive(Clone)]
struct SplitHit {
    timestamp_millis: i64,
    split_idx: usize,
    doc: DocAddress,
}

/// A document-mode stream's tombstones as last fetched: ascending by
/// `seq`, append-only between refreshes (the list is what every split's
/// incremental `apply_tombstones` consumes).
struct StreamTombstones {
    entries: Arc<Vec<Tombstone>>,
    /// None = explicitly invalidated (a local write); refresh on next use.
    fetched_at: Option<Instant>,
    /// When the list was last loaded from scratch.
    rebuilt_at: Instant,
}

struct SplitOutcome {
    /// Exact count, or a lower bound when capped by track_total_hits.
    count: usize,
    count_is_lower_bound: bool,
    hits: Vec<SplitHit>,
    aggs: Option<IntermediateAggregationResults>,
}

/// A resolved stream: its metastore record plus the schema built from its
/// mapping, cached for [`STREAM_CACHE_TTL`].
struct CachedStream {
    record: rsearch_metastore::StreamRecord,
    schema: Arc<MappedSchema>,
}

/// Stateless search service: metastore for pruning, storage for split
/// bytes, a sharded LRU cache of open readers with single-flight opens.
pub struct SearchService {
    metastore: Metastore,
    storage: Arc<dyn Storage>,
    cache: Arc<SplitCache>,
    /// Sharded by split id; each shard is a sync mutex (never held across
    /// an await) so concurrent split touches don't serialize globally.
    readers: Vec<std::sync::Mutex<lru::LruCache<String, Arc<SplitReader>>>>,
    /// Per-split open locks so concurrent queries opening the same cold
    /// split don't each pay the open cost.
    opening: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Stream name → resolved record + built schema. Saves the per-query
    /// metastore roundtrip and full Tantivy schema rebuild for data that
    /// only changes on PUT /{index}.
    streams: std::sync::Mutex<HashMap<String, (Arc<CachedStream>, Instant)>>,
    /// Stream id → tombstone list (document-mode streams only).
    tombstones: std::sync::Mutex<HashMap<i64, StreamTombstones>>,
    /// Per-stream single-flight for tombstone refreshes, so N concurrent
    /// queries after a TTL expiry (or a cold start) page the metastore
    /// once, not N times.
    tombstone_refresh: std::sync::Mutex<HashMap<i64, Arc<Mutex<()>>>>,
}

impl SearchService {
    /// Build a search service over the given metastore, storage, and
    /// shared split cache.
    pub fn new(metastore: Metastore, storage: Arc<dyn Storage>, cache: Arc<SplitCache>) -> Self {
        Self {
            metastore,
            storage,
            cache,
            readers: (0..READER_CACHE_SHARDS)
                .map(|_| {
                    std::sync::Mutex::new(lru::LruCache::new(
                        NonZeroUsize::new(READER_CACHE_CAP / READER_CACHE_SHARDS).unwrap(),
                    ))
                })
                .collect(),
            opening: std::sync::Mutex::new(HashMap::new()),
            streams: std::sync::Mutex::new(HashMap::new()),
            tombstones: std::sync::Mutex::new(HashMap::new()),
            tombstone_refresh: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Forget a cached stream record (after this node changed its mode or
    /// mapping) so the next query re-reads it.
    pub fn invalidate_stream(&self, name: &str) {
        self.streams.lock().unwrap().remove(name);
    }

    /// Drop the freshness of a stream's cached tombstones so the next
    /// query re-reads the metastore — called by the node's own write path
    /// so a delete is visible to an immediately following local search.
    pub fn invalidate_tombstones(&self, stream_id: i64) {
        if let Some(entry) = self.tombstones.lock().unwrap().get_mut(&stream_id) {
            entry.fetched_at = None;
        }
    }

    /// The stream's tombstones, ascending by seq, extended from the
    /// metastore when the cache is stale. Steady state costs one small
    /// query per TTL (usually returning nothing, in which case the cached
    /// list is reused untouched); every `TOMBSTONE_REBUILD_EVERY` the list
    /// is reloaded from scratch so purged rows leave memory.
    async fn tombstones_for(&self, stream_id: i64) -> SearchResult<Arc<Vec<Tombstone>>> {
        let snapshot = |this: &Self| {
            this.tombstones
                .lock()
                .unwrap()
                .get(&stream_id)
                .map(|t| (t.entries.clone(), t.fetched_at, t.rebuilt_at))
        };
        let fresh = |fetched_at: Option<Instant>| {
            fetched_at.is_some_and(|at| at.elapsed() < TOMBSTONE_CACHE_TTL)
        };
        if let Some((entries, fetched_at, _)) = snapshot(self)
            && fresh(fetched_at)
        {
            return Ok(entries);
        }
        // Single-flight per stream.
        let gate = self
            .tombstone_refresh
            .lock()
            .unwrap()
            .entry(stream_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = gate.lock().await;
        let cached = snapshot(self);
        if let Some((entries, fetched_at, _)) = &cached
            && fresh(*fetched_at)
        {
            return Ok(entries.clone());
        }
        let rebuild = cached
            .as_ref()
            .is_none_or(|(_, _, rebuilt_at)| rebuilt_at.elapsed() >= TOMBSTONE_REBUILD_EVERY);
        let (mut entries, mut after, rebuilt_at) = match (&cached, rebuild) {
            (Some((entries, _, rebuilt_at)), false) => {
                (None, entries.last().map(|t| t.seq).unwrap_or(0), *rebuilt_at)
            }
            _ => (Some(Vec::new()), 0, Instant::now()),
        };
        loop {
            let page = self
                .metastore
                .tombstones_since(stream_id, after, TOMBSTONE_PAGE)
                .await?;
            let done = (page.len() as i64) < TOMBSTONE_PAGE;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|t| t.seq).unwrap_or(after);
            // Clone the cached list only once something new arrived.
            let list = entries.get_or_insert_with(|| {
                cached
                    .as_ref()
                    .map(|(e, _, _)| e.as_ref().clone())
                    .unwrap_or_default()
            });
            list.extend(page.into_iter().map(|t| Tombstone {
                seq: t.seq,
                doc_id: t.doc_id,
                before_seq: t.before_seq,
            }));
            if done {
                break;
            }
        }
        let entries = match entries {
            Some(list) => Arc::new(list),
            // Nothing new: keep the existing Arc.
            None => cached.as_ref().map(|(e, _, _)| e.clone()).unwrap_or_default(),
        };
        let mut map = self.tombstones.lock().unwrap();
        if map.len() > 10_000 {
            map.clear();
        }
        map.insert(
            stream_id,
            StreamTombstones {
                entries: entries.clone(),
                fetched_at: Some(Instant::now()),
                rebuilt_at,
            },
        );
        Ok(entries)
    }

    fn reader_shard(&self, split_id: &str) -> &std::sync::Mutex<lru::LruCache<String, Arc<SplitReader>>> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        split_id.hash(&mut hasher);
        &self.readers[hasher.finish() as usize % READER_CACHE_SHARDS]
    }

    /// Resolve a stream's record and schema through the TTL cache.
    async fn stream_schema(&self, name: &str) -> SearchResult<Arc<CachedStream>> {
        if let Some((cached, at)) = self.streams.lock().unwrap().get(name)
            && at.elapsed() < STREAM_CACHE_TTL
        {
            return Ok(cached.clone());
        }
        let record = self.metastore.get_stream(name).await?;
        let mapping = IndexMapping::from_json(&record.mapping)
            .map_err(|e| SearchError::BadRequest(e.to_string()))?;
        let cached = Arc::new(CachedStream {
            schema: Arc::new(MappedSchema::build(mapping)),
            record,
        });
        let mut streams = self.streams.lock().unwrap();
        // Bounded so a probe flood of stream names can't grow it forever.
        if streams.len() > 10_000 {
            streams.clear();
        }
        streams.insert(name.to_string(), (cached.clone(), Instant::now()));
        Ok(cached)
    }

    /// Open (or reuse) a split's reader. `applied_through` is the split's
    /// `tombstone_seq_applied`: a fresh reader starts there, so tombstones
    /// compaction already made physical are never looked up again.
    async fn reader(
        &self,
        split_id: &str,
        storage_key: &str,
        applied_through: i64,
    ) -> SearchResult<Arc<SplitReader>> {
        if let Some(reader) = self.reader_shard(split_id).lock().unwrap().get(split_id) {
            return Ok(reader.clone());
        }
        // Single-flight: coalesce concurrent opens of the same split.
        let gate = {
            let mut opening = self.opening.lock().unwrap();
            opening
                .entry(split_id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = gate.lock().await;
        // Someone may have opened it while we waited for the gate.
        if let Some(reader) = self.reader_shard(split_id).lock().unwrap().get(split_id) {
            return Ok(reader.clone());
        }
        // The gate entry must not outlive the attempt: a failed open would
        // otherwise leak it forever (one map entry per failing split id).
        let opened = SplitReader::open(self.storage.clone(), storage_key, self.cache.clone()).await;
        let reader = match opened {
            Ok(reader) => {
                reader.seed_applied_through(applied_through);
                Arc::new(reader)
            }
            Err(e) => {
                self.opening.lock().unwrap().remove(split_id);
                return Err(e.into());
            }
        };
        // LRU insert evicts only the least-recently-used entry, not the
        // whole cache.
        self.reader_shard(split_id)
            .lock()
            .unwrap()
            .put(split_id.to_string(), reader.clone());
        self.opening.lock().unwrap().remove(split_id);
        Ok(reader)
    }

    /// Look a document up by `_id`: the newest live version (tombstones
    /// applied), or None. Reads published splits only — a write still in
    /// an ingest buffer is not visible yet (use `?refresh=wait_for` on the
    /// write when a read-after-write must see it).
    pub async fn get_document(&self, stream: &str, id: &str) -> SearchResult<Option<FoundDocument>> {
        let mut found = self.get_documents(stream, std::slice::from_ref(&id.to_string())).await?;
        Ok(found.remove(id))
    }

    /// Look up many documents by `_id` in a few searches (one `ids` query
    /// per chunk of ids) — the newest live version of each id found.
    pub async fn get_documents(
        &self,
        stream: &str,
        ids: &[String],
    ) -> SearchResult<HashMap<String, FoundDocument>> {
        // Several live versions of one id can only coexist transiently
        // (same-instant writes on different nodes); the newest wins. The
        // page leaves headroom for those duplicates within the result
        // window.
        const CHUNK: usize = 1_000;
        let mut out: HashMap<String, FoundDocument> = HashMap::new();
        for chunk in ids.chunks(CHUNK) {
            let request = SearchRequest {
                stream: stream.to_string(),
                query: json!({"ids": {"values": chunk}}),
                from: 0,
                size: MAX_RESULT_WINDOW,
                sort_desc: true,
                aggs: None,
                include_source: true,
                track_total_hits: Some(0),
            };
            let response = self.search(request).await?;
            for hit in response["hits"]["hits"].as_array().into_iter().flatten() {
                let Some(id) = hit["_id"].as_str() else { continue };
                let version = hit["_version"].as_i64().unwrap_or(0);
                let newer = out.get(id).is_none_or(|cur| version > cur.version);
                if newer {
                    out.insert(
                        id.to_string(),
                        FoundDocument {
                            version,
                            source: hit["_source"].clone(),
                        },
                    );
                }
            }
        }
        Ok(out)
    }

    /// Execute a search and return the full ES-shaped response body.
    pub async fn search(&self, request: SearchRequest) -> SearchResult<Value> {
        use futures::stream::{self, StreamExt};

        let started = Instant::now();
        let cached = self.stream_schema(&request.stream).await?;
        let stream = &cached.record;
        let schema = cached.schema.clone();

        let (t_start, t_end) = extract_time_bounds(&request.query);
        let splits = self
            .metastore
            .splits_for_query(stream.id, t_start, t_end, MAX_QUERY_SPLITS as i64 + 1)
            .await?;
        if splits.len() > MAX_QUERY_SPLITS {
            return Err(SearchError::BadRequest(format!(
                "query spans more than {MAX_QUERY_SPLITS} splits; narrow the time range"
            )));
        }

        // Rewrite/parse aggregations once, share across splits via Arc.
        let aggs_json = request
            .aggs
            .as_ref()
            .map(|aggs| rewrite_agg_fields(&schema, aggs));
        let aggregations: Option<Aggregations> = match &aggs_json {
            Some(json) => Some(
                serde_json::from_value(json.clone())
                    .map_err(|e| SearchError::BadRequest(format!("invalid aggregations: {e}")))?,
            ),
            None => None,
        };

        // Document-mode streams hide tombstoned versions; the list is
        // shared by every split's incremental application below.
        let tombstones: Option<Arc<Vec<Tombstone>>> = if stream.is_document_mode() {
            let list = self.tombstones_for(stream.id).await?;
            (!list.is_empty()).then_some(list)
        } else {
            None
        };

        let fetch_limit = request.from + request.size;
        let query = Arc::new(request.query.clone());
        // Whether the query is exactly match_all — lets fully-covered
        // splits report their doc_count instead of running Count over the
        // whole corpus (H3).
        let is_match_all = query
            .as_object()
            .and_then(|o| o.keys().next().map(|k| k == "match_all"))
            .unwrap_or(false);

        // Open readers and search all splits concurrently (bounded), in
        // split order so split_idx stays meaningful.
        let track = request.track_total_hits;
        let sort_desc = request.sort_desc;
        // Running total of counted matches across splits. Once it passes
        // track_total_hits, later splits skip their Count collector and the
        // response reports `"relation": "gte"` — the default 10k cap stops
        // exact counting instead of scanning every match in the stream.
        let counted = Arc::new(AtomicUsize::new(0));
        let futs: Vec<_> = splits
            .iter()
            .enumerate()
            .map(|(idx, split)| {
                let this = &*self;
                let query = query.clone();
                let aggregations = aggregations.clone();
                let counted = counted.clone();
                let split_id = split.split_id.clone();
                let storage_key = split.storage_key.clone();
                let applied_through = split.tombstone_seq_applied;
                let doc_count = split.doc_count as usize;
                // Only splits that carry ids (seq_min known) can hold a
                // tombstoned version.
                let tombstones = tombstones
                    .clone()
                    .filter(|_| split.seq_min.is_some());
                // A split fully inside [t_start, t_end] needs no filtering
                // for a match_all query — its whole doc_count matches, so it
                // reports its count without scanning (H3).
                let fully_covered = is_match_all
                    && t_start.map(|s| split.time_start_millis >= s).unwrap_or(true)
                    && t_end.map(|e| split.time_end_millis <= e).unwrap_or(true);
                async move {
                    let reader = this.reader(&split_id, &storage_key, applied_through).await?;
                    let skip_count = match track {
                        Some(cap) => counted.load(Ordering::Relaxed) >= cap,
                        None => false,
                    };
                    let outcome = tokio::task::spawn_blocking(move || {
                        let exclusions = match tombstones {
                            Some(list) => Some(reader.apply_tombstones(&list)?),
                            None => None,
                        }
                        .filter(|set| !set.is_empty());
                        search_one_split(
                            &reader,
                            &query,
                            aggregations,
                            fetch_limit,
                            sort_desc,
                            skip_count,
                            idx,
                            doc_count,
                            fully_covered,
                            exclusions,
                        )
                    })
                    .await
                    .map_err(|e| SearchError::Internal(format!("search task panicked: {e}")))??;
                    counted.fetch_add(outcome.count, Ordering::Relaxed);
                    Ok(outcome)
                }
            })
            .collect();
        // buffered(N): at most N splits open/search at once, results in
        // order.
        let outcomes: Vec<SplitOutcome> = stream::iter(futs)
            .buffered(SPLIT_SEARCH_CONCURRENCY)
            .collect::<Vec<SearchResult<SplitOutcome>>>()
            .await
            .into_iter()
            .collect::<SearchResult<Vec<_>>>()?;
        let mut outcomes = outcomes;

        // Merge: global count (with relation), stable top-k, agg fuse.
        let mut total: usize = 0;
        let mut total_is_lower_bound = false;
        for o in &outcomes {
            total += o.count;
            total_is_lower_bound |= o.count_is_lower_bound;
        }
        let mut hits: Vec<SplitHit> = outcomes
            .iter_mut()
            .flat_map(|o| o.hits.drain(..))
            .collect();
        // Stable order: timestamp, then split index, then doc — so equal
        // timestamps page deterministically (L8).
        let cmp = |a: &SplitHit, b: &SplitHit| {
            let ts = if request.sort_desc {
                b.timestamp_millis.cmp(&a.timestamp_millis)
            } else {
                a.timestamp_millis.cmp(&b.timestamp_millis)
            };
            ts.then(a.split_idx.cmp(&b.split_idx))
                .then(a.doc.segment_ord.cmp(&b.doc.segment_ord))
                .then(a.doc.doc_id.cmp(&b.doc.doc_id))
        };
        hits.sort_by(cmp);
        let page: Vec<SplitHit> = hits
            .into_iter()
            .skip(request.from)
            .take(request.size)
            .collect();

        // Fetch the page's documents (`_id`, and `_source` unless disabled)
        // only for the final page, grouped by split so each reader is used
        // once (H4).
        let page_entries = self
            .fetch_page_sources(&splits, &page, &request.stream, request.include_source)
            .await?;

        let merged_aggs = match (&aggregations, &aggs_json) {
            (Some(aggs), Some(_)) => {
                let mut fused: Option<IntermediateAggregationResults> = None;
                for outcome in outcomes {
                    if let Some(intermediate) = outcome.aggs {
                        match fused.as_mut() {
                            Some(acc) => acc
                                .merge_fruits(intermediate)
                                .map_err(|e| SearchError::Internal(e.to_string()))?,
                            None => fused = Some(intermediate),
                        }
                    }
                }
                let final_result: Option<AggregationResults> = match fused {
                    Some(fused) => Some(
                        fused
                            .into_final_result(aggs.clone(), default_limits())
                            .map_err(|e| SearchError::Internal(e.to_string()))?,
                    ),
                    None => None,
                };
                final_result.map(|r| {
                    let mut value = serde_json::to_value(r).unwrap_or(Value::Null);
                    if let Some(request) = &aggs_json {
                        fix_date_histogram_keys(request, &mut value);
                    }
                    value
                })
            }
            _ => None,
        };

        let relation = if total_is_lower_bound { "gte" } else { "eq" };
        let mut response = json!({
            "took": started.elapsed().as_millis() as u64,
            "timed_out": false,
            "_shards": {
                "total": splits.len(),
                "successful": splits.len(),
                "skipped": 0,
                "failed": 0,
            },
            "hits": {
                "total": {"value": total, "relation": relation},
                "max_score": Value::Null,
                "hits": page_entries,
            },
        });
        if let Some(aggs) = merged_aggs {
            response["aggregations"] = aggs;
        }
        Ok(response)
    }

    /// Fetch the stored documents for the final page only, grouping by
    /// split so each reader is used once on a single blocking task. Yields
    /// each hit's `_id` (the stored one, or the synthetic
    /// `split:segment:doc` address for legacy splits without ids) and its
    /// `_source` when `include_source`.
    async fn fetch_page_sources(
        &self,
        splits: &[rsearch_metastore::SplitRecord],
        page: &[SplitHit],
        stream: &str,
        include_source: bool,
    ) -> SearchResult<Vec<Value>> {
        use futures::stream::{self, StreamExt};

        // Group page positions by split, then fetch the splits concurrently
        // (bounded); positions restore the page order afterwards.
        let mut by_split: HashMap<usize, Vec<(usize, DocAddress)>> = HashMap::new();
        for (pos, hit) in page.iter().enumerate() {
            by_split.entry(hit.split_idx).or_default().push((pos, hit.doc));
        }
        let futs: Vec<_> = by_split
            .into_iter()
            .map(|(split_idx, wants)| {
                let this = &*self;
                let split = &splits[split_idx];
                async move {
                    let reader = this
                        .reader(&split.split_id, &split.storage_key, split.tombstone_seq_applied)
                        .await?;
                    tokio::task::spawn_blocking(move || {
                        let searcher = reader.searcher()?;
                        let schema = reader.mapped_schema();
                        let mut out = Vec::with_capacity(wants.len());
                        for (pos, address) in wants {
                            let doc: TantivyDocument =
                                searcher.doc(address).map_err(SearchError::Tantivy)?;
                            let id = schema.id.and_then(|f| {
                                doc.get_first(f).and_then(|v| v.as_str().map(str::to_string))
                            });
                            let seq = match schema.seq {
                                Some(_) => searcher
                                    .segment_reader(address.segment_ord)
                                    .fast_fields()
                                    .i64(rsearch_index::SEQ_FIELD)
                                    .ok()
                                    .and_then(|col| col.first(address.doc_id)),
                                None => None,
                            };
                            let source = include_source
                                .then(|| {
                                    doc.get_first(schema.source)
                                        .and_then(|v| v.as_str().map(str::to_string))
                                })
                                .flatten();
                            out.push((pos, id, seq, source));
                        }
                        Ok::<_, SearchError>(out)
                    })
                    .await
                    .map_err(|e| SearchError::Internal(format!("source fetch panicked: {e}")))?
                }
            })
            .collect();
        let mut fetched_docs: Vec<(Option<String>, Option<i64>, Option<String>)> =
            vec![(None, None, None); page.len()];
        let fetched: Vec<Vec<FetchedDoc>> = stream::iter(futs)
            .buffer_unordered(SPLIT_SEARCH_CONCURRENCY)
            .collect::<Vec<SearchResult<_>>>()
            .await
            .into_iter()
            .collect::<SearchResult<Vec<_>>>()?;
        for (pos, id, seq, source) in fetched.into_iter().flatten() {
            fetched_docs[pos] = (id, seq, source);
        }
        Ok(page
            .iter()
            .zip(fetched_docs)
            .map(|(hit, (id, seq, source))| {
                let source = include_source.then(|| {
                    source
                        .as_deref()
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(Value::Null)
                });
                hit_envelope(hit, splits, stream, id, seq, source)
            })
            .collect())
    }
}

/// Build the ES hit envelope. `id` is the stored `_id` when the split has
/// one (legacy splits fall back to the synthetic split:segment:doc
/// address); `source` is Some(value) when `_source` is requested (value
/// may be Null if unfetchable), None to omit the field.
fn hit_envelope(
    hit: &SplitHit,
    splits: &[rsearch_metastore::SplitRecord],
    stream: &str,
    id: Option<String>,
    seq: Option<i64>,
    source: Option<Value>,
) -> Value {
    let split_id = &splits[hit.split_idx].split_id;
    let id = id.unwrap_or_else(|| {
        format!("{}:{}:{}", split_id, hit.doc.segment_ord, hit.doc.doc_id)
    });
    let mut entry = json!({
        "_index": stream,
        "_id": id,
        "_score": Value::Null,
        "sort": [hit.timestamp_millis],
    });
    // `_version` is the write sequence: what the bulk/_doc responses
    // reported for this version of the document.
    if let Some(seq) = seq {
        entry["_version"] = json!(seq);
    }
    if let Some(source) = source {
        entry["_source"] = source;
    }
    entry
}

fn default_limits() -> AggregationLimitsGuard {
    // 500MB aggregation memory ceiling, 65k buckets — ES-like defaults.
    AggregationLimitsGuard::new(Some(500 << 20), Some(65_000))
}

#[allow(clippy::too_many_arguments)]
fn search_one_split(
    reader: &SplitReader,
    query_json: &Value,
    aggregations: Option<Aggregations>,
    fetch_limit: usize,
    sort_desc: bool,
    skip_count: bool,
    split_idx: usize,
    doc_count: usize,
    fully_covered: bool,
    exclusions: Option<Arc<ExclusionSet>>,
) -> SearchResult<SplitOutcome> {
    let index = reader.index();
    // Translate against the split's own schema: field ordinals follow the
    // mapping the split was built with, which can differ from the stream's
    // current mapping (PUT /{index} after the split was written).
    let mut query = translate_query(index, reader.mapped_schema(), query_json)?;
    // Tombstoned versions are excluded inside the query so every collector
    // (top-k, Count, aggregations) agrees; the doc_count shortcut below
    // subtracts them instead of scanning.
    let mut doc_count = doc_count;
    if let Some(set) = exclusions {
        doc_count = doc_count.saturating_sub(set.len());
        query = Box::new(BooleanQuery::new(vec![
            (Occur::Must, query),
            (Occur::MustNot, Box::new(ExcludeDocsQuery::new(set)) as Box<dyn Query>),
        ]));
    }
    let searcher = reader.searcher()?;

    let order = if sort_desc { Order::Desc } else { Order::Asc };
    let top_collector = TopDocs::with_limit(fetch_limit.max(1))
        .order_by_fast_field::<tantivy::DateTime>("_timestamp", order);

    // match_all over a fully-covered split: the count is the split's
    // doc_count; skip the Count collector entirely (H3). Still runs the
    // top-k collector for the page.
    if fully_covered && aggregations.is_none() {
        let top = searcher
            .search(&query, &top_collector)
            .map_err(SearchError::Tantivy)?;
        let hits = top
            .into_iter()
            .map(|(timestamp, doc)| SplitHit {
                timestamp_millis: timestamp
                    .map(|t| t.into_timestamp_millis())
                    .unwrap_or_default(),
                split_idx,
                doc,
            })
            .collect();
        return Ok(SplitOutcome {
            count: doc_count,
            count_is_lower_bound: false,
            hits,
            aggs: None,
        });
    }

    // skip_count (track_total_hits:false, or the running total already
    // passed the cap) → don't run the Count collector at all; the total
    // becomes a lower bound (the page length). Aggregations still need
    // their collector, and counting is free alongside their full scan.
    let (count, count_is_lower_bound, top, agg_result) = match (aggregations, skip_count) {
        (Some(aggs), _) => {
            let agg_collector = tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                aggs,
                tantivy::aggregation::AggContextParams::new(
                    default_limits(),
                    index.tokenizers().clone(),
                ),
            );
            let (count, top, aggs) = searcher
                .search(&query, &(Count, top_collector, agg_collector))
                .map_err(SearchError::Tantivy)?;
            (count, false, top, Some(aggs))
        }
        (None, true) => {
            let top = searcher
                .search(&query, &top_collector)
                .map_err(SearchError::Tantivy)?;
            // Lower bound: at least the number of hits we returned.
            (top.len(), true, top, None)
        }
        (None, false) => {
            let (count, top) = searcher
                .search(&query, &(Count, top_collector))
                .map_err(SearchError::Tantivy)?;
            (count, false, top, None)
        }
    };

    // Source is fetched later, only for the merged final page — here we
    // just record references (H4).
    let hits = top
        .into_iter()
        .map(|(timestamp, doc)| SplitHit {
            timestamp_millis: timestamp
                .map(|t| t.into_timestamp_millis())
                .unwrap_or_default(),
            split_idx,
            doc,
        })
        .collect();
    Ok(SplitOutcome {
        count,
        count_is_lower_bound,
        hits,
        aggs: agg_result,
    })
}
