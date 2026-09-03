//! Search execution: prune splits via the metastore, search splits
//! concurrently on blocking threads, merge hits and aggregations, fetch
//! `_source` only for the final page, shape the ES response.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tantivy::aggregation::AggregationLimitsGuard;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, Query, RangeQuery};
use tantivy::schema::{Term, Value as _};
use tantivy::{DocAddress, TantivyDocument};
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

/// A `search_after` cursor: the `sort` values of the previous page's
/// last hit, i.e. `[timestamp_millis, _seq]` (issue #52). Paging resumes
/// strictly past this position in (timestamp, `_seq`) order.
#[derive(Clone, Copy, Debug)]
pub struct SearchAfter {
    /// Timestamp sort value, epoch millis.
    pub timestamp_millis: i64,
    /// `_seq` tiebreak. `None` when the caller passed a bare `[ts]`
    /// cursor — paging is then strictly by timestamp, so equal-timestamp
    /// documents at the page boundary are skipped. `-1` is the sentinel
    /// legacy (pre-`_seq`) hits report in their `sort` values.
    pub seq: Option<i64>,
}

/// A parsed `_search` request body.
#[derive(Debug)]
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
    /// Resume paging strictly past this cursor (requires `from` = 0).
    /// Unlike `from`/`size`, each page costs only `size` per split, so it
    /// pages past `max_result_window` (issue #52). Totals and
    /// aggregations still reflect the full query, as in ES.
    pub search_after: Option<SearchAfter>,
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
        // search_after: the previous page's last `sort` values. The
        // timestamp is taken verbatim as epoch millis (it is what our own
        // `sort` emitted — no unit heuristic, which would rescale small
        // values), clamped to the tantivy-safe range like every other
        // timestamp input so a hostile cursor can't overflow the nanos
        // conversion.
        let search_after = match body.get("search_after") {
            None | Some(Value::Null) => None,
            Some(Value::Array(vals)) if (1..=2).contains(&vals.len()) => {
                // Whole-valued floats are accepted: JSON round-trips in
                // some clients re-encode the echoed integer as a float.
                let as_i64 = |v: &Value| {
                    v.as_i64()
                        .or_else(|| {
                            v.as_f64()
                                .filter(|f| f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0)
                                .map(|f| f as i64)
                        })
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                };
                let timestamp_millis = as_i64(&vals[0])
                    .ok_or_else(|| {
                        SearchError::BadRequest(
                            "search_after[0] must be the timestamp sort value (epoch millis)"
                                .into(),
                        )
                    })?
                    .clamp(-rsearch_index::MAX_SAFE_MILLIS, rsearch_index::MAX_SAFE_MILLIS);
                let seq = vals
                    .get(1)
                    .map(|v| {
                        as_i64(v).ok_or_else(|| {
                            SearchError::BadRequest(
                                "search_after[1] must be the _seq sort value (integer)".into(),
                            )
                        })
                    })
                    .transpose()?;
                Some(SearchAfter { timestamp_millis, seq })
            }
            Some(_) => {
                return Err(SearchError::BadRequest(
                    "search_after must be an array of 1-2 sort values ([timestamp, _seq])".into(),
                ));
            }
        };
        if search_after.is_some() && from != 0 {
            return Err(SearchError::BadRequest(
                "[from] parameter must be set to 0 when [search_after] is used".into(),
            ));
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
            search_after,
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
    /// `_seq` tiebreak (None on legacy splits without the column). Part
    /// of the global sort order so `search_after` cursors page
    /// deterministically across splits (issue #52).
    seq: Option<i64>,
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
                search_after: None,
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
        let cursor = request.search_after;
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
                // A split lying entirely on the already-paged side of the
                // cursor cannot contribute page hits — its top-k pass is
                // skipped, and when the count is skippable too the split
                // is not even opened. (Strict compare: a split touching
                // the boundary timestamp may still hold page docs.)
                let outside_cursor = match cursor {
                    Some(c) if sort_desc => split.time_start_millis > c.timestamp_millis,
                    Some(c) => split.time_end_millis < c.timestamp_millis,
                    None => false,
                };
                async move {
                    let skip_count = match track {
                        Some(cap) => counted.load(Ordering::Relaxed) >= cap,
                        None => false,
                    };
                    if outside_cursor && skip_count && aggregations.is_none() {
                        return Ok(SplitOutcome {
                            count: 0,
                            count_is_lower_bound: true,
                            hits: Vec::new(),
                            aggs: None,
                        });
                    }
                    let reader = this.reader(&split_id, &storage_key, applied_through).await?;
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
                            cursor,
                            outside_cursor,
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
        // Stable order: timestamp, then `_seq` (the search_after tiebreak
        // — legacy docs without one sort as -1), then split index and doc
        // for determinism — so equal timestamps page deterministically
        // (L8) and cursors resume at the same global position (issue #52).
        let cmp = |a: &SplitHit, b: &SplitHit| {
            let (ts, seq) = if request.sort_desc {
                (
                    b.timestamp_millis.cmp(&a.timestamp_millis),
                    b.seq.unwrap_or(-1).cmp(&a.seq.unwrap_or(-1)),
                )
            } else {
                (
                    a.timestamp_millis.cmp(&b.timestamp_millis),
                    a.seq.unwrap_or(-1).cmp(&b.seq.unwrap_or(-1)),
                )
            };
            ts.then(seq)
                .then(a.split_idx.cmp(&b.split_idx))
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
    // `sort` is the search_after cursor: [timestamp, _seq]. Legacy docs
    // without a `_seq` report the -1 sentinel (issue #52).
    let mut entry = json!({
        "_index": stream,
        "_id": id,
        "_score": Value::Null,
        "sort": [hit.timestamp_millis, seq.unwrap_or(-1)],
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

/// The query clause keeping only documents strictly past a `search_after`
/// cursor in (timestamp, `_seq`) order — every leg is a fast-field range
/// scan (issue #52). Built per split: schemas differ, and a legacy split
/// has no `_seq` column. Legacy docs sort as seq -1 — after every real
/// doc at the same timestamp in desc order, before them in asc — so a
/// desc cursor with a real `_seq` must still include the boundary
/// timestamp on a legacy split; every other legacy case pages strictly by
/// timestamp, skipping equal-timestamp boundary docs (merging old splits
/// to the current format restores exact paging).
fn cursor_clause(schema: &MappedSchema, cursor: SearchAfter, sort_desc: bool) -> Box<dyn Query> {
    let ts_term = |ms: i64| {
        Term::from_field_date(schema.timestamp, tantivy::DateTime::from_timestamp_millis(ms))
    };
    let past_ts: Box<dyn Query> = Box::new(if sort_desc {
        RangeQuery::new(Bound::Unbounded, Bound::Excluded(ts_term(cursor.timestamp_millis)))
    } else {
        RangeQuery::new(Bound::Excluded(ts_term(cursor.timestamp_millis)), Bound::Unbounded)
    });
    let Some(cursor_seq) = cursor.seq else {
        // Bare [ts] cursor: strictly past the timestamp.
        return past_ts;
    };
    match schema.seq {
        Some(seq_field) => {
            let seq_term = |s: i64| Term::from_field_i64(seq_field, s);
            let past_seq: Box<dyn Query> = Box::new(if sort_desc {
                RangeQuery::new(Bound::Unbounded, Bound::Excluded(seq_term(cursor_seq)))
            } else {
                RangeQuery::new(Bound::Excluded(seq_term(cursor_seq)), Bound::Unbounded)
            });
            let at_ts: Box<dyn Query> = Box::new(RangeQuery::new(
                Bound::Included(ts_term(cursor.timestamp_millis)),
                Bound::Included(ts_term(cursor.timestamp_millis)),
            ));
            let tie: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                (Occur::Must, at_ts),
                (Occur::Must, past_seq),
            ]));
            Box::new(BooleanQuery::new(vec![
                (Occur::Should, past_ts),
                (Occur::Should, tie),
            ]))
        }
        None if sort_desc && cursor_seq >= 0 => Box::new(RangeQuery::new(
            Bound::Unbounded,
            Bound::Included(ts_term(cursor.timestamp_millis)),
        )),
        None => past_ts,
    }
}

/// Per-split top-k collector ordering by the same key the global merge
/// uses — (timestamp, `_seq`), direction-normalized (asc negates both
/// legs) so the collector's greatest-first heap yields the requested
/// direction, ties broken by ascending doc id exactly like the merge.
/// Truncating per split by any other order hands the merge the wrong end
/// of an equal-timestamp group: with timestamp-only ordering, docs
/// beyond the page size in such a group became unreachable through
/// `search_after` and duplicated under `from`/`size` (review finding).
/// Legacy splits (no `_seq` column) yield the -1 sentinel.
fn top_sort_key_collector(
    limit: usize,
    sort_desc: bool,
) -> impl tantivy::collector::Collector<Fruit = Vec<((i64, i64), DocAddress)>> {
    TopDocs::with_limit(limit.max(1)).order_by(move |segment: &tantivy::SegmentReader| {
        let ts_col = segment.fast_fields().date("_timestamp").ok();
        let seq_col = segment.fast_fields().i64(rsearch_index::SEQ_FIELD).ok();
        move |doc: tantivy::DocId| {
            let ts = ts_col
                .as_ref()
                .and_then(|col| col.first(doc))
                .map(|t| t.into_timestamp_millis())
                .unwrap_or_default();
            let seq = seq_col.as_ref().and_then(|col| col.first(doc)).unwrap_or(-1);
            if sort_desc { (ts, seq) } else { (ts.saturating_neg(), seq.saturating_neg()) }
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn search_one_split(
    reader: &SplitReader,
    query_json: &Value,
    aggregations: Option<Aggregations>,
    fetch_limit: usize,
    sort_desc: bool,
    cursor: Option<SearchAfter>,
    page_outside_cursor: bool,
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
    let mut query = translate_query(index, reader.mapped_schema(), query_json, &|| {
        Ok(reader.dynamic_string_paths()?.to_vec())
    })?;
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

    let top_collector = top_sort_key_collector(fetch_limit, sort_desc);

    // Resolve the collector's ((ts, seq), doc) pairs into hits,
    // un-normalizing the asc negation; -1 marks a doc without a `_seq`
    // (legacy split).
    let make_hits = |top: Vec<((i64, i64), DocAddress)>| -> Vec<SplitHit> {
        top.into_iter()
            .map(|((ts, seq), doc)| {
                let (ts, seq) =
                    if sort_desc { (ts, seq) } else { (ts.saturating_neg(), seq.saturating_neg()) };
                SplitHit {
                    timestamp_millis: ts,
                    seq: (seq >= 0).then_some(seq),
                    split_idx,
                    doc,
                }
            })
            .collect()
    };
    // The cursor narrows only the page's top-k query — totals and
    // aggregations keep reflecting the full query, as in ES. A split
    // wholly on the paged side of the cursor skips the pass entirely.
    // Builds its own collector so the combined-collector arms below can
    // consume the shared one.
    let page_search = |base: Box<dyn Query>| -> SearchResult<Vec<((i64, i64), DocAddress)>> {
        if page_outside_cursor {
            return Ok(Vec::new());
        }
        let collector = top_sort_key_collector(fetch_limit, sort_desc);
        let page_query: Box<dyn Query> = match cursor {
            Some(c) => Box::new(BooleanQuery::new(vec![
                (Occur::Must, base),
                (Occur::Must, cursor_clause(reader.mapped_schema(), c, sort_desc)),
            ])),
            None => base,
        };
        searcher
            .search(&page_query, &collector)
            .map_err(SearchError::Tantivy)
    };

    // match_all over a fully-covered split: the count is the split's
    // doc_count; skip the Count collector entirely (H3). Still runs the
    // top-k collector for the page.
    if fully_covered && aggregations.is_none() {
        let hits = make_hits(page_search(query)?);
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
    // With a search_after cursor the page runs as its own search, so the
    // count/agg pass over the full query stays separate.
    let (count, count_is_lower_bound, top, agg_result) = match (aggregations, skip_count, cursor) {
        (Some(aggs), _, cursor) => {
            let agg_collector = tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                aggs,
                tantivy::aggregation::AggContextParams::new(
                    default_limits(),
                    index.tokenizers().clone(),
                ),
            );
            match cursor {
                None => {
                    let (count, top, aggs) = searcher
                        .search(&query, &(Count, top_collector, agg_collector))
                        .map_err(SearchError::Tantivy)?;
                    (count, false, top, Some(aggs))
                }
                Some(_) => {
                    let (count, aggs) = searcher
                        .search(&query, &(Count, agg_collector))
                        .map_err(SearchError::Tantivy)?;
                    let top = page_search(query)?;
                    (count, false, top, Some(aggs))
                }
            }
        }
        (None, true, _) => {
            let top = page_search(query)?;
            // Lower bound: at least the number of hits we returned.
            (top.len(), true, top, None)
        }
        (None, false, None) => {
            let (count, top) = searcher
                .search(&query, &(Count, top_collector))
                .map_err(SearchError::Tantivy)?;
            (count, false, top, None)
        }
        (None, false, Some(_)) => {
            let count = searcher.search(&query, &Count).map_err(SearchError::Tantivy)?;
            let top = page_search(query)?;
            (count, false, top, None)
        }
    };

    // Source is fetched later, only for the merged final page — here we
    // just record references (H4).
    let hits = make_hits(top);
    Ok(SplitOutcome {
        count,
        count_is_lower_bound,
        hits,
        aggs: agg_result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsearch_index::{DocIdentity, DocumentConverter, IndexMapping};

    #[test]
    fn parses_search_after() {
        let parsed = SearchRequest::parse(
            "logs",
            &json!({"size": 5, "search_after": [2000, 5]}),
        )
        .unwrap();
        let cursor = parsed.search_after.unwrap();
        assert_eq!(cursor.timestamp_millis, 2000);
        assert_eq!(cursor.seq, Some(5));

        // Bare [ts] cursor; numeric strings (JSON bigint safety) accepted.
        let parsed = SearchRequest::parse("logs", &json!({"search_after": ["2000"]})).unwrap();
        let cursor = parsed.search_after.unwrap();
        assert_eq!(cursor.timestamp_millis, 2000);
        assert_eq!(cursor.seq, None);

        // The timestamp is NOT unit-heuristic rescaled: small sort values
        // page as-is.
        let parsed = SearchRequest::parse("logs", &json!({"search_after": [1000, -1]})).unwrap();
        assert_eq!(parsed.search_after.unwrap().timestamp_millis, 1000);
        assert_eq!(parsed.search_after.unwrap().seq, Some(-1));

        // Whole-valued floats are accepted (JSON round-trips in some
        // clients re-encode echoed integers as floats).
        let parsed =
            SearchRequest::parse("logs", &json!({"search_after": [2000.0, 5.0]})).unwrap();
        let cursor = parsed.search_after.unwrap();
        assert_eq!((cursor.timestamp_millis, cursor.seq), (2000, Some(5)));

        // An explicit null means "no cursor", not a 400.
        let parsed = SearchRequest::parse("logs", &json!({"search_after": null})).unwrap();
        assert!(parsed.search_after.is_none());

        // A hostile timestamp clamps to the tantivy-safe range instead of
        // overflowing the nanos conversion downstream.
        let parsed =
            SearchRequest::parse("logs", &json!({"search_after": [i64::MAX, 1]})).unwrap();
        assert_eq!(
            parsed.search_after.unwrap().timestamp_millis,
            rsearch_index::MAX_SAFE_MILLIS
        );
    }

    #[test]
    fn search_after_rejects_bad_shapes() {
        for body in [
            json!({"from": 5, "search_after": [1000]}),
            json!({"search_after": []}),
            json!({"search_after": [1, 2, 3]}),
            json!({"search_after": {"ts": 1}}),
            json!({"search_after": ["not-a-number"]}),
            json!({"search_after": [1000, "x"]}),
            // A fractional float is not a sort value we ever emitted.
            json!({"search_after": [1000.5]}),
        ] {
            let err = SearchRequest::parse("logs", &body).unwrap_err();
            assert!(matches!(err, SearchError::BadRequest(_)), "{body}");
        }
    }

    /// In-RAM index of (timestamp_millis, seq) docs at the given schema
    /// version.
    fn build_index(schema_version: u32, docs: &[(i64, i64)]) -> (MappedSchema, tantivy::Index) {
        let schema = MappedSchema::build_versioned(IndexMapping::default(), schema_version);
        let index = schema.create_in_ram();
        let converter = DocumentConverter::new(schema.clone());
        let mut writer = index.writer_with_num_threads(1, 20 << 20).unwrap();
        for (i, (ts, seq)) in docs.iter().enumerate() {
            let identity = DocIdentity::new(format!("d{i}"), *seq);
            let (doc, _) = converter
                .convert_with_source(
                    json!({"n": i}),
                    None,
                    &identity,
                    tantivy::DateTime::from_timestamp_millis(*ts),
                )
                .unwrap();
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
        (schema, index)
    }

    /// Docs as (timestamp_millis, seq) at the given schema version →
    /// how many pass the cursor clause.
    fn count_past(
        schema_version: u32,
        docs: &[(i64, i64)],
        cursor: SearchAfter,
        sort_desc: bool,
    ) -> usize {
        let (schema, index) = build_index(schema_version, docs);
        let searcher = index.reader().unwrap().searcher();
        let clause = cursor_clause(&schema, cursor, sort_desc);
        searcher.search(&clause, &Count).unwrap()
    }

    /// (ts, seq): a=(1000,10) b=(1000,20) c=(2000,5) d=(3000,1).
    /// desc order: d, c, b, a — asc order: a, b, c, d.
    const DOCS: [(i64, i64); 4] = [(1000, 10), (1000, 20), (2000, 5), (3000, 1)];

    fn after(ts: i64, seq: Option<i64>) -> SearchAfter {
        SearchAfter { timestamp_millis: ts, seq }
    }

    #[test]
    fn cursor_pages_strictly_past_ts_and_seq() {
        // desc, cursor at c=(2000,5): b and a remain.
        assert_eq!(count_past(1, &DOCS, after(2000, Some(5)), true), 2);
        // desc, cursor at b=(1000,20): the equal-timestamp tie resolves by
        // seq — only a remains.
        assert_eq!(count_past(1, &DOCS, after(1000, Some(20)), true), 1);
        // desc, cursor at a: page exhausted.
        assert_eq!(count_past(1, &DOCS, after(1000, Some(10)), true), 0);
        // asc, cursor at a=(1000,10): b, c, d remain (tie kept).
        assert_eq!(count_past(1, &DOCS, after(1000, Some(10)), false), 3);
        // asc, cursor at d: exhausted.
        assert_eq!(count_past(1, &DOCS, after(3000, Some(1)), false), 0);
    }

    #[test]
    fn bare_ts_cursor_pages_strictly_by_timestamp() {
        // desc past ts=2000: only the two 1000s remain (equal-ts skipped
        // by design for a 1-element cursor).
        assert_eq!(count_past(1, &DOCS, after(2000, None), true), 2);
        assert_eq!(count_past(1, &DOCS, after(1000, None), true), 0);
        // asc past ts=2000: only d.
        assert_eq!(count_past(1, &DOCS, after(2000, None), false), 1);
    }

    /// Review finding (critical): the per-split top-k must truncate in
    /// the same (timestamp, `_seq`) order the merge and cursors use.
    /// With timestamp-only collection, an equal-timestamp group larger
    /// than the page size kept the wrong end of the group, making the
    /// rest unreachable through search_after (and duplicating pages
    /// under from/size).
    #[test]
    fn pagination_survives_tie_groups_larger_than_page() {
        use tantivy::query::AllQuery;
        let docs: Vec<(i64, i64)> = (1..=6).map(|seq| (1000, seq)).collect();
        let (schema, index) = build_index(1, &docs);
        let searcher = index.reader().unwrap().searcher();
        let page_seqs = |cursor: Option<SearchAfter>, sort_desc: bool| -> Vec<i64> {
            let query: Box<dyn Query> = match cursor {
                Some(c) => Box::new(BooleanQuery::new(vec![
                    (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                    (Occur::Must, cursor_clause(&schema, c, sort_desc)),
                ])),
                None => Box::new(AllQuery),
            };
            searcher
                .search(&query, &top_sort_key_collector(2, sort_desc))
                .unwrap()
                .into_iter()
                .map(|((_, seq), _)| if sort_desc { seq } else { seq.saturating_neg() })
                .collect()
        };
        // desc: pages tile the tie group newest-seq-first, each page's
        // cursor being its last (ts, seq); no doc unreachable, none
        // repeated.
        assert_eq!(page_seqs(None, true), vec![6, 5]);
        assert_eq!(page_seqs(Some(after(1000, Some(5))), true), vec![4, 3]);
        assert_eq!(page_seqs(Some(after(1000, Some(3))), true), vec![2, 1]);
        assert_eq!(page_seqs(Some(after(1000, Some(1))), true), Vec::<i64>::new());
        // asc mirrors.
        assert_eq!(page_seqs(None, false), vec![1, 2]);
        assert_eq!(page_seqs(Some(after(1000, Some(2))), false), vec![3, 4]);
        assert_eq!(page_seqs(Some(after(1000, Some(4))), false), vec![5, 6]);
        assert_eq!(page_seqs(Some(after(1000, Some(6))), false), Vec::<i64>::new());
    }

    /// The collector's primary key stays the timestamp; `_seq` only
    /// breaks ties.
    #[test]
    fn collector_orders_by_timestamp_then_seq() {
        use tantivy::query::AllQuery;
        // Higher seq at an older timestamp must not outrank a newer doc.
        let (_, index) = build_index(1, &[(1000, 900), (2000, 5), (1000, 950)]);
        let searcher = index.reader().unwrap().searcher();
        let keys: Vec<(i64, i64)> = searcher
            .search(&AllQuery, &top_sort_key_collector(3, true))
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, vec![(2000, 5), (1000, 950), (1000, 900)]);
    }

    #[test]
    fn legacy_split_cursor_degrades_by_timestamp() {
        // Version-0 splits have no _seq column; docs sort as seq -1.
        // desc + a real-seq cursor: legacy docs at the boundary timestamp
        // sort after every real doc there, so they are still included.
        assert_eq!(count_past(0, &DOCS, after(2000, Some(5)), true), 3);
        // desc + a legacy (-1) cursor: strictly past the timestamp.
        assert_eq!(count_past(0, &DOCS, after(2000, Some(-1)), true), 2);
        // asc + a real-seq cursor: legacy docs at the boundary already
        // sorted before it — strictly past.
        assert_eq!(count_past(0, &DOCS, after(2000, Some(5)), false), 1);
    }
}
