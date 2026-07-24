//! Search execution: prune splits via the metastore, search splits
//! concurrently on blocking threads, merge hits and aggregations, fetch
//! `_source` only for the final page, shape the ES response.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{Value, json};
use tantivy::aggregation::AggregationLimitsGuard;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::collector::{Count, TopDocs};
use tantivy::schema::Value as _;
use tantivy::{DocAddress, Order, TantivyDocument};
use tokio::sync::Mutex;
use tracing::warn;

use rsearch_index::{IndexMapping, MappedSchema, SplitCache, SplitReader};
use rsearch_metastore::Metastore;
use rsearch_storage::Storage;

use crate::error::{SearchError, SearchResult};
use crate::query_dsl::{extract_time_bounds, rewrite_agg_fields, translate_query};

const TIMESTAMP_ALIASES: [&str; 3] = ["@timestamp", "timestamp", "_timestamp"];
/// Cap on cached open split readers (LRU).
const READER_CACHE_CAP: usize = 256;
/// Max concurrent split searches per query.
const SPLIT_SEARCH_CONCURRENCY: usize = 16;
/// ES-compatible default exact-count ceiling; beyond this the reported
/// total is a lower bound (`"relation": "gte"`).
const DEFAULT_TRACK_TOTAL_HITS: usize = 10_000;
/// Hard ceiling on from+size (ES max_result_window).
const MAX_RESULT_WINDOW: usize = 10_000;

/// A parsed `_search` request body.
pub struct SearchRequest {
    pub stream: String,
    pub query: Value,
    pub from: usize,
    pub size: usize,
    pub sort_desc: bool,
    pub aggs: Option<Value>,
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

/// A hit reference — no `_source` yet; it's fetched only for the final
/// page after the global merge, and includes a stable tiebreaker.
#[derive(Clone)]
struct SplitHit {
    timestamp_millis: i64,
    split_idx: usize,
    doc: DocAddress,
}

struct SplitOutcome {
    /// Exact count, or a lower bound when capped by track_total_hits.
    count: usize,
    count_is_lower_bound: bool,
    hits: Vec<SplitHit>,
    aggs: Option<IntermediateAggregationResults>,
}

/// Stateless search service: metastore for pruning, storage for split
/// bytes, an LRU cache of open readers with single-flight opens.
pub struct SearchService {
    metastore: Metastore,
    storage: Arc<dyn Storage>,
    cache: Arc<SplitCache>,
    readers: Mutex<lru::LruCache<String, Arc<SplitReader>>>,
    /// Per-split open locks so concurrent queries opening the same cold
    /// split don't each pay the open cost.
    opening: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl SearchService {
    pub fn new(metastore: Metastore, storage: Arc<dyn Storage>, cache: Arc<SplitCache>) -> Self {
        Self {
            metastore,
            storage,
            cache,
            readers: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(READER_CACHE_CAP).unwrap(),
            )),
            opening: Mutex::new(HashMap::new()),
        }
    }

    async fn reader(&self, split_id: &str, storage_key: &str) -> SearchResult<Arc<SplitReader>> {
        if let Some(reader) = self.readers.lock().await.get(split_id).cloned() {
            return Ok(reader);
        }
        // Single-flight: coalesce concurrent opens of the same split.
        let gate = {
            let mut opening = self.opening.lock().await;
            opening
                .entry(split_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = gate.lock().await;
        // Someone may have opened it while we waited for the gate.
        if let Some(reader) = self.readers.lock().await.get(split_id).cloned() {
            return Ok(reader);
        }
        let reader = Arc::new(
            SplitReader::open(self.storage.clone(), storage_key, self.cache.clone()).await?,
        );
        // LRU insert evicts only the least-recently-used entry, not the
        // whole cache.
        self.readers
            .lock()
            .await
            .put(split_id.to_string(), reader.clone());
        self.opening.lock().await.remove(split_id);
        Ok(reader)
    }

    /// Execute a search and return the full ES-shaped response body.
    pub async fn search(&self, request: SearchRequest) -> SearchResult<Value> {
        use futures::stream::{self, StreamExt};

        let started = Instant::now();
        let stream = self.metastore.get_stream(&request.stream).await?;
        let mapping = IndexMapping::from_json(&stream.mapping)
            .map_err(|e| SearchError::BadRequest(e.to_string()))?;
        let schema = Arc::new(MappedSchema::build(mapping));

        let (t_start, t_end) = extract_time_bounds(&request.query);
        let splits = self
            .metastore
            .splits_for_query(stream.id, t_start, t_end)
            .await?;

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
        let futs: Vec<_> = splits
            .iter()
            .enumerate()
            .map(|(idx, split)| {
                let this = &*self;
                let schema = schema.clone();
                let query = query.clone();
                let aggregations = aggregations.clone();
                let split_id = split.split_id.clone();
                let storage_key = split.storage_key.clone();
                let doc_count = split.doc_count as usize;
                // A split fully inside [t_start, t_end] needs no filtering
                // for a match_all query — its whole doc_count matches, so it
                // reports its count without scanning (H3).
                let fully_covered = is_match_all
                    && t_start.map(|s| split.time_start_millis >= s).unwrap_or(true)
                    && t_end.map(|e| split.time_end_millis <= e).unwrap_or(true);
                async move {
                    let reader = this.reader(&split_id, &storage_key).await?;
                    tokio::task::spawn_blocking(move || {
                        search_one_split(
                            &reader,
                            &schema,
                            &query,
                            aggregations,
                            fetch_limit,
                            sort_desc,
                            track,
                            idx,
                            doc_count,
                            fully_covered,
                        )
                    })
                    .await
                    .map_err(|e| SearchError::Internal(format!("search task panicked: {e}")))?
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

        // Fetch _source only for the final page, grouped by split so each
        // reader is used once (H4).
        let page_entries = if request.include_source {
            self.fetch_page_sources(&splits, &schema, &page, &request.stream)
                .await?
        } else {
            page.iter()
                .map(|hit| hit_envelope(hit, &splits, &request.stream, None))
                .collect()
        };

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
                final_result
                    .map(|r| serde_json::to_value(r).unwrap_or(Value::Null))
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

    /// Fetch `_source` for the final page only, grouping by split so each
    /// reader is used once on a single blocking task.
    async fn fetch_page_sources(
        &self,
        splits: &[rsearch_metastore::SplitRecord],
        schema: &Arc<MappedSchema>,
        page: &[SplitHit],
        stream: &str,
    ) -> SearchResult<Vec<Value>> {
        // Group page positions by split.
        let mut by_split: HashMap<usize, Vec<(usize, DocAddress)>> = HashMap::new();
        for (pos, hit) in page.iter().enumerate() {
            by_split.entry(hit.split_idx).or_default().push((pos, hit.doc));
        }
        let mut sources: Vec<Value> = vec![Value::Null; page.len()];
        for (split_idx, wants) in by_split {
            let split = &splits[split_idx];
            let reader = self.reader(&split.split_id, &split.storage_key).await?;
            let schema = schema.clone();
            let fetched = tokio::task::spawn_blocking(move || {
                let searcher = reader.searcher()?;
                let mut out = Vec::with_capacity(wants.len());
                for (pos, address) in wants {
                    let doc: TantivyDocument =
                        searcher.doc(address).map_err(SearchError::Tantivy)?;
                    let source = doc
                        .get_first(schema.source)
                        .and_then(|v| v.as_str().map(str::to_string));
                    out.push((pos, source));
                }
                Ok::<_, SearchError>(out)
            })
            .await
            .map_err(|e| SearchError::Internal(format!("source fetch panicked: {e}")))??;
            for (pos, source) in fetched {
                sources[pos] = source
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(Value::Null);
            }
        }
        Ok(page
            .iter()
            .zip(sources)
            .map(|(hit, source)| hit_envelope(hit, splits, stream, Some(source)))
            .collect())
    }
}

/// Build the ES hit envelope. `source` is Some(value) when `_source` is
/// requested (value may be Null if unfetchable), None to omit the field.
fn hit_envelope(
    hit: &SplitHit,
    splits: &[rsearch_metastore::SplitRecord],
    stream: &str,
    source: Option<Value>,
) -> Value {
    let split_id = &splits[hit.split_idx].split_id;
    let mut entry = json!({
        "_index": stream,
        "_id": format!("{}:{}:{}", split_id, hit.doc.segment_ord, hit.doc.doc_id),
        "_score": Value::Null,
        "sort": [hit.timestamp_millis],
    });
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
    schema: &MappedSchema,
    query_json: &Value,
    aggregations: Option<Aggregations>,
    fetch_limit: usize,
    sort_desc: bool,
    track_total_hits: Option<usize>,
    split_idx: usize,
    doc_count: usize,
    fully_covered: bool,
) -> SearchResult<SplitOutcome> {
    let index = reader.index();
    let query = translate_query(index, schema, query_json)?;
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

    // track_total_hits:false → don't run the Count collector at all; the
    // total becomes a lower bound (the page length). Aggregations still
    // need their collector.
    let skip_count = track_total_hits == Some(0);

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
