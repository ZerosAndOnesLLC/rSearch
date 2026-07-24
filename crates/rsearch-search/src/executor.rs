//! Search execution: prune splits via the metastore, search each split on
//! blocking threads, merge hits and aggregations, shape the ES response.

use std::collections::HashMap;
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
/// Cap on cached open split readers.
const READER_CACHE_CAP: usize = 256;

/// A parsed `_search` request body.
pub struct SearchRequest {
    pub stream: String,
    pub query: Value,
    pub from: usize,
    pub size: usize,
    pub sort_desc: bool,
    pub aggs: Option<Value>,
    pub include_source: bool,
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
        if size > 10_000 {
            return Err(SearchError::BadRequest(
                "size must be <= 10000".to_string(),
            ));
        }
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
        })
    }
}

struct SplitHit {
    timestamp_millis: i64,
    source: Option<String>,
    split_id: String,
    doc: DocAddress,
}

struct SplitOutcome {
    count: usize,
    hits: Vec<SplitHit>,
    aggs: Option<IntermediateAggregationResults>,
}

/// Stateless search service: metastore for pruning, storage for split
/// bytes, an LRU'd cache of open readers.
pub struct SearchService {
    metastore: Metastore,
    storage: Arc<dyn Storage>,
    cache: Arc<SplitCache>,
    readers: Mutex<HashMap<String, Arc<SplitReader>>>,
}

impl SearchService {
    pub fn new(metastore: Metastore, storage: Arc<dyn Storage>, cache: Arc<SplitCache>) -> Self {
        Self {
            metastore,
            storage,
            cache,
            readers: Mutex::new(HashMap::new()),
        }
    }

    async fn reader(&self, split_id: &str, storage_key: &str) -> SearchResult<Arc<SplitReader>> {
        {
            let readers = self.readers.lock().await;
            if let Some(reader) = readers.get(split_id) {
                return Ok(reader.clone());
            }
        }
        let reader = Arc::new(
            SplitReader::open(self.storage.clone(), storage_key, self.cache.clone()).await?,
        );
        let mut readers = self.readers.lock().await;
        if readers.len() >= READER_CACHE_CAP {
            // Simple pressure valve; opened readers are cheap to rebuild.
            readers.clear();
        }
        readers.insert(split_id.to_string(), reader.clone());
        Ok(reader)
    }

    /// Execute a search and return the full ES-shaped response body.
    pub async fn search(&self, request: SearchRequest) -> SearchResult<Value> {
        let started = Instant::now();
        let stream = self.metastore.get_stream(&request.stream).await?;
        let mapping = IndexMapping::from_json(&stream.mapping)
            .map_err(|e| SearchError::BadRequest(e.to_string()))?;
        let schema = MappedSchema::build(mapping);

        let (t_start, t_end) = extract_time_bounds(&request.query);
        let splits = self
            .metastore
            .splits_for_query(stream.id, t_start, t_end)
            .await?;

        // Aggregations are rewritten once and parsed per split.
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
        let mut outcomes = Vec::new();
        for split in &splits {
            let reader = self.reader(&split.split_id, &split.storage_key).await?;
            let schema = schema.clone();
            let query_json = request.query.clone();
            let aggregations = aggregations.clone();
            let sort_desc = request.sort_desc;
            let include_source = request.include_source;
            let split_id = split.split_id.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                search_one_split(
                    &reader,
                    &schema,
                    &query_json,
                    aggregations,
                    fetch_limit,
                    sort_desc,
                    include_source,
                    split_id,
                )
            })
            .await
            .map_err(|e| SearchError::Internal(format!("search task panicked: {e}")))??;
            outcomes.push(outcome);
        }

        // Merge: global count, top-k re-sort, aggregation fuse.
        let total: usize = outcomes.iter().map(|o| o.count).sum();
        let mut hits: Vec<SplitHit> = outcomes
            .iter_mut()
            .flat_map(|o| o.hits.drain(..))
            .collect();
        if request.sort_desc {
            hits.sort_by(|a, b| b.timestamp_millis.cmp(&a.timestamp_millis));
        } else {
            hits.sort_by(|a, b| a.timestamp_millis.cmp(&b.timestamp_millis));
        }
        let page: Vec<Value> = hits
            .into_iter()
            .skip(request.from)
            .take(request.size)
            .map(|hit| {
                let source: Value = hit
                    .source
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(Value::Null);
                let mut entry = json!({
                    "_index": request.stream,
                    "_id": format!("{}:{}:{}", hit.split_id, hit.doc.segment_ord, hit.doc.doc_id),
                    "_score": Value::Null,
                    "sort": [hit.timestamp_millis],
                });
                if request.include_source {
                    entry["_source"] = source;
                }
                entry
            })
            .collect();

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
                "total": {"value": total, "relation": "eq"},
                "max_score": Value::Null,
                "hits": page,
            },
        });
        if let Some(aggs) = merged_aggs {
            response["aggregations"] = aggs;
        }
        Ok(response)
    }
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
    include_source: bool,
    split_id: String,
) -> SearchResult<SplitOutcome> {
    let index = reader.index();
    let query = translate_query(index, schema, query_json)?;
    let searcher = reader.searcher()?;

    let order = if sort_desc { Order::Desc } else { Order::Asc };
    let top_collector = TopDocs::with_limit(fetch_limit.max(1))
        .order_by_fast_field::<tantivy::DateTime>("_timestamp", order);

    let (count, top, agg_result) = match aggregations {
        Some(aggs) => {
            let agg_collector =
                tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                    aggs,
                    tantivy::aggregation::AggContextParams::new(
                        default_limits(),
                        index.tokenizers().clone(),
                    ),
                );
            let (count, top, aggs) = searcher
                .search(&query, &(Count, top_collector, agg_collector))
                .map_err(SearchError::Tantivy)?;
            (count, top, Some(aggs))
        }
        None => {
            let (count, top) = searcher
                .search(&query, &(Count, top_collector))
                .map_err(SearchError::Tantivy)?;
            (count, top, None)
        }
    };

    let mut hits = Vec::with_capacity(top.len());
    for (timestamp, address) in top {
        let source = if include_source {
            let doc: TantivyDocument = searcher.doc(address).map_err(SearchError::Tantivy)?;
            doc.get_first(schema.source)
                .and_then(|v| v.as_str().map(str::to_string))
        } else {
            None
        };
        hits.push(SplitHit {
            timestamp_millis: timestamp
                .map(|t| t.into_timestamp_millis())
                .unwrap_or_default(),
            source,
            split_id: split_id.clone(),
            doc: address,
        });
    }
    Ok(SplitOutcome {
        count,
        hits,
        aggs: agg_result,
    })
}
