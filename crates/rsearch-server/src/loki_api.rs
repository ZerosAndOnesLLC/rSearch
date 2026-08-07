//! Loki-compatible HTTP query API subset (#11) so Grafana's built-in
//! Loki datasource (and Logs Drilldown) work against rSearch unmodified.
//!
//! Model mapping:
//! - the `service_name` label ⇄ the rSearch stream name (Drilldown's
//!   anchor label — every stream appears as a browsable service)
//! - other labels ⇄ the stream's keyword-mapped fields; values come from
//!   terms aggregations (capped at [`LABEL_VALUES_LIMIT`])
//! - a log "line" is the doc's `message` field when present, else the
//!   raw `_source` JSON
//! - timestamps convert between Loki nanoseconds and rSearch millis
//!
//! Endpoints: query_range, query, labels, label values, series,
//! index/volume, index/volume_range, tail (WebSocket), ready. LogQL
//! coverage is the subset Grafana sends: selectors with =/!=/=~/!~,
//! line filters |= and != (regex line filters are rejected with a clear
//! error), count_over_time / rate, sum / sum by (…).

use std::collections::{BTreeMap, HashMap};

use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{Value, json};

use rsearch_search::SearchRequest;
use rsearch_search::logql::{self, FilterOp, LogQlQuery, LogSelector, MatchOp, MetricOp, MetricQuery};

use crate::state::AppState;

/// Cap on distinct values returned per label.
const LABEL_VALUES_LIMIT: usize = 1000;
/// Bounded per-request stream fan-out.
const STREAM_CONCURRENCY: usize = 8;
/// Default / max entries for log queries (Loki's defaults are 100/5000).
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 5000;
// ---------- parameter + time helpers ----------

/// Merge query-string and (form-encoded) body parameters, so both the
/// GET and POST forms Grafana uses are accepted.
fn params(uri: &Uri, body: &str) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for source in [uri.query().unwrap_or(""), body] {
        for (k, v) in url::form_urlencoded::parse(source.as_bytes()) {
            out.entry(k.into_owned()).or_default().push(v.into_owned());
        }
    }
    out
}

fn first<'a>(params: &'a HashMap<String, Vec<String>>, key: &str) -> Option<&'a str> {
    params.get(key).and_then(|v| v.first()).map(String::as_str)
}

/// Loki times arrive as nanosecond epoch integers or float seconds.
fn parse_time_ms(s: &str) -> Option<i64> {
    if let Ok(n) = s.parse::<i128>() {
        let ms = if n.abs() >= 100_000_000_000_000_000 {
            n / 1_000_000 // nanoseconds
        } else if n.abs() >= 100_000_000_000_000 {
            n / 1_000 // microseconds
        } else if n.abs() >= 100_000_000_000 {
            n // milliseconds
        } else {
            n * 1000 // seconds
        };
        return i64::try_from(ms).ok();
    }
    s.parse::<f64>().ok().map(|secs| (secs * 1000.0) as i64)
}

/// Step arrives as float seconds ("15") or a duration string ("15s").
fn parse_step_ms(s: &str) -> Option<i64> {
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0).max(1.0) as i64);
    }
    let mut total = 0i64;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            continue;
        }
        let mut unit: String = c.to_string();
        if let Some(&next) = chars.peek()
            && next.is_ascii_alphabetic()
        {
            unit.push(next);
            chars.next();
        }
        let scale = match unit.as_str() {
            "ms" => 1.0,
            "s" => 1000.0,
            "m" => 60_000.0,
            "h" => 3_600_000.0,
            "d" => 86_400_000.0,
            _ => return None,
        };
        total += (num.parse::<f64>().ok()? * scale) as i64;
        num.clear();
    }
    (total > 0).then_some(total)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn loki_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({
            "status": "error",
            "errorType": if status == StatusCode::BAD_REQUEST { "bad_data" } else { "internal" },
            "error": message,
        })),
    )
        .into_response()
}

fn success(data: Value) -> Response {
    Json(json!({"status": "success", "data": data})).into_response()
}

// ---------- selector resolution ----------

/// Streams a selector's `service_name` matchers allow.
async fn resolve_streams(state: &AppState, selector: &LogSelector) -> Result<Vec<String>, String> {
    let all = state
        .cached_stream_names()
        .await
        .map_err(|e| format!("stream listing failed: {e}"))?;
    let mut names: Vec<String> = all.iter().cloned().collect();
    for matcher in selector.matchers.iter().filter(|m| m.label == "service_name") {
        match matcher.op {
            MatchOp::Eq => names.retain(|n| n == &matcher.value),
            MatchOp::Neq => names.retain(|n| n != &matcher.value),
            MatchOp::Re | MatchOp::NotRe => {
                let re = anchored(&matcher.value)?;
                let keep_match = matcher.op == MatchOp::Re;
                names.retain(|n| re.is_match(n) == keep_match);
            }
        }
    }
    Ok(names)
}

/// Loki label regexes are fully anchored.
fn anchored(pattern: &str) -> Result<regex::Regex, String> {
    regex::Regex::new(&format!("^(?:{pattern})$")).map_err(|e| format!("invalid regex: {e}"))
}

/// Keyword-mapped fields of a stream — the fields exposed as labels.
async fn label_fields(state: &AppState, stream: &str) -> Vec<String> {
    let Ok(record) = state.metastore.get_stream(stream).await else {
        return Vec::new();
    };
    let mut fields: Vec<String> = record
        .mapping
        .get("properties")
        .and_then(Value::as_object)
        .map(|props| {
            props
                .iter()
                .filter(|(_, spec)| {
                    spec.get("type").and_then(Value::as_str) == Some("keyword")
                })
                .map(|(name, _)| name.clone())
                .collect()
        })
        .unwrap_or_default();
    fields.sort();
    fields
}

/// Distinct values of a label field within one stream (terms agg).
async fn field_values(
    state: &AppState,
    stream: &str,
    field: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> Vec<String> {
    let Some(service) = state.search.clone() else {
        return Vec::new();
    };
    let mut filters = vec![];
    if start_ms.is_some() || end_ms.is_some() {
        let mut range = serde_json::Map::new();
        if let Some(s) = start_ms {
            range.insert("gte".into(), json!(s));
        }
        if let Some(e) = end_ms {
            range.insert("lte".into(), json!(e));
        }
        filters.push(json!({"range": {"@timestamp": Value::Object(range)}}));
    }
    let body = json!({
        "size": 0,
        "track_total_hits": false,
        "query": {"bool": {"filter": filters}},
        "aggs": {"v": {"terms": {"field": field, "size": LABEL_VALUES_LIMIT}}},
    });
    let Ok(request) = SearchRequest::parse(stream, &body) else {
        return Vec::new();
    };
    match service.search(request).await {
        Ok(result) => result["aggregations"]["v"]["buckets"]
            .as_array()
            .map(|buckets| {
                buckets
                    .iter()
                    .filter_map(|b| b["key"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Build the ES bool query for a selector against one stream. Returns
/// None when a matcher can never match in this stream (e.g. a regex that
/// matches no values), so the stream is skipped entirely.
async fn selector_query(
    state: &AppState,
    stream: &str,
    selector: &LogSelector,
    start_ms: i64,
    end_ms: i64,
) -> Result<Option<Value>, String> {
    let mut filter = vec![json!({"range": {"@timestamp": {"gte": start_ms, "lte": end_ms}}})];
    let mut must_not = vec![];

    for matcher in &selector.matchers {
        if matcher.label == "service_name" {
            continue; // already applied via stream resolution
        }
        match matcher.op {
            MatchOp::Eq => filter.push(json!({"term": {&matcher.label: matcher.value}})),
            MatchOp::Neq => must_not.push(json!({"term": {&matcher.label: matcher.value}})),
            MatchOp::Re | MatchOp::NotRe => {
                let re = anchored(&matcher.value)?;
                let values: Vec<String> = field_values(state, stream, &matcher.label, None, None)
                    .await
                    .into_iter()
                    .filter(|v| re.is_match(v))
                    .collect();
                match matcher.op {
                    MatchOp::Re if values.is_empty() => return Ok(None),
                    MatchOp::Re => filter.push(json!({"terms": {&matcher.label: values}})),
                    _ if !values.is_empty() => {
                        must_not.push(json!({"terms": {&matcher.label: values}}));
                    }
                    _ => {}
                }
            }
        }
    }

    for line_filter in &selector.filters {
        match line_filter.op {
            FilterOp::Contains => {
                filter.push(json!({"match_phrase": {"message": line_filter.text}}));
            }
            FilterOp::NotContains => {
                must_not.push(json!({"match_phrase": {"message": line_filter.text}}));
            }
            FilterOp::Regex | FilterOp::NotRegex => {
                return Err(
                    "regex line filters (|~, !~) are not supported yet; use |= / != text filters"
                        .to_string(),
                );
            }
        }
    }

    Ok(Some(json!({"bool": {"filter": filter, "must_not": must_not}})))
}

// ---------- log queries ----------

struct LogHit {
    ts_ms: i64,
    labels: BTreeMap<String, String>,
    line: String,
}

/// Run a log selector across its streams and return merged, direction-
/// sorted, limit-truncated hits.
async fn run_log_query(
    state: &AppState,
    selector: &LogSelector,
    start_ms: i64,
    end_ms: i64,
    limit: usize,
    forward: bool,
) -> Result<Vec<LogHit>, String> {
    use futures::stream::{self, StreamExt};
    let service = state
        .search
        .clone()
        .ok_or_else(|| "this node does not run the search role".to_string())?;
    let streams = resolve_streams(state, selector).await?;
    let order = if forward { "asc" } else { "desc" };

    let futs: Vec<_> = streams
        .iter()
        .map(|stream| {
            let service = service.clone();
            async move {
                let Some(query) = selector_query(state, stream, selector, start_ms, end_ms).await?
                else {
                    return Ok::<_, String>(Vec::new());
                };
                let fields = label_fields(state, stream).await;
                let body = json!({
                    "size": limit,
                    "track_total_hits": false,
                    "sort": [{"@timestamp": {"order": order}}],
                    "query": query,
                });
                let request = SearchRequest::parse(stream, &body).map_err(|e| e.to_string())?;
                let result = match service.search(request).await {
                    Ok(result) => result,
                    // A stream vanishing mid-query is not an error.
                    Err(rsearch_search::SearchError::Metastore(
                        rsearch_metastore::MetastoreError::StreamNotFound(_),
                    )) => return Ok(Vec::new()),
                    Err(e) => return Err(e.to_string()),
                };
                let hits = result["hits"]["hits"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                Ok(hits
                    .into_iter()
                    .filter_map(|hit| {
                        let ts_ms = hit["sort"][0].as_i64()?;
                        let source = &hit["_source"];
                        let line = source
                            .get("message")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| source.to_string());
                        let mut labels = BTreeMap::new();
                        labels.insert("service_name".to_string(), stream.clone());
                        for field in &fields {
                            match source.get(field) {
                                Some(Value::String(s)) => {
                                    labels.insert(field.clone(), s.clone());
                                }
                                Some(Value::Number(n)) => {
                                    labels.insert(field.clone(), n.to_string());
                                }
                                Some(Value::Bool(b)) => {
                                    labels.insert(field.clone(), b.to_string());
                                }
                                _ => {}
                            }
                        }
                        Some(LogHit { ts_ms, labels, line })
                    })
                    .collect::<Vec<_>>())
            }
        })
        .collect();

    let per_stream: Vec<Result<Vec<LogHit>, String>> = stream::iter(futs)
        .buffer_unordered(STREAM_CONCURRENCY)
        .collect()
        .await;
    let mut hits: Vec<LogHit> = Vec::new();
    for result in per_stream {
        hits.extend(result?);
    }
    if forward {
        hits.sort_by_key(|h| h.ts_ms);
    } else {
        hits.sort_by_key(|h| std::cmp::Reverse(h.ts_ms));
    }
    hits.truncate(limit);
    Ok(hits)
}

/// Group hits into Loki `streams` result entries.
fn to_streams_result(hits: Vec<LogHit>) -> Value {
    let mut by_labels: BTreeMap<String, (BTreeMap<String, String>, Vec<(i64, String)>)> =
        BTreeMap::new();
    for hit in hits {
        let key = format!("{:?}", hit.labels);
        by_labels
            .entry(key)
            .or_insert_with(|| (hit.labels.clone(), Vec::new()))
            .1
            .push((hit.ts_ms, hit.line));
    }
    let result: Vec<Value> = by_labels
        .into_values()
        .map(|(labels, values)| {
            json!({
                "stream": labels,
                "values": values
                    .into_iter()
                    .map(|(ts_ms, line)| {
                        json!([format!("{}", (ts_ms as i128) * 1_000_000), line])
                    })
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({"resultType": "streams", "result": result, "stats": {}})
}

// ---------- metric queries ----------

/// One matrix/vector series under construction: label set + bucket sums.
type SeriesMap = BTreeMap<String, (BTreeMap<String, String>, BTreeMap<i64, f64>)>;

/// Execute a metric query, returning series of (labels, ts_ms → value).
async fn run_metric_query(
    state: &AppState,
    metric: &MetricQuery,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<SeriesMap, String> {
    use futures::stream::{self, StreamExt};
    let service = state
        .search
        .clone()
        .ok_or_else(|| "this node does not run the search role".to_string())?;
    if metric.group_by.len() > 1 {
        return Err("sum by (…) supports at most one label".to_string());
    }
    let group_label = metric.group_by.first().map(String::as_str);
    let streams = resolve_streams(state, &metric.selector).await?;
    // Values are per-second for rate, raw counts for count_over_time.
    let divisor = match metric.op {
        MetricOp::Rate => (metric.range_millis.max(1) as f64) / 1000.0,
        MetricOp::CountOverTime => 1.0,
    };

    let futs: Vec<_> = streams
        .iter()
        .map(|stream| {
            let service = service.clone();
            let stream = stream.clone();
            async move {
                let run = async {
                let stream = stream.as_str();
                let Some(query) =
                    selector_query(state, stream, &metric.selector, start_ms, end_ms).await?
                else {
                    return Ok::<_, String>(Vec::new());
                };
                let histogram = json!({
                    "date_histogram": {"field": "@timestamp", "fixed_interval": format!("{step_ms}ms")},
                });
                let aggs = match group_label {
                    Some(label) if label != "service_name" => {
                        let mut ts = histogram;
                        ts["aggs"] =
                            json!({"by": {"terms": {"field": label, "size": LABEL_VALUES_LIMIT}}});
                        json!({"ts": ts})
                    }
                    _ => json!({"ts": histogram}),
                };
                let body = json!({
                    "size": 0,
                    "track_total_hits": false,
                    "query": query,
                    "aggs": aggs,
                });
                let request = SearchRequest::parse(stream, &body).map_err(|e| e.to_string())?;
                let result = match service.search(request).await {
                    Ok(result) => result,
                    Err(rsearch_search::SearchError::Metastore(
                        rsearch_metastore::MetastoreError::StreamNotFound(_),
                    )) => return Ok(Vec::new()),
                    Err(e) => return Err(e.to_string()),
                };
                let buckets = result["aggregations"]["ts"]["buckets"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                // (bucket ts, optional group value, count)
                let mut out: Vec<(i64, Option<String>, f64)> = Vec::new();
                for bucket in buckets {
                    let ts = bucket["key"]
                        .as_f64()
                        .map(|k| k as i64)
                        .or_else(|| bucket["key"].as_i64())
                        .unwrap_or(0);
                    match group_label {
                        Some(label) if label != "service_name" => {
                            for sub in bucket["by"]["buckets"].as_array().into_iter().flatten() {
                                let value = match &sub["key"] {
                                    Value::String(s) => s.clone(),
                                    other => other.to_string(),
                                };
                                let count = sub["doc_count"].as_f64().unwrap_or(0.0);
                                out.push((ts, Some(value), count));
                            }
                        }
                        _ => {
                            let count = bucket["doc_count"].as_f64().unwrap_or(0.0);
                            out.push((ts, None, count));
                        }
                    }
                }
                Ok(out)
                };
                let result = run.await;
                (stream, result)
            }
        })
        .collect();

    let per_stream: Vec<(String, Result<Vec<(i64, Option<String>, f64)>, String>)> =
        stream::iter(futs)
            .buffer_unordered(STREAM_CONCURRENCY)
            .collect()
            .await;

    let mut series: SeriesMap = BTreeMap::new();
    for (stream, result) in per_stream {
        for (ts, group_value, count) in result? {
            // Which series does this sample belong to?
            let labels: BTreeMap<String, String> = match (group_label, &group_value) {
                (Some(label), Some(value)) => {
                    BTreeMap::from([(label.to_string(), value.clone())])
                }
                (Some("service_name"), None) => {
                    BTreeMap::from([("service_name".to_string(), stream.clone())])
                }
                (None, _) if metric.summed => BTreeMap::new(),
                _ => BTreeMap::from([("service_name".to_string(), stream.clone())]),
            };
            let key = format!("{labels:?}");
            let entry = series.entry(key).or_insert_with(|| (labels, BTreeMap::new()));
            *entry.1.entry(ts).or_insert(0.0) += count / divisor;
        }
    }
    Ok(series)
}

fn to_matrix_result(series: SeriesMap) -> Value {
    let result: Vec<Value> = series
        .into_values()
        .map(|(labels, points)| {
            json!({
                "metric": labels,
                "values": points
                    .into_iter()
                    .map(|(ts_ms, v)| json!([ts_ms as f64 / 1000.0, format!("{v}")]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({"resultType": "matrix", "result": result, "stats": {}})
}

// ---------- handlers ----------

/// GET/POST /loki/api/v1/query_range
pub async fn query_range(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let Some(query) = first(&params, "query") else {
        return loki_error(StatusCode::BAD_REQUEST, "missing 'query' parameter");
    };
    let end_ms = first(&params, "end").and_then(parse_time_ms).unwrap_or_else(now_ms);
    let start_ms = first(&params, "start")
        .and_then(parse_time_ms)
        .unwrap_or(end_ms - 3_600_000);
    let limit = first(&params, "limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);
    let forward = first(&params, "direction") == Some("forward");

    match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => {
            match run_log_query(&state, &selector, start_ms, end_ms, limit, forward).await {
                Ok(hits) => success(to_streams_result(hits)),
                Err(e) => loki_error(StatusCode::BAD_REQUEST, &e),
            }
        }
        Ok(LogQlQuery::Metric(metric)) => {
            let step_ms = first(&params, "step")
                .and_then(parse_step_ms)
                .unwrap_or_else(|| ((end_ms - start_ms) / 250).max(1000));
            match run_metric_query(&state, &metric, start_ms, end_ms, step_ms).await {
                Ok(series) => success(to_matrix_result(series)),
                Err(e) => loki_error(StatusCode::BAD_REQUEST, &e),
            }
        }
        Err(e) => loki_error(StatusCode::BAD_REQUEST, &format!("LogQL parse error: {e}")),
    }
}

/// GET/POST /loki/api/v1/query — instant query.
pub async fn query_instant(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let Some(query) = first(&params, "query") else {
        return loki_error(StatusCode::BAD_REQUEST, "missing 'query' parameter");
    };
    let time_ms = first(&params, "time").and_then(parse_time_ms).unwrap_or_else(now_ms);
    let limit = first(&params, "limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);

    match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => {
            // Instant log query: last `limit` lines up to `time`.
            match run_log_query(&state, &selector, time_ms - 3_600_000, time_ms, limit, false)
                .await
            {
                Ok(hits) => success(to_streams_result(hits)),
                Err(e) => loki_error(StatusCode::BAD_REQUEST, &e),
            }
        }
        Ok(LogQlQuery::Metric(metric)) => {
            // One bucket covering [time - range, time] → vector.
            let start_ms = time_ms - metric.range_millis;
            match run_metric_query(&state, &metric, start_ms, time_ms, metric.range_millis.max(1))
                .await
            {
                Ok(series) => {
                    let result: Vec<Value> = series
                        .into_values()
                        .map(|(labels, points)| {
                            let total: f64 = points.values().sum();
                            json!({
                                "metric": labels,
                                "value": [time_ms as f64 / 1000.0, format!("{total}")],
                            })
                        })
                        .collect();
                    success(json!({"resultType": "vector", "result": result, "stats": {}}))
                }
                Err(e) => loki_error(StatusCode::BAD_REQUEST, &e),
            }
        }
        Err(e) => loki_error(StatusCode::BAD_REQUEST, &format!("LogQL parse error: {e}")),
    }
}

/// GET /loki/api/v1/labels
pub async fn labels(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let _ = params(&uri, &body); // start/end accepted but unused
    let streams = match state.cached_stream_names().await {
        Ok(streams) => streams,
        Err(e) => return loki_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let mut labels: Vec<String> = vec!["service_name".to_string()];
    for stream in streams.iter() {
        for field in label_fields(&state, stream).await {
            if !labels.contains(&field) {
                labels.push(field);
            }
        }
    }
    labels.sort();
    success(json!(labels))
}

/// GET /loki/api/v1/label/{name}/values
pub async fn label_values(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    uri: Uri,
    body: String,
) -> Response {
    let params = params(&uri, &body);
    let start_ms = first(&params, "start").and_then(parse_time_ms);
    let end_ms = first(&params, "end").and_then(parse_time_ms);
    let streams = match state.cached_stream_names().await {
        Ok(streams) => streams,
        Err(e) => return loki_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    if name == "service_name" {
        let mut names: Vec<String> = streams.iter().cloned().collect();
        names.sort();
        return success(json!(names));
    }
    let mut values: Vec<String> = Vec::new();
    for stream in streams.iter() {
        if !label_fields(&state, stream).await.contains(&name) {
            continue;
        }
        for value in field_values(&state, stream, &name, start_ms, end_ms).await {
            if !values.contains(&value) {
                values.push(value);
            }
        }
        if values.len() >= LABEL_VALUES_LIMIT {
            break;
        }
    }
    values.sort();
    values.truncate(LABEL_VALUES_LIMIT);
    success(json!(values))
}

/// GET/POST /loki/api/v1/series
pub async fn series(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let selectors: Vec<&String> = params
        .get("match[]")
        .into_iter()
        .flatten()
        .chain(params.get("match").into_iter().flatten())
        .collect();
    let mut sets: Vec<Value> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for raw in selectors {
        let Ok(LogQlQuery::Log(selector)) = logql::parse(raw) else {
            continue;
        };
        let Ok(streams) = resolve_streams(&state, &selector).await else {
            continue;
        };
        for stream in streams {
            if !seen.contains(&stream) {
                sets.push(json!({"service_name": stream}));
                seen.push(stream);
            }
        }
    }
    success(json!(sets))
}

/// GET/POST /loki/api/v1/index/volume — total counts per label set.
pub async fn volume(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let Some(query) = first(&params, "query") else {
        return loki_error(StatusCode::BAD_REQUEST, "missing 'query' parameter");
    };
    let end_ms = first(&params, "end").and_then(parse_time_ms).unwrap_or_else(now_ms);
    let start_ms = first(&params, "start")
        .and_then(parse_time_ms)
        .unwrap_or(end_ms - 3_600_000);
    let selector = match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => selector,
        Ok(LogQlQuery::Metric(metric)) => metric.selector,
        Err(e) => return loki_error(StatusCode::BAD_REQUEST, &format!("LogQL parse error: {e}")),
    };
    let group_label = first(&params, "targetLabels")
        .or_else(|| first(&params, "aggregateBy"))
        .and_then(|l| l.split(',').next())
        .filter(|l| !l.is_empty() && *l != "none")
        .map(str::to_string)
        .or_else(|| {
            selector
                .matchers
                .iter()
                .map(|m| m.label.clone())
                .next()
        })
        .unwrap_or_else(|| "service_name".to_string());

    let metric = MetricQuery {
        selector,
        range_millis: (end_ms - start_ms).max(1),
        op: MetricOp::CountOverTime,
        group_by: vec![group_label],
        summed: true,
    };
    match run_metric_query(&state, &metric, start_ms, end_ms, (end_ms - start_ms).max(1)).await {
        Ok(series) => {
            let mut result: Vec<Value> = series
                .into_values()
                .map(|(labels, points)| {
                    let total: f64 = points.values().sum();
                    json!({
                        "metric": labels,
                        "value": [end_ms as f64 / 1000.0, format!("{total}")],
                    })
                })
                .collect();
            // Loki orders volume results largest-first.
            result.sort_by(|a, b| {
                let val = |v: &Value| {
                    v["value"][1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0)
                };
                val(b).partial_cmp(&val(a)).unwrap_or(std::cmp::Ordering::Equal)
            });
            if let Some(limit) = first(&params, "limit").and_then(|l| l.parse::<usize>().ok()) {
                result.truncate(limit);
            }
            success(json!({"resultType": "vector", "result": result, "stats": {}}))
        }
        Err(e) => loki_error(StatusCode::BAD_REQUEST, &e),
    }
}

/// GET/POST /loki/api/v1/index/volume_range — volume over time (matrix).
pub async fn volume_range(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let Some(query) = first(&params, "query") else {
        return loki_error(StatusCode::BAD_REQUEST, "missing 'query' parameter");
    };
    let end_ms = first(&params, "end").and_then(parse_time_ms).unwrap_or_else(now_ms);
    let start_ms = first(&params, "start")
        .and_then(parse_time_ms)
        .unwrap_or(end_ms - 3_600_000);
    let step_ms = first(&params, "step")
        .and_then(parse_step_ms)
        .unwrap_or_else(|| ((end_ms - start_ms) / 100).max(1000));
    let selector = match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => selector,
        Ok(LogQlQuery::Metric(metric)) => metric.selector,
        Err(e) => return loki_error(StatusCode::BAD_REQUEST, &format!("LogQL parse error: {e}")),
    };
    let group_label = first(&params, "targetLabels")
        .or_else(|| first(&params, "aggregateBy"))
        .and_then(|l| l.split(',').next())
        .filter(|l| !l.is_empty() && *l != "none")
        .unwrap_or("service_name")
        .to_string();
    let metric = MetricQuery {
        selector,
        range_millis: step_ms,
        op: MetricOp::CountOverTime,
        group_by: vec![group_label],
        summed: true,
    };
    match run_metric_query(&state, &metric, start_ms, end_ms, step_ms).await {
        Ok(series) => success(to_matrix_result(series)),
        Err(e) => loki_error(StatusCode::BAD_REQUEST, &e),
    }
}

/// GET /loki/api/v1/tail — WebSocket live tail, implemented as a 1s poll
/// with a monotonically advancing cursor.
pub async fn tail(
    State(state): State<AppState>,
    ws: axum::extract::ws::WebSocketUpgrade,
    uri: Uri,
) -> Response {
    let params = params(&uri, "");
    let Some(query) = first(&params, "query").map(str::to_string) else {
        return loki_error(StatusCode::BAD_REQUEST, "missing 'query' parameter");
    };
    let selector = match logql::parse(&query) {
        Ok(LogQlQuery::Log(selector)) => selector,
        Ok(LogQlQuery::Metric(_)) => {
            return loki_error(StatusCode::BAD_REQUEST, "tail requires a log selector");
        }
        Err(e) => return loki_error(StatusCode::BAD_REQUEST, &format!("LogQL parse error: {e}")),
    };
    let limit = first(&params, "limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);
    // Docs become searchable only once their batch publishes a split
    // (up to ingest.max_batch_secs after arrival), so a hard cursor at
    // "now" would skip everything. Instead each tick re-scans a trailing
    // window and dedupes what it already emitted; the watermark advances
    // once entries are old enough that late publishes are no longer
    // expected. Emission latency is one publish + one poll tick.
    const TAIL_RESCAN_MS: i64 = 60_000;
    const TAIL_SEEN_CAP: usize = 100_000;
    let mut watermark_ms = first(&params, "start")
        .and_then(parse_time_ms)
        .unwrap_or_else(|| now_ms() - 5_000);

    ws.on_upgrade(move |mut socket| async move {
        use std::hash::{Hash, Hasher};
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                message = socket.recv() => {
                    // Client closed (or errored): stop tailing.
                    match message {
                        None | Some(Err(_)) => return,
                        Some(Ok(axum::extract::ws::Message::Close(_))) => return,
                        Some(Ok(_)) => {}
                    }
                }
                _ = interval.tick() => {
                    let end_ms = now_ms();
                    if end_ms <= watermark_ms {
                        continue;
                    }
                    let hits = match run_log_query(
                        &state, &selector, watermark_ms, end_ms, limit, true,
                    ).await {
                        Ok(hits) => hits,
                        Err(_) => continue, // transient; retry next tick
                    };
                    let fresh: Vec<LogHit> = hits
                        .into_iter()
                        .filter(|hit| {
                            let mut hasher =
                                std::collections::hash_map::DefaultHasher::new();
                            (hit.ts_ms, &hit.line, &hit.labels).hash(&mut hasher);
                            seen.insert(hasher.finish())
                        })
                        .collect();
                    // Advance past entries old enough that a late split
                    // publish can no longer add to them; prune the dedup
                    // set to the remaining window (or reset if flooded).
                    watermark_ms = watermark_ms.max(end_ms - TAIL_RESCAN_MS);
                    if seen.len() > TAIL_SEEN_CAP {
                        seen.clear();
                        watermark_ms = end_ms;
                    }
                    if fresh.is_empty() {
                        continue;
                    }
                    let frame = to_streams_result(fresh);
                    let payload = json!({
                        "streams": frame["result"],
                        "dropped_entries": Value::Null,
                    });
                    if socket
                        .send(axum::extract::ws::Message::Text(payload.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    })
}

/// GET /ready — Loki readiness probe (Grafana datasource health check).
pub async fn ready() -> Response {
    (StatusCode::OK, "ready\n").into_response()
}
