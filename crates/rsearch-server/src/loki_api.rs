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
//! Line filters (`|=`, `!=`, `|~`, `!~`) are true substring/regex tests
//! against the rendered line, applied after fetching — never a tokenized
//! index match that could silently miss "error" when searching "err".
//! The cost of that honesty: a filtered query examines at most
//! [`SCAN_LIMIT`] selector-matching docs per stream per request, newest
//! first; results deeper than the scan window require narrowing the
//! selector or time range.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{Value, json};

use rsearch_search::SearchRequest;
use rsearch_search::logql::{
    self, FilterOp, LineFilter, LogQlQuery, LogSelector, MatchOp, MetricOp, MetricQuery,
};

use crate::state::AppState;

/// Cap on distinct values returned per label.
const LABEL_VALUES_LIMIT: usize = 1000;
/// Bounded per-request stream fan-out.
const STREAM_CONCURRENCY: usize = 8;
/// Default / max entries for log queries (Loki's defaults are 100/5000).
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 5000;
/// Docs examined per stream when line filters (or filtered metrics)
/// require post-filtering the rendered lines.
const SCAN_LIMIT: usize = 5000;
/// Ceiling on histogram buckets for a metric query — guards absurd
/// step/range combinations from allocating unbounded bucket maps.
const MAX_METRIC_BUCKETS: i64 = 20_000;
/// Concurrent WebSocket tail sessions per node.
const MAX_TAIL_SESSIONS: usize = 16;
/// Hard ceiling on one tail session's lifetime.
const TAIL_MAX_SESSION_SECS: u64 = 3600;

// ---------- errors ----------

/// User errors become 400 `bad_data`; backend errors become 503 so
/// clients (and shippers behind Grafana) treat them as retryable rather
/// than as a broken query.
enum QueryError {
    User(String),
    Backend(String),
}

impl QueryError {
    fn into_response(self) -> Response {
        match self {
            QueryError::User(message) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "errorType": "bad_data", "error": message})),
            )
                .into_response(),
            QueryError::Backend(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"status": "error", "errorType": "internal", "error": message})),
            )
                .into_response(),
        }
    }

    fn message(&self) -> &str {
        match self {
            QueryError::User(m) | QueryError::Backend(m) => m,
        }
    }

    fn from_search(e: rsearch_search::SearchError) -> Self {
        match e {
            rsearch_search::SearchError::BadRequest(m) => QueryError::User(m),
            other => QueryError::Backend(other.to_string()),
        }
    }
}

fn bad_request(message: impl Into<String>) -> Response {
    QueryError::User(message.into()).into_response()
}

/// Success envelope; Loki carries partial-result warnings at the top
/// level next to `data`.
fn success(data: Value, warnings: Vec<String>) -> Response {
    let mut body = json!({"status": "success", "data": data});
    if !warnings.is_empty() {
        body["warnings"] = json!(warnings);
    }
    Json(body).into_response()
}

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

/// Loki times arrive as nanosecond epoch integers, float seconds, or
/// RFC3339 strings.
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
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| (dt.unix_timestamp_nanos() / 1_000_000) as i64)
}

/// A time param that is present but unparseable is a 400, not a silent
/// fallback to the default window.
fn time_param(
    params: &HashMap<String, Vec<String>>,
    key: &str,
    default: i64,
) -> Result<i64, Response> {
    match first(params, key) {
        None => Ok(default),
        Some(raw) => parse_time_ms(raw)
            .ok_or_else(|| bad_request(format!("invalid '{key}' timestamp: {raw:?}"))),
    }
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

// ---------- selector resolution ----------

/// Loki rejects selectors where every matcher is "empty-compatible"
/// (would match the empty string) — such a query fans out to everything.
fn validate_selector(selector: &LogSelector) -> Result<(), QueryError> {
    for matcher in &selector.matchers {
        match matcher.op {
            MatchOp::Eq if !matcher.value.is_empty() => return Ok(()),
            MatchOp::Re => {
                if let Ok(re) = anchored(&matcher.value)
                    && !re.is_match("")
                {
                    return Ok(());
                }
            }
            _ => {}
        }
    }
    Err(QueryError::User(
        "queries require at least one matcher that does not match the empty string \
         (e.g. {service_name=~\".+\"})"
            .to_string(),
    ))
}

/// Streams a selector's `service_name` matchers allow.
async fn resolve_streams(
    state: &AppState,
    selector: &LogSelector,
) -> Result<Vec<String>, QueryError> {
    let all = state
        .cached_stream_names()
        .await
        .map_err(|e| QueryError::Backend(format!("stream listing failed: {e}")))?;
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
fn anchored(pattern: &str) -> Result<regex::Regex, QueryError> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|e| QueryError::User(format!("invalid regex: {e}")))
}

/// Distinct values of a label field within one stream (terms agg),
/// bounded to the query's time range. `saturated` is set when the agg
/// hit [`LABEL_VALUES_LIMIT`] — callers that need completeness (regex
/// matcher expansion) must treat that as an error, not silence.
async fn field_values(
    state: &AppState,
    stream: &str,
    field: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> Result<(Vec<String>, bool), QueryError> {
    let Some(service) = state.search.clone() else {
        return Err(QueryError::Backend(
            "this node does not run the search role".to_string(),
        ));
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
    let request = SearchRequest::parse(stream, &body).map_err(QueryError::from_search)?;
    let result = service.search(request).await.map_err(QueryError::from_search)?;
    let values: Vec<String> = result["aggregations"]["v"]["buckets"]
        .as_array()
        .map(|buckets| {
            buckets
                .iter()
                .filter_map(|b| b["key"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let saturated = values.len() >= LABEL_VALUES_LIMIT;
    Ok((values, saturated))
}

/// Build the ES bool query for a selector's label matchers against one
/// stream (line filters are applied later, against rendered lines).
/// Returns None when a matcher can never match in this stream.
async fn selector_query(
    state: &AppState,
    stream: &str,
    selector: &LogSelector,
    start_ms: i64,
    end_ms: i64,
) -> Result<Option<Value>, QueryError> {
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
                let (values, saturated) =
                    field_values(state, stream, &matcher.label, Some(start_ms), Some(end_ms))
                        .await?;
                if saturated {
                    return Err(QueryError::User(format!(
                        "label '{}' has more than {LABEL_VALUES_LIMIT} distinct values in \
                         the query range; regex matchers cannot be applied — narrow the \
                         time range or use =/!= matchers",
                        matcher.label
                    )));
                }
                let values: Vec<String> =
                    values.into_iter().filter(|v| re.is_match(v)).collect();
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

    Ok(Some(json!({"bool": {"filter": filter, "must_not": must_not}})))
}

// ---------- line filters (post-fetch, true substring/regex) ----------

enum CompiledFilter {
    Contains(String),
    NotContains(String),
    Regex(regex::Regex),
    NotRegex(regex::Regex),
}

fn compile_filters(filters: &[LineFilter]) -> Result<Vec<CompiledFilter>, QueryError> {
    filters
        .iter()
        .map(|f| {
            Ok(match f.op {
                FilterOp::Contains => CompiledFilter::Contains(f.text.clone()),
                FilterOp::NotContains => CompiledFilter::NotContains(f.text.clone()),
                // Line-filter regexes are unanchored in Loki.
                FilterOp::Regex => CompiledFilter::Regex(
                    regex::Regex::new(&f.text)
                        .map_err(|e| QueryError::User(format!("invalid line regex: {e}")))?,
                ),
                FilterOp::NotRegex => CompiledFilter::NotRegex(
                    regex::Regex::new(&f.text)
                        .map_err(|e| QueryError::User(format!("invalid line regex: {e}")))?,
                ),
            })
        })
        .collect()
}

fn line_matches(line: &str, filters: &[CompiledFilter]) -> bool {
    filters.iter().all(|f| match f {
        CompiledFilter::Contains(text) => line.contains(text.as_str()),
        CompiledFilter::NotContains(text) => !line.contains(text.as_str()),
        CompiledFilter::Regex(re) => re.is_match(line),
        CompiledFilter::NotRegex(re) => !re.is_match(line),
    })
}

// ---------- log queries ----------

struct LogHit {
    ts_ms: i64,
    labels: BTreeMap<String, String>,
    line: String,
}

struct LogQueryOutcome {
    hits: Vec<LogHit>,
    /// Per-stream failures (partial results) and scan-window saturation.
    warnings: Vec<String>,
}

/// Run a log selector across its streams: fetch selector-matching docs,
/// apply line filters to the rendered lines, merge with direction, and
/// truncate to `limit`. Individual stream failures degrade to warnings;
/// the query only errors when every stream fails.
async fn run_log_query(
    state: &AppState,
    selector: &LogSelector,
    start_ms: i64,
    end_ms: i64,
    limit: usize,
    forward: bool,
) -> Result<LogQueryOutcome, QueryError> {
    use futures::stream::{self, StreamExt};
    let service = state
        .search
        .clone()
        .ok_or_else(|| QueryError::Backend("this node does not run the search role".into()))?;
    let streams = resolve_streams(state, selector).await?;
    let filters = compile_filters(&selector.filters)?;
    let filters = &filters;
    // Line filters are applied after the fetch, so filtered queries
    // over-fetch: examine up to SCAN_LIMIT selector-matching docs per
    // stream (newest/oldest per direction) and filter those.
    let fetch = if filters.is_empty() { limit.min(MAX_LIMIT) } else { SCAN_LIMIT };
    let order = if forward { "asc" } else { "desc" };

    let futs: Vec<_> = streams
        .iter()
        .map(|stream| {
            let service = service.clone();
            let stream = stream.clone();
            async move {
                let run = async {
                    let stream = stream.as_str();
                    let Some(query) =
                        selector_query(state, stream, selector, start_ms, end_ms).await?
                    else {
                        return Ok::<_, QueryError>((Vec::new(), false));
                    };
                    let fields = state.cached_label_fields(stream).await;
                    let body = json!({
                        "size": fetch,
                        "track_total_hits": false,
                        "sort": [{"@timestamp": {"order": order}}],
                        "query": query,
                    });
                    let request =
                        SearchRequest::parse(stream, &body).map_err(QueryError::from_search)?;
                    let result = match service.search(request).await {
                        Ok(result) => result,
                        // A stream vanishing mid-query is not an error.
                        Err(rsearch_search::SearchError::Metastore(
                            rsearch_metastore::MetastoreError::StreamNotFound(_),
                        )) => return Ok((Vec::new(), false)),
                        Err(e) => return Err(QueryError::from_search(e)),
                    };
                    let raw = result["hits"]["hits"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    let scanned = raw.len();
                    let hits: Vec<LogHit> = raw
                        .into_iter()
                        .filter_map(|hit| {
                            let ts_ms = hit["sort"][0].as_i64()?;
                            let source = &hit["_source"];
                            let line = source
                                .get("message")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                                .unwrap_or_else(|| source.to_string());
                            if !line_matches(&line, filters) {
                                return None;
                            }
                            let mut labels = BTreeMap::new();
                            labels.insert("service_name".to_string(), stream.to_string());
                            for field in fields.iter() {
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
                        .collect();
                    // Saturated scan: matches may exist beyond the window.
                    let saturated = !filters.is_empty() && scanned >= fetch;
                    Ok((hits, saturated))
                };
                let result = run.await;
                (stream, result)
            }
        })
        .collect();

    let per_stream: Vec<(String, Result<(Vec<LogHit>, bool), QueryError>)> = stream::iter(futs)
        .buffer_unordered(STREAM_CONCURRENCY)
        .collect()
        .await;

    let mut hits: Vec<LogHit> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut failures = 0usize;
    let total = per_stream.len();
    let mut first_error: Option<QueryError> = None;
    for (stream, result) in per_stream {
        match result {
            Ok((stream_hits, saturated)) => {
                if saturated {
                    warnings.push(format!(
                        "stream '{stream}': line filters examined only the \
                         {SCAN_LIMIT} most recent matching docs; narrow the time \
                         range for complete results"
                    ));
                }
                hits.extend(stream_hits);
            }
            Err(e) => {
                failures += 1;
                warnings.push(format!("stream '{stream}' failed: {}", e.message()));
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }
    if failures == total
        && let Some(error) = first_error
    {
        return Err(error);
    }
    if forward {
        hits.sort_by_key(|h| h.ts_ms);
    } else {
        hits.sort_by_key(|h| std::cmp::Reverse(h.ts_ms));
    }
    hits.truncate(limit);
    Ok(LogQueryOutcome { hits, warnings })
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

/// Series under construction: label set + (bucket ts → count). Buckets
/// are at gcd(step, range) granularity; step points are derived after.
type SeriesMap = BTreeMap<String, (BTreeMap<String, String>, BTreeMap<i64, f64>)>;

fn gcd(a: i64, b: i64) -> i64 {
    let (mut a, mut b) = (a.max(1), b.max(1));
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

struct MetricOutcome {
    series: SeriesMap,
    bucket_ms: i64,
    warnings: Vec<String>,
}

/// Collect per-bucket counts for a metric query. Buckets come from a
/// date_histogram when there are no line filters, or from scanning and
/// bucketing rendered lines when there are (same honesty trade-off as
/// log queries — the scan is bounded and saturation is surfaced).
async fn run_metric_query(
    state: &AppState,
    metric: &MetricQuery,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<MetricOutcome, QueryError> {
    use futures::stream::{self, StreamExt};
    if metric.group_by.len() > 1 {
        return Err(QueryError::User(
            "sum by (…) supports at most one label".to_string(),
        ));
    }
    let group_label = metric.group_by.first().map(String::as_str);
    // Sliding windows: buckets at gcd granularity so every step point's
    // trailing range window is an exact sum of whole buckets.
    let bucket_ms = gcd(step_ms, metric.range_millis);
    if (end_ms - start_ms + metric.range_millis) / bucket_ms > MAX_METRIC_BUCKETS {
        return Err(QueryError::User(format!(
            "step/range combination produces too many buckets (limit {MAX_METRIC_BUCKETS}); \
             use a coarser step"
        )));
    }
    // The first step point's window reaches back before `start`.
    let fetch_start_ms = start_ms - metric.range_millis;

    if !metric.selector.filters.is_empty() {
        // Filtered metrics: fetch matching lines and bucket in-process.
        let outcome = run_log_query(
            state,
            &metric.selector,
            fetch_start_ms,
            end_ms,
            usize::MAX,
            false,
        )
        .await?;
        let mut series: SeriesMap = BTreeMap::new();
        for hit in outcome.hits {
            let labels = series_labels(metric, group_label, &hit.labels);
            // Grouping by a label the doc lacks drops the sample, like a
            // terms agg would.
            let Some(labels) = labels else { continue };
            let bucket = hit.ts_ms.div_euclid(bucket_ms) * bucket_ms;
            let key = format!("{labels:?}");
            let entry = series.entry(key).or_insert_with(|| (labels, BTreeMap::new()));
            *entry.1.entry(bucket).or_insert(0.0) += 1.0;
        }
        return Ok(MetricOutcome {
            series,
            bucket_ms,
            warnings: outcome.warnings,
        });
    }

    let service = state
        .search
        .clone()
        .ok_or_else(|| QueryError::Backend("this node does not run the search role".into()))?;
    let streams = resolve_streams(state, &metric.selector).await?;
    let futs: Vec<_> = streams
        .iter()
        .map(|stream| {
            let service = service.clone();
            let stream = stream.clone();
            async move {
                let run = async {
                    let stream = stream.as_str();
                    let Some(query) = selector_query(
                        state,
                        stream,
                        &metric.selector,
                        fetch_start_ms,
                        end_ms,
                    )
                    .await?
                    else {
                        return Ok::<_, QueryError>(Vec::new());
                    };
                    let histogram = json!({
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": format!("{bucket_ms}ms"),
                        },
                    });
                    let aggs = match group_label {
                        Some(label) if label != "service_name" => {
                            let mut ts = histogram;
                            ts["aggs"] = json!({
                                "by": {"terms": {"field": label, "size": LABEL_VALUES_LIMIT}}
                            });
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
                    let request =
                        SearchRequest::parse(stream, &body).map_err(QueryError::from_search)?;
                    let result = match service.search(request).await {
                        Ok(result) => result,
                        Err(rsearch_search::SearchError::Metastore(
                            rsearch_metastore::MetastoreError::StreamNotFound(_),
                        )) => return Ok(Vec::new()),
                        Err(e) => return Err(QueryError::from_search(e)),
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
                                for sub in
                                    bucket["by"]["buckets"].as_array().into_iter().flatten()
                                {
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
            .map(|(stream, result)| (stream, result.map_err(|e: QueryError| e.message().to_string())))
            .collect()
            .await;

    let mut series: SeriesMap = BTreeMap::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut failures = 0usize;
    let total = per_stream.len();
    for (stream, result) in per_stream {
        let samples = match result {
            Ok(samples) => samples,
            Err(message) => {
                failures += 1;
                warnings.push(format!("stream '{stream}' failed: {message}"));
                continue;
            }
        };
        for (ts, group_value, count) in samples {
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
            *entry.1.entry(ts).or_insert(0.0) += count;
        }
    }
    if failures == total && total > 0 {
        return Err(QueryError::Backend(
            warnings.first().cloned().unwrap_or_else(|| "all streams failed".into()),
        ));
    }
    Ok(MetricOutcome {
        series,
        bucket_ms,
        warnings,
    })
}

/// The series labels a scanned hit contributes to, honoring sum-by.
fn series_labels(
    metric: &MetricQuery,
    group_label: Option<&str>,
    hit_labels: &BTreeMap<String, String>,
) -> Option<BTreeMap<String, String>> {
    match group_label {
        Some(label) => hit_labels
            .get(label)
            .map(|value| BTreeMap::from([(label.to_string(), value.clone())])),
        None if metric.summed => Some(BTreeMap::new()),
        None => Some(BTreeMap::from([(
            "service_name".to_string(),
            hit_labels.get("service_name").cloned().unwrap_or_default(),
        )])),
    }
}

/// Turn per-bucket counts into per-step samples: each step point T sums
/// the trailing `range` window of buckets — matching Loki, where
/// `count_over_time({…}[5m])` at T counts [T-5m, T) regardless of step.
fn to_step_points(
    metric: &MetricQuery,
    outcome: MetricOutcome,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)> {
    let divisor = match metric.op {
        MetricOp::Rate => (metric.range_millis.max(1) as f64) / 1000.0,
        MetricOp::CountOverTime => 1.0,
    };
    let bucket_ms = outcome.bucket_ms;
    // Single evaluation point (instant queries, volume): the whole
    // range-filtered query IS the window, so the exact total is simply
    // every bucket — no alignment concerns.
    let single_point = start_ms == end_ms;
    outcome
        .series
        .into_values()
        .map(|(labels, buckets)| {
            let mut points = Vec::new();
            if single_point {
                let sum: f64 = buckets.values().sum();
                if sum > 0.0 {
                    points.push((start_ms, sum / divisor));
                }
                return (labels, points);
            }
            let mut t = start_ms;
            while t <= end_ms {
                // Histogram buckets are epoch-aligned to bucket_ms, but T
                // follows the caller's step grid — snap the window to the
                // bucket grid so it always sums whole buckets. The window
                // effectively evaluates at the nearest bucket boundary at
                // or before T (skew < bucket_ms ≤ step).
                let t_aligned = t.div_euclid(bucket_ms) * bucket_ms;
                let sum: f64 = buckets
                    .range(t_aligned - metric.range_millis..t_aligned)
                    .map(|(_, v)| v)
                    .sum();
                if sum > 0.0 {
                    points.push((t, sum / divisor));
                }
                t += step_ms;
            }
            (labels, points)
        })
        .filter(|(_, points)| !points.is_empty())
        .collect()
}

fn to_matrix_result(series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>) -> Value {
    let result: Vec<Value> = series
        .into_iter()
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
        return bad_request("missing 'query' parameter");
    };
    let end_ms = match time_param(&params, "end", now_ms()) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let start_ms = match time_param(&params, "start", end_ms - 3_600_000) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let limit = first(&params, "limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);
    let forward = first(&params, "direction") == Some("forward");

    match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => {
            if let Err(e) = validate_selector(&selector) {
                return e.into_response();
            }
            match run_log_query(&state, &selector, start_ms, end_ms, limit, forward).await {
                Ok(outcome) => {
                    let warnings = outcome.warnings.clone();
                    success(to_streams_result(outcome.hits), warnings)
                }
                Err(e) => e.into_response(),
            }
        }
        Ok(LogQlQuery::Metric(metric)) => {
            if let Err(e) = validate_selector(&metric.selector) {
                return e.into_response();
            }
            let step_ms = first(&params, "step")
                .and_then(parse_step_ms)
                .unwrap_or_else(|| ((end_ms - start_ms) / 250).max(1000));
            match run_metric_query(&state, &metric, start_ms, end_ms, step_ms).await {
                Ok(outcome) => {
                    let warnings = outcome.warnings.clone();
                    let series = to_step_points(&metric, outcome, start_ms, end_ms, step_ms);
                    success(to_matrix_result(series), warnings)
                }
                Err(e) => e.into_response(),
            }
        }
        Err(e) => bad_request(format!("LogQL parse error: {e}")),
    }
}

/// GET/POST /loki/api/v1/query — instant query.
pub async fn query_instant(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let Some(query) = first(&params, "query") else {
        return bad_request("missing 'query' parameter");
    };
    let time_ms = match time_param(&params, "time", now_ms()) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let limit = first(&params, "limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .min(MAX_LIMIT);

    match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => {
            if let Err(e) = validate_selector(&selector) {
                return e.into_response();
            }
            // Instant log query: last `limit` lines up to `time`.
            match run_log_query(&state, &selector, time_ms - 3_600_000, time_ms, limit, false)
                .await
            {
                Ok(outcome) => {
                    let warnings = outcome.warnings.clone();
                    success(to_streams_result(outcome.hits), warnings)
                }
                Err(e) => e.into_response(),
            }
        }
        Ok(LogQlQuery::Metric(metric)) => {
            if let Err(e) = validate_selector(&metric.selector) {
                return e.into_response();
            }
            // One step point at `time` (its window is [time-range, time]).
            match run_metric_query(&state, &metric, time_ms, time_ms, metric.range_millis.max(1))
                .await
            {
                Ok(outcome) => {
                    let warnings = outcome.warnings.clone();
                    let series =
                        to_step_points(&metric, outcome, time_ms, time_ms, metric.range_millis.max(1));
                    let result: Vec<Value> = series
                        .into_iter()
                        .map(|(labels, points)| {
                            let total: f64 = points.last().map(|(_, v)| *v).unwrap_or(0.0);
                            json!({
                                "metric": labels,
                                "value": [time_ms as f64 / 1000.0, format!("{total}")],
                            })
                        })
                        .collect();
                    success(
                        json!({"resultType": "vector", "result": result, "stats": {}}),
                        warnings,
                    )
                }
                Err(e) => e.into_response(),
            }
        }
        Err(e) => bad_request(format!("LogQL parse error: {e}")),
    }
}

/// GET /loki/api/v1/labels
pub async fn labels(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    use futures::stream::{self, StreamExt};
    let _ = params(&uri, &body); // start/end accepted but unused
    let streams = match state.cached_stream_names().await {
        Ok(streams) => streams,
        Err(e) => return QueryError::Backend(e.to_string()).into_response(),
    };
    let futs: Vec<_> = streams
        .iter()
        .map(|stream| {
            let state = &state;
            async move { state.cached_label_fields(stream).await }
        })
        .collect();
    let per_stream: Vec<_> = stream::iter(futs)
        .buffer_unordered(STREAM_CONCURRENCY)
        .collect()
        .await;
    let mut labels: Vec<String> = vec!["service_name".to_string()];
    for fields in per_stream {
        for field in fields.iter() {
            if !labels.contains(field) {
                labels.push(field.clone());
            }
        }
    }
    labels.sort();
    success(json!(labels), Vec::new())
}

/// GET /loki/api/v1/label/{name}/values
pub async fn label_values(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    uri: Uri,
    body: String,
) -> Response {
    use futures::stream::{self, StreamExt};
    let params = params(&uri, &body);
    let start_ms = match first(&params, "start").map(parse_time_ms) {
        Some(None) => return bad_request("invalid 'start' timestamp"),
        other => other.flatten(),
    };
    let end_ms = match first(&params, "end").map(parse_time_ms) {
        Some(None) => return bad_request("invalid 'end' timestamp"),
        other => other.flatten(),
    };
    let streams = match state.cached_stream_names().await {
        Ok(streams) => streams,
        Err(e) => return QueryError::Backend(e.to_string()).into_response(),
    };
    if name == "service_name" {
        let mut names: Vec<String> = streams.iter().cloned().collect();
        names.sort();
        return success(json!(names), Vec::new());
    }
    let name = &name;
    let futs: Vec<_> = streams
        .iter()
        .map(|stream| {
            let state = &state;
            async move {
                if !state.cached_label_fields(stream).await.contains(name) {
                    return Vec::new();
                }
                // Values endpoint tolerates the cap (it is a browse
                // surface); errors degrade to empty per stream.
                match field_values(state, stream, name, start_ms, end_ms).await {
                    Ok((values, _)) => values,
                    Err(_) => Vec::new(),
                }
            }
        })
        .collect();
    let per_stream: Vec<Vec<String>> = stream::iter(futs)
        .buffer_unordered(STREAM_CONCURRENCY)
        .collect()
        .await;
    let mut values: Vec<String> = Vec::new();
    for stream_values in per_stream {
        for value in stream_values {
            if !values.contains(&value) {
                values.push(value);
            }
        }
    }
    values.sort();
    values.truncate(LABEL_VALUES_LIMIT);
    success(json!(values), Vec::new())
}

/// GET/POST /loki/api/v1/series
pub async fn series(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    use futures::stream::{self, StreamExt};
    let params = params(&uri, &body);
    let end_ms = match time_param(&params, "end", now_ms()) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let start_ms = match time_param(&params, "start", end_ms - 3_600_000) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let selectors: Vec<&String> = params
        .get("match[]")
        .into_iter()
        .flatten()
        .chain(params.get("match").into_iter().flatten())
        .collect();
    let mut sets: Vec<Value> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for raw in selectors {
        let selector = match logql::parse(raw) {
            Ok(LogQlQuery::Log(selector)) => selector,
            Ok(LogQlQuery::Metric(_)) => {
                return bad_request(format!("series match must be a log selector: {raw}"));
            }
            Err(e) => return bad_request(format!("invalid series match {raw:?}: {e}")),
        };
        let streams = match resolve_streams(&state, &selector).await {
            Ok(streams) => streams,
            Err(e) => return e.into_response(),
        };
        let has_field_matchers =
            selector.matchers.iter().any(|m| m.label != "service_name");
        // With only service_name matchers the stream list is the answer;
        // otherwise probe each stream for at least one matching doc.
        let matched: Vec<String> = if !has_field_matchers {
            streams
        } else {
            let selector = &selector;
            let futs: Vec<_> = streams
                .into_iter()
                .map(|stream| {
                    let state = &state;
                    async move {
                        let outcome = run_log_query(
                            state,
                            &LogSelector {
                                matchers: selector
                                    .matchers
                                    .iter()
                                    .filter(|m| m.label != "service_name")
                                    .cloned()
                                    .chain(std::iter::once(logql::LabelMatcher {
                                        label: "service_name".to_string(),
                                        op: MatchOp::Eq,
                                        value: stream.clone(),
                                    }))
                                    .collect(),
                                filters: Vec::new(),
                            },
                            start_ms,
                            end_ms,
                            1,
                            false,
                        )
                        .await;
                        match outcome {
                            Ok(o) if !o.hits.is_empty() => Some(stream),
                            _ => None,
                        }
                    }
                })
                .collect();
            stream::iter(futs)
                .buffer_unordered(STREAM_CONCURRENCY)
                .collect::<Vec<Option<String>>>()
                .await
                .into_iter()
                .flatten()
                .collect()
        };
        for stream in matched {
            if !seen.contains(&stream) {
                sets.push(json!({"service_name": stream}));
                seen.push(stream);
            }
        }
    }
    success(json!(sets), Vec::new())
}

/// Shared selector + group-label extraction for the volume endpoints.
/// Only one grouping label is supported; extras become a warning.
fn volume_grouping(
    params: &HashMap<String, Vec<String>>,
    selector: &LogSelector,
) -> (String, Vec<String>) {
    let mut warnings = Vec::new();
    let requested: Vec<&str> = first(params, "targetLabels")
        .or_else(|| first(params, "aggregateBy"))
        .map(|l| {
            l.split(',')
                .map(str::trim)
                .filter(|l| !l.is_empty() && *l != "none")
                .collect()
        })
        .unwrap_or_default();
    if requested.len() > 1 {
        warnings.push(format!(
            "volume grouping supports one label; grouping by '{}' and ignoring {:?}",
            requested[0],
            &requested[1..]
        ));
    }
    let label = requested.first().map(|l| l.to_string()).unwrap_or_else(|| {
        selector
            .matchers
            .iter()
            // Only positive matchers make a sensible grouping default.
            .find(|m| matches!(m.op, MatchOp::Eq | MatchOp::Re))
            .map(|m| m.label.clone())
            .unwrap_or_else(|| "service_name".to_string())
    });
    (label, warnings)
}

/// GET/POST /loki/api/v1/index/volume — total counts per label set.
pub async fn volume(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let Some(query) = first(&params, "query") else {
        return bad_request("missing 'query' parameter");
    };
    let end_ms = match time_param(&params, "end", now_ms()) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let start_ms = match time_param(&params, "start", end_ms - 3_600_000) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let selector = match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => selector,
        Ok(LogQlQuery::Metric(metric)) => metric.selector,
        Err(e) => return bad_request(format!("LogQL parse error: {e}")),
    };
    if let Err(e) = validate_selector(&selector) {
        return e.into_response();
    }
    let (group_label, mut warnings) = volume_grouping(&params, &selector);
    let range = (end_ms - start_ms).max(1);
    let metric = MetricQuery {
        selector,
        range_millis: range,
        op: MetricOp::CountOverTime,
        group_by: vec![group_label],
        summed: true,
    };
    match run_metric_query(&state, &metric, end_ms, end_ms, range).await {
        Ok(outcome) => {
            warnings.extend(outcome.warnings.clone());
            let series = to_step_points(&metric, outcome, end_ms, end_ms, range);
            let mut result: Vec<Value> = series
                .into_iter()
                .map(|(labels, points)| {
                    let total: f64 = points.last().map(|(_, v)| *v).unwrap_or(0.0);
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
            success(
                json!({"resultType": "vector", "result": result, "stats": {}}),
                warnings,
            )
        }
        Err(e) => e.into_response(),
    }
}

/// GET/POST /loki/api/v1/index/volume_range — volume over time (matrix).
pub async fn volume_range(State(state): State<AppState>, uri: Uri, body: String) -> Response {
    let params = params(&uri, &body);
    let Some(query) = first(&params, "query") else {
        return bad_request("missing 'query' parameter");
    };
    let end_ms = match time_param(&params, "end", now_ms()) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let start_ms = match time_param(&params, "start", end_ms - 3_600_000) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let step_ms = first(&params, "step")
        .and_then(parse_step_ms)
        .unwrap_or_else(|| ((end_ms - start_ms) / 100).max(1000));
    let selector = match logql::parse(query) {
        Ok(LogQlQuery::Log(selector)) => selector,
        Ok(LogQlQuery::Metric(metric)) => metric.selector,
        Err(e) => return bad_request(format!("LogQL parse error: {e}")),
    };
    if let Err(e) = validate_selector(&selector) {
        return e.into_response();
    }
    let (group_label, mut warnings) = volume_grouping(&params, &selector);
    let metric = MetricQuery {
        selector,
        range_millis: step_ms,
        op: MetricOp::CountOverTime,
        group_by: vec![group_label],
        summed: true,
    };
    match run_metric_query(&state, &metric, start_ms, end_ms, step_ms).await {
        Ok(outcome) => {
            warnings.extend(outcome.warnings.clone());
            let series = to_step_points(&metric, outcome, start_ms, end_ms, step_ms);
            success(to_matrix_result(series), warnings)
        }
        Err(e) => e.into_response(),
    }
}

// ---------- tail ----------

/// Live concurrent tail sessions on this node.
static TAIL_SESSIONS: AtomicUsize = AtomicUsize::new(0);

struct TailSlot;

impl TailSlot {
    fn acquire() -> Option<TailSlot> {
        let mut current = TAIL_SESSIONS.load(Ordering::Relaxed);
        loop {
            if current >= MAX_TAIL_SESSIONS {
                return None;
            }
            match TAIL_SESSIONS.compare_exchange(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(TailSlot),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for TailSlot {
    fn drop(&mut self) {
        TAIL_SESSIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// GET /loki/api/v1/tail — WebSocket live tail, implemented as a 1s poll
/// with a trailing re-scan window.
pub async fn tail(
    State(state): State<AppState>,
    ws: axum::extract::ws::WebSocketUpgrade,
    uri: Uri,
) -> Response {
    let params = params(&uri, "");
    let Some(query) = first(&params, "query").map(str::to_string) else {
        return bad_request("missing 'query' parameter");
    };
    let selector = match logql::parse(&query) {
        Ok(LogQlQuery::Log(selector)) => selector,
        Ok(LogQlQuery::Metric(_)) => return bad_request("tail requires a log selector"),
        Err(e) => return bad_request(format!("LogQL parse error: {e}")),
    };
    if let Err(e) = validate_selector(&selector) {
        return e.into_response();
    }
    let Some(_slot) = TailSlot::acquire() else {
        return QueryError::Backend(format!(
            "too many concurrent tail sessions (limit {MAX_TAIL_SESSIONS})"
        ))
        .into_response();
    };
    // Docs become searchable only once their batch publishes a split
    // (up to ingest.max_batch_secs after arrival), so a hard cursor at
    // "now" would skip everything. Instead each tick re-scans a trailing
    // window and dedupes what it already emitted; the watermark advances
    // once entries are old enough that late publishes are no longer
    // expected. Emission latency is one publish + one poll tick. Dedup
    // hashes (ts, line, labels) — identical duplicate lines within one
    // millisecond collapse to one emission; an accepted trade-off of the
    // re-scan design.
    const TAIL_RESCAN_MS: i64 = 60_000;
    const TAIL_SEEN_CAP: usize = 100_000;
    let mut watermark_ms = match first(&params, "start").map(parse_time_ms) {
        Some(None) => return bad_request("invalid 'start' timestamp"),
        Some(Some(start)) => start,
        None => now_ms() - 5_000,
    };

    ws.on_upgrade(move |mut socket| async move {
        use std::hash::{Hash, Hasher};
        let _slot = _slot; // owned by the session; frees the slot on exit
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        // Detect half-open peers (client vanished without a close frame):
        // periodic pings force the socket to surface the failure.
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(TAIL_MAX_SESSION_SECS);
        loop {
            tokio::select! {
                message = socket.recv() => {
                    match message {
                        None | Some(Err(_)) => return,
                        Some(Ok(axum::extract::ws::Message::Close(_))) => return,
                        Some(Ok(_)) => {} // pongs and client chatter
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let _ = socket.send(axum::extract::ws::Message::Close(None)).await;
                    return;
                }
                _ = ping_interval.tick() => {
                    if socket
                        .send(axum::extract::ws::Message::Ping(Vec::new().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                _ = interval.tick() => {
                    let end_ms = now_ms();
                    if end_ms <= watermark_ms {
                        continue;
                    }
                    // Fetch newest-first with the full scan budget (not the
                    // client display limit) so a busy window doesn't starve
                    // recent lines behind old ones.
                    let outcome = match run_log_query(
                        &state, &selector, watermark_ms, end_ms, SCAN_LIMIT, false,
                    ).await {
                        Ok(outcome) => outcome,
                        Err(_) => continue, // transient; retry next tick
                    };
                    let saturated = outcome.hits.len() >= SCAN_LIMIT;
                    let mut fresh: Vec<LogHit> = outcome.hits
                        .into_iter()
                        .filter(|hit| {
                            let mut hasher =
                                std::collections::hash_map::DefaultHasher::new();
                            (hit.ts_ms, &hit.line, &hit.labels).hash(&mut hasher);
                            seen.insert(hasher.finish())
                        })
                        .collect();
                    fresh.sort_by_key(|h| h.ts_ms);
                    if saturated {
                        tracing::warn!(
                            query = %query,
                            "tail window saturated; oldest lines in the window may be skipped"
                        );
                    }
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
