use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use rsearch_metastore::{MetastoreError, StreamMode};
use rsearch_search::{SearchError, SearchRequest};

use crate::state::AppState;

/// POST/GET /{index}/_search (`?scroll=<keep-alive>` opens a scroll).
pub async fn search(
    State(state): State<AppState>,
    Path(index): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: String,
) -> Response {
    let Some(service) = state.search.clone() else {
        return es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "this node does not run the search role",
        );
    };
    let mut body_json: Value = if body.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                return es_error(
                    StatusCode::BAD_REQUEST,
                    "parse_exception",
                    &format!("invalid search body: {e}"),
                );
            }
        }
    };
    let keep_alive = match params.get("scroll").map(|s| parse_keep_alive(s)) {
        None => None,
        Some(Ok(secs)) => Some(secs),
        Some(Err(response)) => return response,
    };
    if keep_alive.is_some() {
        // OpenSearch's validations for a scroll context.
        if body_json.get("from").and_then(Value::as_u64).unwrap_or(0) != 0 {
            return es_error(
                StatusCode::BAD_REQUEST,
                "action_request_validation_exception",
                "Validation Failed: 1: using [from] is not allowed in a scroll context;",
            );
        }
        if body_json.get("search_after").is_some_and(|v| !v.is_null()) {
            return es_error(
                StatusCode::BAD_REQUEST,
                "search_exception",
                "`search_after` cannot be used in a scroll context.",
            );
        }
        // `_doc` order is the scroll idiom: our stable (timestamp, _seq)
        // sequence serves it; a `size` must survive into every page.
        if !body_json.is_object() {
            body_json = json!({});
        }
    }
    let request = match SearchRequest::parse(&index, &body_json) {
        Ok(request) => request,
        Err(e) => return map_search_error(e),
    };
    let mut response = match service.search(request).await {
        Ok(response) => response,
        Err(e) => return map_search_error(e),
    };
    if let Some(keep_alive) = keep_alive {
        // Later pages re-run the search from the last hit's cursor;
        // aggregations belong to the first page only, as in OpenSearch.
        let mut stored = body_json.clone();
        if let Some(map) = stored.as_object_mut() {
            map.remove("aggs");
            map.remove("aggregations");
        }
        let cursor = last_hit_sort(&response);
        let total = response["hits"]["total"].clone();
        let id = uuid::Uuid::new_v4().simple().to_string();
        if let Err(e) = state
            .metastore
            .create_scroll(&id, &index, &stored, cursor.as_ref(), &total, keep_alive)
            .await
        {
            return es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string());
        }
        response["_scroll_id"] = json!(id);
    }
    Json(response).into_response()
}

/// The `sort` values of a page's last hit — the next page's cursor.
fn last_hit_sort(response: &Value) -> Option<Value> {
    response["hits"]["hits"]
        .as_array()
        .and_then(|hits| hits.last())
        .and_then(|hit| hit.get("sort"))
        .filter(|sort| sort.is_array())
        .cloned()
}

/// Longest scroll keep-alive accepted (OpenSearch's
/// `search.max_keep_alive` default).
const MAX_KEEP_ALIVE_SECS: f64 = 24.0 * 3600.0;

/// Parse an ES time value (`1m`, `30s`, `2h`, `500ms`, `1d`) into
/// seconds; a 400 for anything else or beyond the ceiling.
fn parse_keep_alive(value: &str) -> Result<f64, Response> {
    let value = value.trim();
    let split = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let amount: f64 = match number.parse() {
        Ok(n) if number.is_empty() => n,
        Ok(n) => n,
        Err(_) => f64::NAN,
    };
    let secs = match unit {
        "ms" => amount / 1000.0,
        "s" => amount,
        "m" => amount * 60.0,
        "h" => amount * 3600.0,
        "d" => amount * 86_400.0,
        _ => f64::NAN,
    };
    if number.is_empty() || !secs.is_finite() || secs < 0.0 {
        return Err(es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            &format!(
                "failed to parse setting [scroll] with value [{value}] as a time value: \
                 unit is missing or unrecognized"
            ),
        ));
    }
    if secs > MAX_KEEP_ALIVE_SECS {
        return Err(es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            &format!(
                "Keep alive for scroll ({value}) is too large. It must be less than (1d). \
                 This limit can be set by changing the [search.max_keep_alive] cluster level setting."
            ),
        ));
    }
    Ok(secs)
}

/// Scroll ids are UUIDs in simple form: anything else cannot be ours.
fn valid_scroll_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

fn scroll_id_unparseable() -> Response {
    es_error(
        StatusCode::BAD_REQUEST,
        "illegal_argument_exception",
        "Cannot parse scroll id",
    )
}

/// Body/query parameters of a scroll continuation or clear.
struct ScrollParams {
    ids: Vec<String>,
    keep_alive: Option<Result<f64, Response>>,
}

fn scroll_params(
    params: &std::collections::HashMap<String, String>,
    body: &str,
    path_id: Option<&str>,
) -> Result<ScrollParams, Response> {
    let body_json: Value = if body.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(body) {
            Ok(v) => v,
            Err(e) => {
                return Err(es_error(
                    StatusCode::BAD_REQUEST,
                    "parse_exception",
                    &format!("invalid scroll body: {e}"),
                ));
            }
        }
    };
    let mut ids: Vec<String> = Vec::new();
    if let Some(id) = path_id {
        ids.extend(id.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()));
    }
    match body_json.get("scroll_id") {
        Some(Value::String(id)) => ids.push(id.clone()),
        Some(Value::Array(items)) => {
            ids.extend(items.iter().filter_map(Value::as_str).map(str::to_string));
        }
        _ => {}
    }
    if let Some(id) = params.get("scroll_id") {
        ids.extend(id.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()));
    }
    let keep_alive = body_json
        .get("scroll")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| params.get("scroll").cloned())
        .map(|v| parse_keep_alive(&v));
    Ok(ScrollParams { ids, keep_alive })
}

/// POST/GET /_search/scroll — the next page of a scroll (issue #72).
pub async fn scroll_next(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
    body: String,
) -> Response {
    scroll_page(state, params, identity, body, None).await
}

/// POST/GET /_search/scroll/{scroll_id}
pub async fn scroll_next_by_path(
    State(state): State<AppState>,
    Path(scroll_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
    body: String,
) -> Response {
    scroll_page(state, params, identity, body, Some(&scroll_id)).await
}

async fn scroll_page(
    state: AppState,
    params: std::collections::HashMap<String, String>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
    body: String,
    path_id: Option<&str>,
) -> Response {
    let Some(service) = state.search.clone() else {
        return es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "this node does not run the search role",
        );
    };
    let parsed = match scroll_params(&params, &body, path_id) {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };
    let Some(id) = parsed.ids.first().cloned() else {
        return es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "scroll_id is required",
        );
    };
    if !valid_scroll_id(&id) {
        return scroll_id_unparseable();
    }
    let keep_alive = match parsed.keep_alive {
        Some(Ok(secs)) => Some(secs),
        Some(Err(response)) => return response,
        None => None,
    };
    let context = match state.metastore.get_scroll(&id).await {
        Ok(Some(context)) => context,
        Ok(None) => return scroll_context_missing(&id),
        Err(e) => {
            return es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string());
        }
    };
    if let Some(axum::Extension(identity)) = &identity
        && !identity.is_admin
        && !identity.allows_stream(&context.stream)
    {
        return es_error(
            StatusCode::FORBIDDEN,
            "security_exception",
            &format!(
                "identity '{}' is not permitted to read index [{}]",
                identity.name, context.stream
            ),
        );
    }
    let mut page_body = context.request.clone();
    let mut response = match &context.cursor {
        // The first page had no hits: nothing follows.
        None => empty_scroll_page(&context),
        Some(cursor) => {
            page_body["search_after"] = cursor.clone();
            // Totals come from the first page; skipping the recount also
            // lets splits wholly behind the cursor be skipped.
            page_body["track_total_hits"] = json!(false);
            let request = match SearchRequest::parse(&context.stream, &page_body) {
                Ok(request) => request,
                Err(e) => return map_search_error(e),
            };
            match service.search(request).await {
                Ok(response) => response,
                Err(e) => return map_search_error(e),
            }
        }
    };
    response["hits"]["total"] = context.total.clone();
    let cursor = last_hit_sort(&response);
    // A continuation without `scroll` keeps the context alive only for
    // the remainder of its current window, as in OpenSearch.
    let advanced = match keep_alive {
        Some(secs) => state.metastore.advance_scroll(&id, cursor.as_ref(), secs).await,
        None => state.metastore.advance_scroll_cursor(&id, cursor.as_ref()).await,
    };
    if let Err(e) = advanced {
        return es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string());
    }
    response["_scroll_id"] = json!(id);
    Json(response).into_response()
}

fn empty_scroll_page(context: &rsearch_metastore::ScrollRecord) -> Value {
    json!({
        "took": 0,
        "timed_out": false,
        "_shards": {"total": 0, "successful": 0, "skipped": 0, "failed": 0},
        "hits": {"total": context.total, "max_score": Value::Null, "hits": []},
    })
}

fn scroll_context_missing(id: &str) -> Response {
    let reason = format!("No search context found for id [{id}]");
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": {
                "type": "search_phase_execution_exception",
                "reason": "all shards failed",
                "phase": "query",
                "grouped": true,
                "root_cause": [{"type": "search_context_missing_exception", "reason": reason}],
                "caused_by": {"type": "search_context_missing_exception", "reason": reason},
            },
            "status": 404,
        })),
    )
        .into_response()
}

/// DELETE /_search/scroll — free scroll contexts (`scroll_id` in the
/// body, a string or an array).
pub async fn clear_scroll(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
    body: String,
) -> Response {
    clear_scrolls(state, params, identity, body, None).await
}

/// DELETE /_search/scroll/{scroll_id} (`_all` frees every context).
pub async fn clear_scroll_by_path(
    State(state): State<AppState>,
    Path(scroll_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
    body: String,
) -> Response {
    clear_scrolls(state, params, identity, body, Some(&scroll_id)).await
}

/// A scoped identity frees only contexts on streams inside its scope:
/// `_all` clears its own, and a foreign id is a 403 (never a silent
/// no-op, so a tenant cannot probe for other tenants' ids).
async fn clear_scrolls(
    state: AppState,
    params: std::collections::HashMap<String, String>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
    body: String,
    path_id: Option<&str>,
) -> Response {
    let parsed = match scroll_params(&params, &body, path_id) {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };
    let scoped = identity
        .as_ref()
        .map(|axum::Extension(identity)| identity)
        .filter(|identity| !identity.is_admin && !identity.streams.iter().any(|s| s == "*"));
    let all = parsed.ids.iter().any(|id| id == "_all");
    if !all {
        if parsed.ids.is_empty() {
            return es_error(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                "scroll_id is required",
            );
        }
        if parsed.ids.iter().any(|id| !valid_scroll_id(id)) {
            return scroll_id_unparseable();
        }
    }
    let freed = match (scoped, all) {
        (None, true) => state.metastore.delete_all_scrolls().await,
        (None, false) => state.metastore.delete_scrolls(&parsed.ids).await,
        (Some(identity), all) => match state.metastore.list_scrolls().await {
            Ok(live) => {
                let mine: Vec<String> = live
                    .iter()
                    .filter(|(id, stream)| {
                        identity.allows_stream(stream) && (all || parsed.ids.contains(id))
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                if !all
                    && let Some((_, stream)) = live
                        .iter()
                        .find(|(id, stream)| parsed.ids.contains(id) && !identity.allows_stream(stream))
                {
                    return es_error(
                        StatusCode::FORBIDDEN,
                        "security_exception",
                        &format!(
                            "identity '{}' is not permitted to read index [{stream}]",
                            identity.name
                        ),
                    );
                }
                state.metastore.delete_scrolls(&mine).await
            }
            Err(e) => Err(e),
        },
    };
    match freed {
        Ok(n) => Json(json!({"succeeded": true, "num_freed": n})).into_response(),
        Err(e) => es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string()),
    }
}

/// POST /_msearch — NDJSON header/body pairs (Grafana's transport).
pub async fn msearch(State(state): State<AppState>, body: String) -> Response {
    if state.search.is_none() {
        return es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "this node does not run the search role",
        );
    }
    let service = state.search.clone().unwrap();

    let lines: Vec<&str> = body
        .split('\n')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();

    // Parse all header/body pairs first, then run them concurrently — a
    // Grafana dashboard sends one panel per pair, so serial execution made
    // dashboard load time the sum of panels instead of the max (M13).
    let mut pairs: Vec<(Value, Value)> = Vec::new();
    let mut i = 0;
    while i + 1 < lines.len() {
        let header: Value = match serde_json::from_str(lines[i]) {
            Ok(v) => v,
            Err(e) => {
                return es_error(
                    StatusCode::BAD_REQUEST,
                    "parse_exception",
                    &format!("invalid msearch header: {e}"),
                );
            }
        };
        let search_body: Value = match serde_json::from_str(lines[i + 1]) {
            Ok(v) => v,
            Err(e) => {
                return es_error(
                    StatusCode::BAD_REQUEST,
                    "parse_exception",
                    &format!("invalid msearch body: {e}"),
                );
            }
        };
        pairs.push((header, search_body));
        i += 2;
    }
    if i < lines.len() {
        return es_error(
            StatusCode::BAD_REQUEST,
            "parse_exception",
            "msearch body ends with a header line that has no body line",
        );
    }

    let futures = pairs.into_iter().map(|(header, search_body)| {
        let state = &state;
        let service = &service;
        async move {
            let index_pattern = header
                .get("index")
                .map(|v| match v {
                    Value::Array(items) => items
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(","),
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            let result = match resolve_stream(state, &index_pattern).await {
                Ok(stream) => match SearchRequest::parse(&stream, &search_body) {
                    Ok(request) => service.search(request).await.map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                },
                Err(e) => Err(e),
            };
            match result {
                Ok(mut response) => {
                    response["status"] = json!(200);
                    response
                }
                Err(reason) => json!({
                    "error": {"type": "search_exception", "reason": reason},
                    "status": 400,
                }),
            }
        }
    });
    // Bounded intra-request concurrency (ES: max_concurrent_searches).
    // join_all ran every pair at once — thousands of tiny pairs fit in
    // the body limit, and each search fans out into up to 16 blocking
    // split searches, so an unbounded request could saturate the blocking
    // pool it shares with bulk WAL appends.
    use futures::StreamExt;
    let max_concurrent = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let responses: Vec<Value> = futures::stream::iter(futures)
        .buffered(max_concurrent)
        .collect()
        .await;
    Json(json!({"took": 0, "responses": responses})).into_response()
}

/// Resolve an index expression (possibly `logs-*` style) to one stream.
async fn resolve_stream(state: &AppState, pattern: &str) -> Result<String, String> {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return Err("no index specified".to_string());
    }
    if !pattern.contains('*') {
        return Ok(pattern.to_string());
    }
    let streams = state
        .cached_stream_names()
        .await
        .map_err(|e| e.to_string())?;
    let matches: Vec<String> = streams
        .iter()
        .filter(|name| crate::auth::stream_glob_matches(pattern, name))
        .cloned()
        .collect();
    match matches.len() {
        0 => Err(format!("no stream matches '{pattern}'")),
        1 => Ok(matches.into_iter().next().unwrap()),
        n => Err(format!(
            "'{pattern}' matches {n} streams; multi-stream search is not supported yet"
        )),
    }
}

/// GET /{index}/_mapping — Grafana fetches this to discover fields.
pub async fn get_mapping(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    match state.metastore.get_stream(&index).await {
        Ok(stream) => {
            let mappings = mappings_json(&state, &stream).await;
            Json(json!({ index: {"mappings": mappings} })).into_response()
        }
        Err(MetastoreError::StreamNotFound(_)) => index_not_found(&index),
        Err(e) => es_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            &e.to_string(),
        ),
    }
}

/// GET/POST /{index}/_count — the ES `count` helper. The body may carry a
/// `query` (nothing else: OpenSearch rejects `size`, `sort`, `aggs`, …
/// with a parsing_exception), or `?q=` runs a `query_string`.
pub async fn count(
    State(state): State<AppState>,
    Path(index): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: String,
) -> Response {
    let Some(service) = state.search.clone() else {
        return es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "this node does not run the search role",
        );
    };
    let body_json: Value = if body.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                return es_error(
                    StatusCode::BAD_REQUEST,
                    "parse_exception",
                    &format!("invalid count body: {e}"),
                );
            }
        }
    };
    let Some(map) = body_json.as_object() else {
        return es_error(
            StatusCode::BAD_REQUEST,
            "parsing_exception",
            "count body must be an object",
        );
    };
    if let Some(key) = map.keys().find(|k| k.as_str() != "query") {
        return es_error(
            StatusCode::BAD_REQUEST,
            "parsing_exception",
            &format!("request does not support [{key}]"),
        );
    }
    let mut search_body = json!({"size": 0, "track_total_hits": true});
    if let Some(query) = map.get("query") {
        search_body["query"] = query.clone();
    } else if let Some(q) = params.get("q") {
        search_body["query"] = json!({"query_string": {"query": q}});
    }
    let mut request = match SearchRequest::parse(&index, &search_body) {
        Ok(request) => request,
        Err(e) => return map_search_error(e),
    };
    // Exact count, never the 10k lower bound a search reports.
    request.track_total_hits = None;
    match service.search(request).await {
        Ok(response) => {
            let count = response["hits"]["total"]["value"].clone();
            let shards = response["_shards"].clone();
            Json(json!({"count": count, "_shards": shards})).into_response()
        }
        Err(e) => map_search_error(e),
    }
}

/// PUT /{index}/_mapping — add fields to an existing index's mapping
/// (issue #73). OpenSearch semantics: new fields are added, a field that
/// already exists must keep its type (400 otherwise), and the index must
/// exist (404). The body is `{"properties": {...}}`.
pub async fn put_mapping(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: String,
) -> Response {
    let body_json: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return es_error(
                StatusCode::BAD_REQUEST,
                "parse_exception",
                &format!("invalid body: {e}"),
            );
        }
    };
    let incoming = match rsearch_index::IndexMapping::from_json(&body_json) {
        Ok(mapping) => mapping,
        Err(e) => {
            return es_error(
                StatusCode::BAD_REQUEST,
                "mapper_parsing_exception",
                &e.to_string(),
            );
        }
    };
    let stream = match state.metastore.get_stream(&index).await {
        Ok(stream) => stream,
        Err(MetastoreError::StreamNotFound(_)) => return index_not_found(&index),
        Err(e) => {
            return es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string());
        }
    };
    let existing = rsearch_index::IndexMapping::from_json(&stream.mapping).unwrap_or_default();
    // Merge: keep every existing field's declaration, add the new ones.
    // Types are compared normalized (`integer` and `long` are one type
    // here), so re-declaring a field the way it already is is a no-op.
    let mut merged = stream
        .mapping
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let incoming_props = body_json
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (name, ty) in &incoming.properties {
        match existing.properties.get(name) {
            Some(current) if current != ty => {
                let from = serde_json::to_value(current).unwrap_or(Value::Null);
                let to = serde_json::to_value(ty).unwrap_or(Value::Null);
                return es_error(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    &format!(
                        "mapper [{name}] cannot be changed from type [{}] to [{}]",
                        from.as_str().unwrap_or("?"),
                        to.as_str().unwrap_or("?")
                    ),
                );
            }
            Some(_) => {}
            None => {
                if let Some(spec) = incoming_props.get(name) {
                    merged.insert(name.clone(), spec.clone());
                }
            }
        }
    }
    let mut mapping = stream.mapping.clone();
    if !mapping.is_object() {
        mapping = json!({});
    }
    mapping["properties"] = Value::Object(merged);
    if let Err(e) = state.metastore.update_stream_mapping(&index, &mapping).await {
        return match e {
            MetastoreError::StreamNotFound(_) => index_not_found(&index),
            other => es_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &other.to_string(),
            ),
        };
    }
    // This node's search cache sees the new mapping now; other nodes
    // within their 10s stream-cache TTL. Ingest workers re-read the
    // mapping before every flush.
    for service in [&state.search, &state.doc_lookup].into_iter().flatten() {
        service.invalidate_stream(&index);
    }
    Json(json!({"acknowledged": true})).into_response()
}

/// Read the stream mode out of a `PUT /{index}` body: ES-shaped
/// `settings.index.mode`, `settings.mode`, or a top-level `mode`.
fn mode_from_body(body: &Value) -> Result<Option<StreamMode>, String> {
    let raw = body
        .pointer("/settings/index/mode")
        .or_else(|| body.pointer("/settings/mode"))
        .or_else(|| body.get("mode"));
    match raw {
        None => Ok(None),
        Some(Value::String(s)) => StreamMode::parse(s)
            .map(Some)
            .ok_or_else(|| format!("unknown index mode '{s}' (expected 'log' or 'document')")),
        Some(other) => Err(format!("index mode must be a string, got {other}")),
    }
}

/// PUT /{index} — create a stream (optionally with `settings.index.mode`)
/// or update its mapping. The mode is fixed once the stream holds data.
pub async fn put_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: String,
) -> Response {
    let body_json: Value = if body.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                return es_error(
                    StatusCode::BAD_REQUEST,
                    "parse_exception",
                    &format!("invalid body: {e}"),
                );
            }
        }
    };
    let mode = match mode_from_body(&body_json) {
        Ok(mode) => mode,
        Err(reason) => {
            return es_error(StatusCode::BAD_REQUEST, "illegal_argument_exception", &reason);
        }
    };
    let mapping = body_json.get("mappings").cloned();
    // Validate before storing.
    if let Some(mapping) = &mapping
        && let Err(e) = rsearch_index::IndexMapping::from_json(mapping)
    {
        return es_error(
            StatusCode::BAD_REQUEST,
            "mapper_parsing_exception",
            &e.to_string(),
        );
    }
    let result = async {
        let record = match mode {
            Some(mode) => state.metastore.ensure_stream_with_mode(&index, mode).await?,
            None => state.metastore.ensure_stream(&index).await?,
        };
        if let Some(mode) = mode
            && record.mode() != mode
        {
            // Existing stream with a different mode: allowed only while it
            // is still empty (e.g. auto-created by a first _bulk).
            state.metastore.set_stream_mode(&index, mode).await?;
            // This node's caches see the change now; other nodes within
            // their 10s TTLs (a mode can only change while the stream is
            // empty, so nothing written in that window is affected yet).
            if let Some(pipeline) = &state.pipeline {
                pipeline.forget_stream(&index);
            }
            for service in [&state.search, &state.doc_lookup].into_iter().flatten() {
                service.invalidate_stream(&index);
            }
        }
        if let Some(mapping) = &mapping {
            state.metastore.update_stream_mapping(&index, mapping).await?;
        }
        Ok::<_, MetastoreError>(())
    }
    .await;
    match result {
        Ok(()) => Json(json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": index,
        }))
        .into_response(),
        Err(MetastoreError::StreamModeFixed(_)) => es_error(
            StatusCode::CONFLICT,
            "illegal_argument_exception",
            &format!("index [{index}] already holds data; its mode cannot be changed"),
        ),
        Err(e) => es_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            &e.to_string(),
        ),
    }
}

/// DELETE /{index} — delete one or more indices (issue #71). The path is
/// an index expression: a name, a comma-separated list, or globs. As in
/// OpenSearch a named index that does not exist is a 404 and a glob that
/// matches nothing is acknowledged; unlike OpenSearch's default, `_all`
/// and a bare `*` are refused (`action.destructive_requires_name`).
///
/// Deletion is immediate for readers and writers — the stream's splits
/// leave the published set and the name is free to re-create at once —
/// while storage is reclaimed by the control leader's GC after its grace
/// period. Documents still buffered on an ingest node when the index is
/// deleted are flushed into the re-created index (or re-create it), the
/// way a `_bulk` to a missing index always has.
pub async fn delete_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
) -> Response {
    let expression = index.trim();
    if expression.is_empty() || expression == "_all" || expression.split(',').any(|p| p.trim() == "*") {
        return es_error(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "Wildcard expressions or all indices are not allowed",
        );
    }
    let names = match state.metastore.list_streams().await {
        Ok(streams) => streams.into_iter().map(|s| s.name).collect::<Vec<_>>(),
        Err(e) => {
            return es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string());
        }
    };
    let mut targets: Vec<String> = Vec::new();
    for part in expression.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if part.contains('*') {
            targets.extend(
                names
                    .iter()
                    .filter(|n| crate::auth::stream_glob_matches(part, n))
                    .cloned(),
            );
        } else if names.iter().any(|n| n == part) {
            targets.push(part.to_string());
        } else {
            return index_not_found(part);
        }
    }
    targets.sort();
    targets.dedup();
    // A glob or list must stay inside the caller's stream scope: the
    // middleware matched the raw expression, not what it expands to.
    if let Some(axum::Extension(identity)) = &identity
        && !identity.is_admin
        && let Some(outside) = targets.iter().find(|t| !identity.allows_stream(t))
    {
        return es_error(
            StatusCode::FORBIDDEN,
            "security_exception",
            &format!(
                "identity '{}' is not permitted to delete index [{outside}]",
                identity.name
            ),
        );
    }
    for target in &targets {
        match state.metastore.retire_stream(target).await {
            // Raced with another delete of the same name: already gone.
            Ok(_) | Err(MetastoreError::StreamNotFound(_)) => {}
            Err(e) => {
                return es_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    &e.to_string(),
                );
            }
        }
        // This node forgets the stream now; peers within their 10s
        // stream-cache TTLs (their queries see no published splits
        // meanwhile, and their ingest workers rebind at the next flush).
        if let Some(pipeline) = &state.pipeline {
            pipeline.forget_stream(target);
        }
        for service in [&state.search, &state.doc_lookup].into_iter().flatten() {
            service.invalidate_stream(target);
        }
        state.label_fields.lock().unwrap().remove(target);
        state.audit("delete_index", target, "").await;
    }
    *state.stream_names.lock().unwrap() = None;
    Json(json!({"acknowledged": true})).into_response()
}

/// ES-shaped settings block for a stream.
fn settings_json(stream: &rsearch_metastore::StreamRecord) -> Value {
    json!({
        "index": {
            "mode": stream.mode,
            "retention_hours": stream.retention_hours,
            "number_of_shards": "1",
            "number_of_replicas": "0",
        }
    })
}

/// ES-shaped mappings block for a stream: the declared properties, the
/// always-present `@timestamp`, and every unmapped field its published
/// splits hold, reported the way OpenSearch's dynamic mapping would
/// have typed it (issue #76): a string is `text` with a `keyword`
/// sub-field (`ignore_above` 256), an integer `long`, a fraction
/// `float`, a boolean `boolean`. Nested paths become nested
/// `properties`.
async fn mappings_json(state: &AppState, stream: &rsearch_metastore::StreamRecord) -> Value {
    let mut properties = stream
        .mapping
        .get("properties")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !properties.is_object() {
        properties = json!({});
    }
    let dynamic = match state.metastore.stream_dynamic_fields(stream.id).await {
        Ok(fields) => fields,
        Err(e) => {
            tracing::warn!(stream = %stream.name, error = %e, "dynamic field inventory unavailable");
            Default::default()
        }
    };
    for (path, types) in dynamic {
        let spec = dynamic_field_spec(&types);
        insert_dynamic_property(&mut properties, &path, spec);
    }
    // The timestamp field is always present and always a date.
    properties["@timestamp"] = json!({"type": "date"});
    json!({"properties": properties})
}

/// The mapping entry OpenSearch reports for a dynamic field seen with
/// these value types. A path seen as both a string and a number reports
/// as text: OpenSearch keeps whichever it saw first, and text is the
/// only view that accepts every value.
fn dynamic_field_spec(types: &[String]) -> Value {
    let has = |t: &str| types.iter().any(|x| x == t);
    if has("string") {
        json!({
            "type": "text",
            "fields": {
                "keyword": {"type": "keyword", "ignore_above": rsearch_index::KEYWORD_IGNORE_ABOVE}
            }
        })
    } else if has("double") {
        json!({"type": "float"})
    } else if has("long") {
        json!({"type": "long"})
    } else if has("boolean") {
        json!({"type": "boolean"})
    } else {
        json!({"type": "date"})
    }
}

/// Place `spec` at `path` inside a `properties` object, creating object
/// levels for `a.b.c` paths. A `\.` in the path is a literal dot in one
/// key (rSearch does not expand dots), kept as part of the name. A
/// declared property is never overwritten: the mapped field wins, and a
/// dynamic path beneath a mapped scalar is dropped.
fn insert_dynamic_property(properties: &mut Value, path: &str, spec: Value) {
    let mut segments: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut chars = path.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if chars.peek() == Some(&'.') => {
                current.push('.');
                chars.next();
            }
            '.' => segments.push(std::mem::take(&mut current)),
            other => current.push(other),
        }
    }
    segments.push(current);
    let Some((last, parents)) = segments.split_last() else { return };
    let mut node = properties;
    for parent in parents {
        let Some(map) = node.as_object_mut() else { return };
        let entry = map
            .entry(parent.clone())
            .or_insert_with(|| json!({"properties": {}}));
        if !entry.is_object() || entry.get("type").is_some() {
            // A mapped scalar field sits where an object would go.
            return;
        }
        if entry.get("properties").is_none() {
            entry["properties"] = json!({});
        }
        node = &mut entry["properties"];
    }
    if let Some(map) = node.as_object_mut()
        && !map.contains_key(last)
    {
        map.insert(last.clone(), spec);
    }
}

/// GET /{index}/_settings
pub async fn get_settings(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    match state.metastore.get_stream(&index).await {
        Ok(stream) => Json(json!({ index: {"settings": settings_json(&stream)} })).into_response(),
        Err(MetastoreError::StreamNotFound(_)) => index_not_found(&index),
        Err(e) => es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string()),
    }
}

/// GET /{index} — settings + mappings (what stock clients call to
/// discover an index).
pub async fn get_index(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    match state.metastore.get_stream(&index).await {
        Ok(stream) => {
            let mappings = mappings_json(&state, &stream).await;
            Json(json!({
                index: {
                    "aliases": {},
                    "mappings": mappings,
                    "settings": settings_json(&stream),
                }
            }))
            .into_response()
        }
        Err(MetastoreError::StreamNotFound(_)) => index_not_found(&index),
        Err(e) => es_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", &e.to_string()),
    }
}

/// HEAD /{index} — 200 if the index exists, 404 otherwise.
pub async fn head_index(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    match state.metastore.get_stream(&index).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(MetastoreError::StreamNotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub(crate) fn index_not_found(index: &str) -> Response {
    es_error(
        StatusCode::NOT_FOUND,
        "index_not_found_exception",
        &format!("no such index [{index}]"),
    )
}

/// GET / — the version handshake clients and shippers probe.
pub async fn root(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "name": state.node_id,
        "cluster_name": state.cluster_name,
        "cluster_uuid": "rsearch",
        "version": {
            // Report ES 7.17 so ES clients, Vector, Fluent Bit, and
            // Grafana (which requires >= 7.16) take their 7.x code paths.
            "number": "7.17.0",
            "build_flavor": "default",
            "build_type": "tar",
            "rsearch_version": env!("CARGO_PKG_VERSION"),
            "lucene_version": "8.11.1",
            "minimum_wire_compatibility_version": "6.8.0",
            "minimum_index_compatibility_version": "6.0.0",
        },
        "tagline": "You Know, for Search",
    }))
}

fn map_search_error(err: SearchError) -> Response {
    match &err {
        SearchError::BadRequest(reason) => {
            es_error(StatusCode::BAD_REQUEST, "illegal_argument_exception", reason)
        }
        SearchError::Metastore(MetastoreError::StreamNotFound(name)) => es_error(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            &format!("no such index [{name}]"),
        ),
        other => es_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            &other.to_string(),
        ),
    }
}

pub(crate) fn es_error(status: StatusCode, error_type: &str, reason: &str) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "type": error_type,
                "reason": reason,
                "root_cause": [{"type": error_type, "reason": reason}],
            },
            "status": status.as_u16(),
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_alive_parses_es_time_values() {
        for (input, secs) in [("1m", 60.0), ("30s", 30.0), ("2h", 7200.0), ("500ms", 0.5), ("1d", 86_400.0)] {
            assert_eq!(parse_keep_alive(input).ok(), Some(secs), "{input}");
        }
        for bad in ["1x", "m", "", "abc", "2d"] {
            assert!(parse_keep_alive(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn scroll_ids_are_uuid_simple() {
        assert!(valid_scroll_id(&uuid::Uuid::new_v4().simple().to_string()));
        assert!(!valid_scroll_id("bogus"));
        assert!(!valid_scroll_id(""));
    }

    #[test]
    fn dynamic_fields_render_like_opensearch() {
        let s = |types: &[&str]| dynamic_field_spec(&types.iter().map(|t| t.to_string()).collect::<Vec<_>>());
        assert_eq!(
            s(&["string"]),
            json!({"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}})
        );
        assert_eq!(s(&["long"]), json!({"type": "long"}));
        assert_eq!(s(&["double", "long"]), json!({"type": "float"}));
        assert_eq!(s(&["boolean"]), json!({"type": "boolean"}));
        // A path seen as string and number is text.
        assert_eq!(s(&["long", "string"])["type"], json!("text"));

        let mut props = json!({"age": {"type": "long"}, "meta": {"type": "keyword"}});
        insert_dynamic_property(&mut props, "role", json!({"type": "text"}));
        insert_dynamic_property(&mut props, "ctx.job", json!({"type": "text"}));
        insert_dynamic_property(&mut props, "ctx.tries", json!({"type": "long"}));
        insert_dynamic_property(&mut props, "log\\.level", json!({"type": "text"}));
        // Declared properties win; a path beneath a mapped scalar is dropped.
        insert_dynamic_property(&mut props, "age", json!({"type": "text"}));
        insert_dynamic_property(&mut props, "meta.x", json!({"type": "text"}));
        assert_eq!(
            props,
            json!({
                "age": {"type": "long"},
                "meta": {"type": "keyword"},
                "role": {"type": "text"},
                "ctx": {"properties": {"job": {"type": "text"}, "tries": {"type": "long"}}},
                "log.level": {"type": "text"},
            })
        );
    }
}
