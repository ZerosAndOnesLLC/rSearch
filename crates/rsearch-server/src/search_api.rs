use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use rsearch_metastore::{MetastoreError, StreamMode};
use rsearch_search::{SearchError, SearchRequest};

use crate::state::AppState;

/// POST/GET /{index}/_search
pub async fn search(
    State(state): State<AppState>,
    Path(index): Path<String>,
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
                    &format!("invalid search body: {e}"),
                );
            }
        }
    };
    let request = match SearchRequest::parse(&index, &body_json) {
        Ok(request) => request,
        Err(e) => return map_search_error(e),
    };
    match service.search(request).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => map_search_error(e),
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
        .filter(|name| glob_match(pattern, name))
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

/// Minimal glob: '*' matches any run of characters.
fn glob_match(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut rest = name;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        match rest.find(part) {
            Some(pos) => {
                // First part must anchor at the start.
                if i == 0 && pos != 0 {
                    return false;
                }
                rest = &rest[pos + part.len()..];
            }
            None => return false,
        }
    }
    // Last part must anchor at the end unless the pattern ends with '*'.
    pattern.ends_with('*') || parts.last().map(|p| rest.is_empty() || p.is_empty()).unwrap_or(true)
}

/// GET /{index}/_mapping — Grafana fetches this to discover fields.
pub async fn get_mapping(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    match state.metastore.get_stream(&index).await {
        Ok(stream) => Json(json!({ index: {"mappings": mappings_json(&stream)} })).into_response(),
        Err(MetastoreError::StreamNotFound(_)) => index_not_found(&index),
        Err(e) => es_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            &e.to_string(),
        ),
    }
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
            if let Some(pipeline) = &state.pipeline {
                pipeline.forget_stream(&index);
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

/// ES-shaped mappings block for a stream.
fn mappings_json(stream: &rsearch_metastore::StreamRecord) -> Value {
    let mut properties = stream
        .mapping
        .get("properties")
        .cloned()
        .unwrap_or_else(|| json!({}));
    // The timestamp field is always present and always a date.
    properties["@timestamp"] = json!({"type": "date"});
    json!({"properties": properties})
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
        Ok(stream) => Json(json!({
            index: {
                "aliases": {},
                "mappings": mappings_json(&stream),
                "settings": settings_json(&stream),
            }
        }))
        .into_response(),
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

fn es_error(status: StatusCode, error_type: &str, reason: &str) -> Response {
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
