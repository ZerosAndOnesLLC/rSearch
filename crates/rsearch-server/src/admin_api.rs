use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::state::AppState;

fn err(status: StatusCode, reason: &str) -> Response {
    (
        status,
        Json(json!({"error": {"type": "illegal_argument_exception", "reason": reason},
                    "status": status.as_u16()})),
    )
        .into_response()
}

/// GET /_cat/indices — streams with published doc/size stats.
pub async fn cat_indices(State(state): State<AppState>) -> Response {
    match state.metastore.stream_stats().await {
        Ok(stats) => Json(
            stats
                .into_iter()
                .map(|s| {
                    json!({
                        "index": s.name,
                        "health": "green",
                        "status": "open",
                        "docs.count": s.doc_count.to_string(),
                        "store.size": format!("{}kb", s.size_bytes / 1024),
                        "splits": s.split_count,
                        "retention_hours": s.retention_hours,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// GET /_rsearch/routing_rules
pub async fn list_rules(State(state): State<AppState>) -> Response {
    match state.metastore.list_routing_rules().await {
        Ok(rules) => Json(json!({"rules": rules})).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// PUT /_rsearch/routing_rules/{name}
/// Body: {"field": "...", "op": "eq|contains|exists", "value": "...",
///        "target_stream": "...", "copy": bool}
pub async fn put_rule(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let Some(field) = body.get("field").and_then(Value::as_str) else {
        return err(StatusCode::BAD_REQUEST, "'field' is required");
    };
    let Some(target) = body.get("target_stream").and_then(Value::as_str) else {
        return err(StatusCode::BAD_REQUEST, "'target_stream' is required");
    };
    let op = body.get("op").and_then(Value::as_str).unwrap_or("eq");
    let value = body.get("value").and_then(Value::as_str).unwrap_or("");
    let copy = body.get("copy").and_then(Value::as_bool).unwrap_or(false);
    match state
        .metastore
        .create_routing_rule(&name, field, op, value, target, copy)
        .await
    {
        Ok(rule) => Json(json!({"acknowledged": true, "rule": rule})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

/// DELETE /_rsearch/routing_rules/{name}
pub async fn delete_rule(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.metastore.delete_routing_rule(&name).await {
        Ok(true) => Json(json!({"acknowledged": true})).into_response(),
        Ok(false) => err(StatusCode::NOT_FOUND, &format!("no rule named '{name}'")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// PUT /_rsearch/streams/{name}/retention — body {"hours": n | null}
pub async fn put_retention(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let hours = match body.get("hours") {
        Some(Value::Null) | None => None,
        Some(Value::Number(n)) => match n.as_i64() {
            Some(h) if h >= 0 && h <= i32::MAX as i64 => Some(h as i32),
            _ => return err(StatusCode::BAD_REQUEST, "'hours' out of range"),
        },
        _ => return err(StatusCode::BAD_REQUEST, "'hours' must be a number or null"),
    };
    match state.metastore.set_stream_retention(&name, hours).await {
        Ok(()) => Json(json!({"acknowledged": true, "stream": name, "retention_hours": hours}))
            .into_response(),
        Err(rsearch_metastore::MetastoreError::StreamNotFound(_)) => {
            err(StatusCode::NOT_FOUND, &format!("no stream named '{name}'"))
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}
