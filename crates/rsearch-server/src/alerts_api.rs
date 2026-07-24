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

/// PUT /_rsearch/alerts/{name}
/// {"stream": ..., "query"?: {...}, "condition_op"?: "gt|lt",
///  "threshold"?: n, "window_secs"?: n, "interval_secs"?: n,
///  "webhook_url": ..., "enabled"?: bool}
pub async fn put_alert(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let Some(stream) = body.get("stream").and_then(Value::as_str) else {
        return err(StatusCode::BAD_REQUEST, "'stream' is required");
    };
    let Some(webhook) = body.get("webhook_url").and_then(Value::as_str) else {
        return err(StatusCode::BAD_REQUEST, "'webhook_url' is required");
    };
    if !webhook.starts_with("http://") && !webhook.starts_with("https://") {
        return err(StatusCode::BAD_REQUEST, "'webhook_url' must be http(s)");
    }
    let query = body.get("query").cloned().unwrap_or_else(|| json!({}));
    let op = body
        .get("condition_op")
        .and_then(Value::as_str)
        .unwrap_or("gt");
    let threshold = body.get("threshold").and_then(Value::as_i64).unwrap_or(0);
    let window = body.get("window_secs").and_then(Value::as_i64).unwrap_or(300);
    let interval = body
        .get("interval_secs")
        .and_then(Value::as_i64)
        .unwrap_or(60);
    if window <= 0 || interval <= 0 {
        return err(StatusCode::BAD_REQUEST, "window/interval must be positive");
    }
    let enabled = body.get("enabled").and_then(Value::as_bool).unwrap_or(true);
    match state
        .metastore
        .upsert_alert(&name, stream, &query, op, threshold, window, interval, webhook, enabled)
        .await
    {
        Ok(alert) => {
            state.audit("alert_upserted", &name, stream).await;
            Json(json!({"acknowledged": true, "alert": alert})).into_response()
        }
        Err(e) => err(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

/// GET /_rsearch/alerts
pub async fn list_alerts(State(state): State<AppState>) -> Response {
    match state.metastore.list_alerts().await {
        Ok(alerts) => Json(json!({"alerts": alerts})).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// DELETE /_rsearch/alerts/{name}
pub async fn delete_alert(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.metastore.delete_alert(&name).await {
        Ok(true) => {
            state.audit("alert_deleted", &name, "").await;
            Json(json!({"acknowledged": true})).into_response()
        }
        Ok(false) => err(StatusCode::NOT_FOUND, &format!("no alert '{name}'")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}
