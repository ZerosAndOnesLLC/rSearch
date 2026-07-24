use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{Value, json};

use rsearch_ingest::{BulkParseOutcome, IngestError, parse_bulk_body};

use crate::state::AppState;

pub async fn bulk_root(State(state): State<AppState>, body: String) -> Response {
    handle_bulk(state, None, body).await
}

pub async fn bulk_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: String,
) -> Response {
    handle_bulk(state, Some(index), body).await
}

async fn handle_bulk(state: AppState, default_index: Option<String>, body: String) -> Response {
    let started = Instant::now();
    let Some(pipeline) = state.pipeline.clone() else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "this node does not run the ingest role",
        );
    };

    let outcome = match parse_bulk_body(&body, default_index.as_deref()) {
        Ok(outcome) => outcome,
        Err(reason) => return error_response(StatusCode::BAD_REQUEST, &reason),
    };
    if outcome.total == 0 {
        return error_response(StatusCode::BAD_REQUEST, "empty bulk body");
    }

    // Durability point: append every accepted doc to the WAL, fsync once.
    let wal = pipeline.wal().clone();
    let wal_items: Vec<(String, Vec<u8>)> = outcome
        .items
        .iter()
        .map(|(_, item)| (item.stream.clone(), item.doc.to_string().into_bytes()))
        .collect();
    let positions = match tokio::task::spawn_blocking(move || wal.append_batch(&wal_items)).await {
        Ok(Ok(positions)) => positions,
        Ok(Err(e)) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("wal append failed: {e}"),
            );
        }
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("wal task failed: {e}"),
            );
        }
    };

    // Enqueue for indexing; saturated items get per-item 429s and their
    // WAL positions confirmed (the shipper will retry them).
    let mut responses: Vec<(usize, Value)> = Vec::with_capacity(outcome.total);
    let BulkParseOutcome {
        items, rejections, ..
    } = outcome;
    for ((position, item), pos) in items.into_iter().zip(positions) {
        let action = item.action.as_str();
        match pipeline.enqueue(&item.stream, item.doc, pos).await {
            Ok(()) => {
                responses.push((
                    position,
                    json!({
                        action: {
                            "_index": item.stream,
                            "_id": item.doc_id,
                            "_version": 1,
                            "result": "created",
                            "status": 201,
                            "_shards": {"total": 1, "successful": 1, "failed": 0},
                        }
                    }),
                ));
            }
            Err(IngestError::Saturated) => {
                pipeline.wal().confirm(&[pos]);
                responses.push((
                    position,
                    json!({
                        action: {
                            "_index": item.stream,
                            "_id": item.doc_id,
                            "status": 429,
                            "error": {
                                "type": "es_rejected_execution_exception",
                                "reason": "ingest queue is full; retry with backoff",
                            }
                        }
                    }),
                ));
            }
            Err(e) => {
                pipeline.wal().confirm(&[pos]);
                responses.push((
                    position,
                    json!({
                        action: {
                            "_index": item.stream,
                            "_id": item.doc_id,
                            "status": 500,
                            "error": {"type": "internal_error", "reason": e.to_string()},
                        }
                    }),
                ));
            }
        }
    }
    for (position, action, index, reason) in rejections {
        responses.push((
            position,
            json!({
                action: {
                    "_index": index,
                    "status": 400,
                    "error": {"type": "illegal_argument_exception", "reason": reason},
                }
            }),
        ));
    }
    responses.sort_by_key(|(position, _)| *position);

    let errors = responses.iter().any(|(_, item)| {
        item.as_object()
            .and_then(|o| o.values().next())
            .and_then(|v| v.get("status"))
            .and_then(Value::as_i64)
            .map(|s| s >= 300)
            .unwrap_or(true)
    });
    let items: Vec<Value> = responses.into_iter().map(|(_, item)| item).collect();
    Json(json!({
        "took": started.elapsed().as_millis() as u64,
        "errors": errors,
        "items": items,
    }))
    .into_response()
}

fn error_response(status: StatusCode, reason: &str) -> Response {
    (
        status,
        Json(json!({
            "error": {"type": "illegal_argument_exception", "reason": reason},
            "status": status.as_u16(),
        })),
    )
        .into_response()
}
