use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{Value, json};

use rsearch_common::crypto;
use rsearch_ingest::{BulkParseOutcome, IngestError, parse_bulk_body};
use rsearch_storage::INTERNAL_TOKEN_HEADER;
use tracing::warn;

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

#[derive(serde::Deserialize)]
pub struct InternalBulkQuery {
    index: Option<String>,
}

/// Receive a batch handed off by a peer (#19). Authenticated by the
/// cluster token like the internal object API; always indexes locally —
/// a handed-off batch is never re-forwarded, so no hop loops.
pub async fn bulk_internal(
    State(state): State<AppState>,
    Query(query): Query<InternalBulkQuery>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let Some(forwarder) = state.bulk_forward.clone() else {
        return error_response(StatusCode::NOT_FOUND, "bulk handoff not enabled");
    };
    let presented = headers
        .get(INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if presented.is_empty() || crypto::token_digest(presented) != forwarder.token_digest {
        return (StatusCode::UNAUTHORIZED, "invalid cluster token").into_response();
    }
    if state.draining.load(Ordering::Relaxed) {
        // A peer picked this node before the drain reached the registry;
        // the 503 makes the sender fall back to indexing locally.
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "node is draining; index this batch elsewhere",
        );
    }
    forwarder.received.fetch_add(1, Ordering::Relaxed);
    handle_bulk_local(state, query.index, body).await
}

async fn handle_bulk(state: AppState, default_index: Option<String>, body: String) -> Response {
    if state.draining.load(Ordering::Relaxed) {
        // Draining: refuse new writes so the WAL empties out before
        // shutdown; shippers retry against another ingest node.
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "node is draining; send bulk traffic to another ingest node",
        );
    }
    if state.pipeline.is_none() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "this node does not run the ingest role",
        );
    }
    // Server-side balancing (#19): shippers pin keep-alive connections to
    // one node, so spread whole batches round-robin across live ingest
    // peers. The ack then comes from the target's WAL. Any handoff
    // failure falls back to local indexing — like shipper retries after a
    // timeout, that is at-least-once, never lost.
    if let Some(forwarder) = state.bulk_forward.clone()
        && let Some((peer_id, peer_addr)) = forwarder.pick_target().await
    {
        match forwarder
            .forward(&peer_addr, default_index.as_deref(), body.clone().into())
            .await
        {
            // Relay only a peer 2xx (its per-item results). Anything else
            // — refused (draining, saturated), failed, or a peer version
            // without the endpoint — falls through to local indexing,
            // which reproduces the right client-facing outcome itself.
            Ok((status, response)) if (200..300).contains(&status) => {
                forwarder.forwarded.fetch_add(1, Ordering::Relaxed);
                let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
                return (
                    status,
                    [(header::CONTENT_TYPE, "application/json")],
                    response,
                )
                    .into_response();
            }
            Ok((status, _)) => {
                forwarder.forward_fallbacks.fetch_add(1, Ordering::Relaxed);
                warn!(peer = %peer_id, status, "bulk handoff refused; indexing locally");
            }
            Err(e) => {
                forwarder.forward_fallbacks.fetch_add(1, Ordering::Relaxed);
                warn!(peer = %peer_id, error = %e, "bulk handoff failed; indexing locally");
            }
        }
    }
    handle_bulk_local(state, default_index, body).await
}

async fn handle_bulk_local(
    state: AppState,
    default_index: Option<String>,
    body: String,
) -> Response {
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

    // Routing expansion, then the durability point: append every routed
    // copy to the WAL with a single fsync.
    let BulkParseOutcome {
        items, rejections, ..
    } = outcome;
    let expanded: Vec<(usize, rsearch_ingest::BulkItem, Vec<String>)> = items
        .into_iter()
        .map(|(position, item)| {
            let routes = pipeline.expand_routes(&item.stream, &item.doc);
            (position, item, routes)
        })
        .collect();
    let wal = pipeline.wal().clone();
    let wal_items: Vec<(String, std::sync::Arc<str>)> = expanded
        .iter()
        .flat_map(|(_, item, routes)| {
            // WAL payload = the client's original line bytes (no
            // re-serialize, no byte copy — the Arc is shared).
            routes
                .iter()
                .map(move |stream| (stream.clone(), item.raw.clone()))
        })
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

    // Enqueue each routed copy; an item succeeds if at least one route
    // was accepted. Saturated routes get confirmed so the WAL drains.
    let mut responses: Vec<(usize, Value)> = Vec::new();
    let mut position_iter = positions.into_iter();
    for (position, item, routes) in expanded {
        let action = item.action.as_str();
        let stream_name = item.stream;
        let doc_id = item.doc_id;
        let raw = item.raw;
        let mut accepted = 0usize;
        let mut saturated = false;
        let mut internal_error: Option<String> = None;
        for stream in routes.iter() {
            // One WAL position exists per routed copy by construction; if
            // a refactor ever breaks that, fail the item — not the process.
            let Some(pos) = position_iter.next() else {
                internal_error = Some("internal: missing WAL position for routed copy".into());
                break;
            };
            match pipeline.enqueue(stream, raw.clone(), pos).await {
                Ok(()) => accepted += 1,
                Err(IngestError::Saturated) => {
                    pipeline.wal().confirm(&[pos]);
                    saturated = true;
                }
                Err(e) => {
                    pipeline.wal().confirm(&[pos]);
                    internal_error = Some(e.to_string());
                }
            }
        }
        let entry = if accepted > 0 {
            json!({
                action: {
                    "_index": stream_name,
                    "_id": doc_id,
                    "_version": 1,
                    "result": "created",
                    "status": 201,
                    "_shards": {"total": 1, "successful": 1, "failed": 0},
                }
            })
        } else if saturated {
            json!({
                action: {
                    "_index": stream_name,
                    "_id": doc_id,
                    "status": 429,
                    "error": {
                        "type": "es_rejected_execution_exception",
                        "reason": "ingest queue is full; retry with backoff",
                    }
                }
            })
        } else {
            json!({
                action: {
                    "_index": stream_name,
                    "_id": doc_id,
                    "status": 500,
                    "error": {
                        "type": "internal_error",
                        "reason": internal_error.unwrap_or_else(|| "no route accepted".into()),
                    },
                }
            })
        };
        responses.push((position, entry));
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
