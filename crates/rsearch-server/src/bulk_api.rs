use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{Value, json};

use rsearch_common::crypto;
use rsearch_index::DocIdentity;
use rsearch_ingest::{
    BulkAction, BulkParseOutcome, IngestError, StreamInfo, WalItem, parse_bulk_body,
};
use rsearch_metastore::{NewTombstone, StreamMode};
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

/// Reason given when `delete`/`update` hit a log-mode stream.
fn log_mode_reason(action: &str, stream: &str) -> String {
    format!(
        "{action} is not supported on log-mode index [{stream}]; create the index with \
         {{\"settings\":{{\"index\":{{\"mode\":\"document\"}}}}}} to enable \
         document-level writes"
    )
}

/// One per-item outcome, positioned for the response.
struct ItemResult {
    position: usize,
    body: Value,
}

fn item_ok(action: &str, index: &str, id: &str, version: i64, result: &str, status: u16) -> Value {
    json!({
        action: {
            "_index": index,
            "_id": id,
            "_version": version,
            "result": result,
            "status": status,
            "_shards": {"total": 1, "successful": 1, "failed": 0},
        }
    })
}

fn item_err(action: &str, index: &str, id: Option<&str>, status: u16, error_type: &str, reason: &str) -> Value {
    let mut body = json!({
        "_index": index,
        "status": status,
        "error": {"type": error_type, "reason": reason},
    });
    if let Some(id) = id {
        body["_id"] = json!(id);
    }
    json!({ action: body })
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
    let BulkParseOutcome {
        items, rejections, ..
    } = outcome;
    let mut results: Vec<ItemResult> = rejections
        .into_iter()
        .map(|(position, action, index, reason)| ItemResult {
            position,
            body: item_err(&action, &index, None, 400, "illegal_argument_exception", &reason),
        })
        .collect();

    // Routing expansion + one write-sequence stamp per item (shared by all
    // its routed copies, so the WAL, the split, and the response agree).
    // Deletes route nowhere: they target exactly the named stream.
    struct Planned {
        position: usize,
        item: rsearch_ingest::BulkItem,
        routes: Vec<String>,
        seq: i64,
    }
    let mut planned: Vec<Planned> = Vec::with_capacity(items.len());
    for (position, item) in items {
        let routes = match item.action {
            BulkAction::Index | BulkAction::Create => {
                pipeline.expand_routes(&item.stream, &item.doc)
            }
            BulkAction::Update | BulkAction::Delete => vec![item.stream.clone()],
        };
        planned.push(Planned {
            position,
            item,
            routes,
            seq: pipeline.next_seq(),
        });
    }

    // Resolve every target stream's id + mode (creating missing streams,
    // as _bulk always has). Document-mode targets get tombstones.
    let mut infos: HashMap<String, StreamInfo> = HashMap::new();
    for plan in &planned {
        for stream in &plan.routes {
            if infos.contains_key(stream) {
                continue;
            }
            match pipeline.stream_info(stream).await {
                Ok(info) => {
                    infos.insert(stream.clone(), info);
                }
                Err(e) => {
                    return error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        &format!("metastore unavailable: {e}"),
                    );
                }
            }
        }
    }

    // Tombstones are written before anything is WAL-appended: a crash
    // between the two can lose an unacked write (the client retries), but
    // never leaves a replayed document without the tombstone that hides
    // its predecessors.
    let mut tombstones: Vec<NewTombstone> = Vec::new();
    let mut writes: Vec<&Planned> = Vec::new();
    for plan in &planned {
        let action = plan.item.action.as_str();
        match plan.item.action {
            BulkAction::Index | BulkAction::Create => {
                if plan.item.explicit_id {
                    for stream in &plan.routes {
                        if let Some(info) = infos.get(stream)
                            && info.mode == StreamMode::Document
                        {
                            tombstones.push(NewTombstone {
                                stream_id: info.id,
                                doc_id: plan.item.doc_id.clone(),
                                before_seq: plan.seq,
                            });
                        }
                    }
                }
                writes.push(plan);
            }
            BulkAction::Delete => match infos.get(&plan.item.stream) {
                Some(info) if info.mode == StreamMode::Document => {
                    tombstones.push(NewTombstone {
                        stream_id: info.id,
                        doc_id: plan.item.doc_id.clone(),
                        before_seq: plan.seq,
                    });
                    results.push(ItemResult {
                        position: plan.position,
                        body: item_ok(action, &plan.item.stream, &plan.item.doc_id, plan.seq, "deleted", 200),
                    });
                }
                _ => results.push(ItemResult {
                    position: plan.position,
                    body: item_err(
                        action,
                        &plan.item.stream,
                        Some(&plan.item.doc_id),
                        400,
                        "illegal_argument_exception",
                        &log_mode_reason(action, &plan.item.stream),
                    ),
                }),
            },
            BulkAction::Update => results.push(ItemResult {
                position: plan.position,
                body: item_err(
                    action,
                    &plan.item.stream,
                    Some(&plan.item.doc_id),
                    400,
                    "illegal_argument_exception",
                    &match infos.get(&plan.item.stream) {
                        Some(info) if info.mode == StreamMode::Document => {
                            "update is not supported in _bulk yet".to_string()
                        }
                        _ => log_mode_reason(action, &plan.item.stream),
                    },
                ),
            }),
        }
    }
    if !tombstones.is_empty() {
        if let Err(e) = state.metastore.upsert_tombstones(&tombstones).await {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("metastore unavailable (tombstones): {e}"),
            );
        }
        // A search immediately after this write on this node must see it.
        if let Some(search) = &state.search {
            let mut seen = std::collections::HashSet::new();
            for t in &tombstones {
                if seen.insert(t.stream_id) {
                    search.invalidate_tombstones(t.stream_id);
                }
            }
        }
    }

    // The durability point: append every routed copy to the WAL with a
    // single fsync.
    let wal = pipeline.wal().clone();
    let wal_items: Vec<WalItem> = writes
        .iter()
        .flat_map(|plan| {
            // WAL payload = the client's original line bytes (no
            // re-serialize, no byte copy — the Arc is shared).
            plan.routes.iter().map(move |stream| WalItem {
                stream: stream.clone(),
                id: plan.item.doc_id.clone(),
                seq: plan.seq,
                doc: plan.item.raw.clone(),
            })
        })
        .collect();
    let positions = if wal_items.is_empty() {
        Vec::new()
    } else {
        match tokio::task::spawn_blocking(move || wal.append_batch(&wal_items)).await {
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
        }
    };

    // Enqueue each routed copy; an item succeeds if at least one route
    // was accepted. Saturated routes get confirmed so the WAL drains.
    let mut position_iter = positions.into_iter();
    for plan in writes {
        let action = plan.item.action.as_str();
        let stream_name = &plan.item.stream;
        let doc_id = &plan.item.doc_id;
        let mut accepted = 0usize;
        let mut saturated = false;
        let mut internal_error: Option<String> = None;
        for stream in plan.routes.iter() {
            // One WAL position exists per routed copy by construction; if
            // a refactor ever breaks that, fail the item — not the process.
            let Some(pos) = position_iter.next() else {
                internal_error = Some("internal: missing WAL position for routed copy".into());
                break;
            };
            let identity = DocIdentity::new(doc_id.clone(), plan.seq);
            match pipeline
                .enqueue(stream, plan.item.raw.clone(), identity, pos)
                .await
            {
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
        let body = if accepted > 0 {
            // ES reports "updated" for an index that replaced a document;
            // an explicit id on a document-mode stream may have, and the
            // cheap honest answer without a lookup is "created" for fresh
            // ids and "updated" for explicit ones (the tombstone covers
            // both cases).
            let replaced = plan.item.explicit_id
                && plan.item.action == BulkAction::Index
                && infos
                    .get(stream_name)
                    .is_some_and(|i| i.mode == StreamMode::Document);
            let (result, status) = if replaced { ("updated", 200) } else { ("created", 201) };
            item_ok(action, stream_name, doc_id, plan.seq, result, status)
        } else if saturated {
            item_err(
                action,
                stream_name,
                Some(doc_id),
                429,
                "es_rejected_execution_exception",
                "ingest queue is full; retry with backoff",
            )
        } else {
            item_err(
                action,
                stream_name,
                Some(doc_id),
                500,
                "internal_error",
                &internal_error.unwrap_or_else(|| "no route accepted".into()),
            )
        };
        results.push(ItemResult {
            position: plan.position,
            body,
        });
    }
    results.sort_by_key(|r| r.position);

    let errors = results.iter().any(|r| {
        r.body
            .as_object()
            .and_then(|o| o.values().next())
            .and_then(|v| v.get("status"))
            .and_then(Value::as_i64)
            .map(|s| s >= 300)
            .unwrap_or(true)
    });
    let items: Vec<Value> = results.into_iter().map(|r| r.body).collect();
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
