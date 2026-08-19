use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
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

/// `?refresh=` on `_bulk` and the document routes: `true` / `wait_for`
/// (treated alike — both wait until the written documents are searchable)
/// or `false` / absent. Only document-mode streams honor it; log streams
/// ignore it so a shipper that sets it can't force a split per request.
pub fn parse_refresh(value: Option<&str>) -> bool {
    matches!(value, Some("true") | Some("wait_for") | Some(""))
}

#[derive(serde::Deserialize, Default)]
pub struct BulkQuery {
    refresh: Option<String>,
}

pub async fn bulk_root(
    State(state): State<AppState>,
    Query(query): Query<BulkQuery>,
    body: String,
) -> Response {
    handle_bulk(state, None, body, parse_refresh(query.refresh.as_deref())).await
}

pub async fn bulk_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    Query(query): Query<BulkQuery>,
    body: String,
) -> Response {
    handle_bulk(state, Some(index), body, parse_refresh(query.refresh.as_deref())).await
}

#[derive(serde::Deserialize)]
pub struct InternalBulkQuery {
    index: Option<String>,
    refresh: Option<String>,
}

/// How long a refresh waits for the cut split to publish before the
/// response goes out anyway (the documents are WAL-durable and will
/// appear; a storage/metastore outage must not hang the client forever).
const REFRESH_WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(60);

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
    let refresh = parse_refresh(query.refresh.as_deref());
    match handle_bulk_local(state, query.index, body, refresh).await {
        Ok(result) => Json(result).into_response(),
        Err(response) => response,
    }
}

async fn handle_bulk(
    state: AppState,
    default_index: Option<String>,
    body: String,
    refresh: bool,
) -> Response {
    match execute_bulk(state, default_index, body, refresh).await {
        Ok(result) => Json(result).into_response(),
        Err(response) => response,
    }
}

/// Run a bulk body end to end — peer handoff when enabled, otherwise
/// locally — and return the ES bulk response JSON, or a whole-request
/// error response. The `_doc` routes synthesize one-item bodies and go
/// through here so every write path shares the same semantics.
pub async fn execute_bulk(
    state: AppState,
    default_index: Option<String>,
    body: String,
    refresh: bool,
) -> Result<Value, Response> {
    if state.draining.load(Ordering::Relaxed) {
        // Draining: refuse new writes so the WAL empties out before
        // shutdown; shippers retry against another ingest node.
        return Err(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "node is draining; send bulk traffic to another ingest node",
        ));
    }
    if state.pipeline.is_none() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "this node does not run the ingest role",
        ));
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
            .forward(&peer_addr, default_index.as_deref(), refresh, body.clone().into())
            .await
        {
            // Relay only a peer 2xx (its per-item results). Anything else
            // — refused (draining, saturated), failed, or a peer version
            // without the endpoint — falls through to local indexing,
            // which reproduces the right client-facing outcome itself.
            Ok((status, response)) if (200..300).contains(&status) => {
                match serde_json::from_slice::<Value>(&response) {
                    Ok(value) => {
                        forwarder.forwarded.fetch_add(1, Ordering::Relaxed);
                        return Ok(value);
                    }
                    Err(e) => {
                        forwarder.forward_fallbacks.fetch_add(1, Ordering::Relaxed);
                        warn!(peer = %peer_id, error = %e, "bulk handoff returned malformed JSON; indexing locally");
                    }
                }
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
    handle_bulk_local(state, default_index, body, refresh).await
}

/// Reason given when `delete`/`update` hit a log-mode stream.
fn log_mode_reason(action: &str, stream: &str) -> String {
    format!(
        "{action} is not supported on log-mode index [{stream}]; create the index with \
         {{\"settings\":{{\"index\":{{\"mode\":\"document\"}}}}}} to enable \
         document-level writes"
    )
}

/// Per-item error for a failed lookup (closure over action/index/id).
type LookupErr = Box<dyn Fn(&str, &str, &str) -> Value + Send + Sync>;

/// Current live versions for all (stream, id) pairs that read-modify-write
/// actions in this batch need, in one `ids` search per stream. Ids not
/// found are simply absent. A stream-level failure is returned per
/// stream so each of its items can answer with the same error.
async fn lookup_batch(
    state: &AppState,
    wanted: &HashMap<String, Vec<String>>,
) -> HashMap<String, Result<HashMap<String, rsearch_search::FoundDocument>, LookupErr>> {
    let mut out = HashMap::new();
    for (stream, ids) in wanted {
        let Some(lookup) = &state.doc_lookup else {
            out.insert(
                stream.clone(),
                Err(Box::new(|action: &str, index: &str, id: &str| {
                    item_err(
                        action,
                        index,
                        Some(id),
                        400,
                        "illegal_argument_exception",
                        "document lookups need a node running the ingest or search role",
                    )
                }) as LookupErr),
            );
            continue;
        };
        let result = match lookup.get_documents(stream, ids).await {
            Ok(found) => Ok(found),
            // A stream that exists but has no splits yet has nothing to find.
            Err(rsearch_search::SearchError::Metastore(
                rsearch_metastore::MetastoreError::StreamNotFound(_),
            )) => Ok(HashMap::new()),
            Err(e) => {
                let reason = e.to_string();
                Err(Box::new(move |action: &str, index: &str, id: &str| {
                    item_err(action, index, Some(id), 503, "unavailable_shards_exception", &reason)
                }) as LookupErr)
            }
        };
        out.insert(stream.clone(), result);
    }
    out
}

/// Apply an ES `_update` body to the current document. Returns the new
/// document and whether it was an upsert (no current version), `Ok(None)`
/// when the document is missing and no upsert applies, or an error for a
/// malformed body. Supports `doc`, `doc_as_upsert`, and `upsert`; scripts
/// are not supported.
fn apply_update(body: &Value, current: Option<Value>) -> Result<Option<(Value, bool)>, String> {
    if body.get("script").is_some() {
        return Err("scripted updates are not supported".to_string());
    }
    let partial = body
        .get("doc")
        .ok_or_else(|| "update body requires 'doc'".to_string())?;
    if !partial.is_object() {
        return Err("'doc' must be an object".to_string());
    }
    match current {
        Some(mut current) => {
            merge_json(&mut current, partial);
            Ok(Some((current, false)))
        }
        None => {
            let doc_as_upsert = body
                .get("doc_as_upsert")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if doc_as_upsert {
                return Ok(Some((partial.clone(), true)));
            }
            match body.get("upsert") {
                Some(upsert) if upsert.is_object() => Ok(Some((upsert.clone(), true))),
                Some(_) => Err("'upsert' must be an object".to_string()),
                None => Ok(None),
            }
        }
    }
}

/// ES partial-update merge: objects merge recursively, everything else
/// (including arrays) is replaced.
fn merge_json(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                match target.get_mut(key) {
                    Some(existing) if existing.is_object() && value.is_object() => {
                        merge_json(existing, value);
                    }
                    _ => {
                        target.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

/// A search immediately after a local tombstone write must see it: drop
/// the freshness of the affected streams' tombstone caches on this node —
/// both the public search service and the write path's lookup service
/// (the same instance when a node runs both roles).
fn invalidate_tombstone_caches(state: &AppState, tombstones: &[NewTombstone]) {
    let mut seen = std::collections::HashSet::new();
    for t in tombstones {
        if seen.insert(t.stream_id) {
            if let Some(search) = &state.search {
                search.invalidate_tombstones(t.stream_id);
            }
            if let Some(lookup) = &state.doc_lookup {
                lookup.invalidate_tombstones(t.stream_id);
            }
        }
    }
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
    refresh: bool,
) -> Result<Value, Response> {
    let started = Instant::now();
    let Some(pipeline) = state.pipeline.clone() else {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "this node does not run the ingest role",
        ));
    };

    let outcome = match parse_bulk_body(&body, default_index.as_deref()) {
        Ok(outcome) => outcome,
        Err(reason) => return Err(error_response(StatusCode::BAD_REQUEST, &reason)),
    };
    if outcome.total == 0 {
        return Err(error_response(StatusCode::BAD_REQUEST, "empty bulk body"));
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

    // Resolve every named stream's id + mode up front: the mode decides
    // what each action means. Writes create missing streams (as _bulk
    // always has); delete/update against a stream that doesn't exist is a
    // 404 like ES, not an implicit creation.
    let mut infos: HashMap<String, StreamInfo> = HashMap::new();
    async fn resolve_info(
        pipeline: &rsearch_ingest::IngestPipeline,
        infos: &mut HashMap<String, StreamInfo>,
        stream: &str,
        create: bool,
    ) -> Result<Option<StreamInfo>, Response> {
        if let Some(info) = infos.get(stream) {
            return Ok(Some(*info));
        }
        let resolved = if create {
            pipeline.stream_info(stream).await.map(Some)
        } else {
            pipeline.stream_info_if_exists(stream).await
        };
        match resolved {
            Ok(Some(info)) => {
                infos.insert(stream.to_string(), info);
                Ok(Some(info))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("metastore unavailable: {e}"),
            )),
        }
    }
    let mut kept: Vec<(usize, rsearch_ingest::BulkItem)> = Vec::with_capacity(items.len());
    for (position, item) in items {
        let create = matches!(item.action, BulkAction::Index | BulkAction::Create);
        match resolve_info(&pipeline, &mut infos, &item.stream, create).await? {
            Some(_) => kept.push((position, item)),
            None => results.push(ItemResult {
                position,
                body: item_err(
                    item.action.as_str(),
                    &item.stream,
                    Some(&item.doc_id),
                    404,
                    "index_not_found_exception",
                    &format!("no such index [{}]", item.stream),
                ),
            }),
        }
    }
    let items = kept;

    // Hybrid-clock causality: every write to an explicit id on a
    // document-mode stream must stamp a sequence above the bound any
    // earlier write (on any node) recorded for that id — otherwise a node
    // whose wall clock lags would produce a "newer" version that the
    // existing tombstone hides. One indexed lookup per batch.
    let doc_mode = |infos: &HashMap<String, StreamInfo>, stream: &str| {
        infos
            .get(stream)
            .is_some_and(|i| i.mode == StreamMode::Document)
    };
    let id_pairs: Vec<(i64, String)> = items
        .iter()
        .filter(|(_, item)| item.explicit_id && doc_mode(&infos, &item.stream))
        .filter_map(|(_, item)| infos.get(&item.stream).map(|i| (i.id, item.doc_id.clone())))
        .collect();
    if !id_pairs.is_empty() {
        match state.metastore.tombstone_bounds(&id_pairs).await {
            Ok(bounds) => {
                if let Some(max) = bounds.iter().map(|(_, _, b)| *b).max() {
                    pipeline.observe_seq(max);
                }
            }
            Err(e) => {
                return Err(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &format!("metastore unavailable: {e}"),
                ));
            }
        }
    }

    // Read-modify-write actions on document-mode streams (update, create)
    // look the current version up first — batched per stream — and become
    // plain index writes, or per-item errors. Lookups see published
    // splits only (a write still in an ingest buffer is invisible until
    // its split is cut; use ?refresh=wait_for on the prior write when that
    // matters).
    let mut wanted: HashMap<String, Vec<String>> = HashMap::new();
    for (_, item) in &items {
        let needs = match item.action {
            BulkAction::Update => doc_mode(&infos, &item.stream),
            BulkAction::Create => doc_mode(&infos, &item.stream) && item.explicit_id,
            _ => false,
        };
        if needs {
            wanted
                .entry(item.stream.clone())
                .or_default()
                .push(item.doc_id.clone());
        }
    }
    let looked_up = lookup_batch(&state, &wanted).await;
    let current_of = |stream: &str,
                      id: &str|
     -> Result<Option<rsearch_search::FoundDocument>, &LookupErr> {
        match looked_up.get(stream) {
            Some(Ok(found)) => Ok(found.get(id).cloned()),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    };
    struct Planned {
        position: usize,
        item: rsearch_ingest::BulkItem,
        routes: Vec<String>,
        seq: i64,
        /// (result, status) the response reports on success.
        outcome: (&'static str, u16),
        /// Action name as the client sent it (an `update` becomes an
        /// `index` write internally but answers as `update`).
        wire_action: &'static str,
    }
    let mut planned: Vec<Planned> = Vec::with_capacity(items.len());
    for (position, mut item) in items {
        let action = item.action.as_str();
        let document_mode = doc_mode(&infos, &item.stream);
        let mut outcome = ("created", 201);
        match item.action {
            BulkAction::Index if document_mode && item.explicit_id => {
                // Without a lookup we can't tell a fresh id from a
                // replacement; ES reports "updated" for the latter, and
                // "updated" is the honest answer for an explicit id on a
                // document index (the tombstone covers both cases).
                outcome = ("updated", 200);
            }
            BulkAction::Index => {}
            BulkAction::Create if document_mode && item.explicit_id => {
                match current_of(&item.stream, &item.doc_id) {
                    Ok(Some(current)) => {
                        results.push(ItemResult {
                            position,
                            body: item_err(
                                action,
                                &item.stream,
                                Some(&item.doc_id),
                                409,
                                "version_conflict_engine_exception",
                                &format!(
                                    "[{}]: version conflict, document already exists (current version [{}])",
                                    item.doc_id, current.version
                                ),
                            ),
                        });
                        continue;
                    }
                    Ok(None) => {}
                    Err(body) => {
                        results.push(ItemResult {
                            position,
                            body: body(action, &item.stream, &item.doc_id),
                        });
                        continue;
                    }
                }
            }
            BulkAction::Create => {}
            BulkAction::Update => {
                if !document_mode {
                    results.push(ItemResult {
                        position,
                        body: item_err(
                            action,
                            &item.stream,
                            Some(&item.doc_id),
                            400,
                            "illegal_argument_exception",
                            &log_mode_reason(action, &item.stream),
                        ),
                    });
                    continue;
                }
                let current = match current_of(&item.stream, &item.doc_id) {
                    Ok(current) => current,
                    Err(body) => {
                        results.push(ItemResult {
                            position,
                            body: body(action, &item.stream, &item.doc_id),
                        });
                        continue;
                    }
                };
                let merged = match apply_update(&item.doc, current.map(|c| c.source)) {
                    Ok(Some((doc, was_upsert))) => {
                        outcome = if was_upsert { ("created", 201) } else { ("updated", 200) };
                        doc
                    }
                    Ok(None) => {
                        results.push(ItemResult {
                            position,
                            body: item_err(
                                action,
                                &item.stream,
                                Some(&item.doc_id),
                                404,
                                "document_missing_exception",
                                &format!("[{}]: document missing", item.doc_id),
                            ),
                        });
                        continue;
                    }
                    Err(reason) => {
                        results.push(ItemResult {
                            position,
                            body: item_err(
                                action,
                                &item.stream,
                                Some(&item.doc_id),
                                400,
                                "illegal_argument_exception",
                                &reason,
                            ),
                        });
                        continue;
                    }
                };
                // Becomes an ordinary explicit-id index write; the response
                // still says "update".
                item.raw = std::sync::Arc::from(merged.to_string());
                item.doc = merged;
                item.action = BulkAction::Index;
            }
            BulkAction::Delete => {
                outcome = ("deleted", 200);
            }
        }
        // Routing expansion + one write-sequence stamp per item (shared by
        // all its routed copies, so the WAL, the split, and the response
        // agree). Deletes route nowhere: they target exactly the named
        // stream.
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
            outcome,
            wire_action: action,
        });
    }
    // Routing rules may fan a document out to streams not named in the
    // request; resolve those too.
    for plan in &planned {
        for stream in &plan.routes {
            resolve_info(&pipeline, &mut infos, stream, true).await?;
        }
    }

    // Delete tombstones are written now, before the response says
    // "deleted". Replacement tombstones (explicit-id index/create on a
    // document-mode stream) are written only *after* the new version is
    // WAL-durable and accepted — a rejected write (429/500) must leave the
    // old version visible, not hide it. The WAL record carries the
    // tombstone flag so a crash between the append and the upsert is
    // repaired on replay.
    let mut tombstones: Vec<NewTombstone> = Vec::new();
    let mut writes: Vec<&Planned> = Vec::new();
    for plan in &planned {
        let action = plan.wire_action;
        match plan.item.action {
            BulkAction::Index | BulkAction::Create => writes.push(plan),
            BulkAction::Delete => match infos.get(&plan.item.stream) {
                Some(info) if info.mode == StreamMode::Document => {
                    tombstones.push(NewTombstone {
                        stream_id: info.id,
                        doc_id: plan.item.doc_id.clone(),
                        before_seq: plan.seq,
                    });
                    results.push(ItemResult {
                        position: plan.position,
                        body: item_ok(
                            action,
                            &plan.item.stream,
                            &plan.item.doc_id,
                            plan.seq,
                            plan.outcome.0,
                            plan.outcome.1,
                        ),
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
            // Updates were rewritten into index writes (or rejected) above.
            BulkAction::Update => results.push(ItemResult {
                position: plan.position,
                body: item_err(
                    action,
                    &plan.item.stream,
                    Some(&plan.item.doc_id),
                    500,
                    "internal_error",
                    "update was not resolved",
                ),
            }),
        }
    }
    if !tombstones.is_empty() {
        if let Err(e) = state.metastore.upsert_tombstones(&tombstones).await {
            return Err(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("metastore unavailable (tombstones): {e}"),
            ));
        }
        invalidate_tombstone_caches(&state, &tombstones);
    }
    // Whether a routed copy of an explicit-id write replaces older
    // versions (document-mode target) and so carries a tombstone.
    let replaces = |plan: &Planned, stream: &str| {
        plan.item.explicit_id
            && infos
                .get(stream)
                .is_some_and(|i| i.mode == StreamMode::Document)
    };

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
                tombstone: replaces(plan, stream),
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
                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("wal append failed: {e}"),
                ));
            }
            Err(e) => {
                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("wal task failed: {e}"),
                ));
            }
        }
    };

    // Enqueue each routed copy; an item succeeds if at least one route
    // was accepted. Saturated routes get confirmed so the WAL drains.
    let mut position_iter = positions.into_iter();
    // Document-mode streams that received a write, for ?refresh.
    let mut to_refresh: Vec<String> = Vec::new();
    // Replacement tombstones for accepted copies, written below.
    let mut replacement_tombstones: Vec<NewTombstone> = Vec::new();
    for plan in writes {
        let action = plan.wire_action;
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
                Ok(()) => {
                    accepted += 1;
                    if let Some(info) = infos.get(stream)
                        && info.mode == StreamMode::Document
                    {
                        if replaces(plan, stream) {
                            replacement_tombstones.push(NewTombstone {
                                stream_id: info.id,
                                doc_id: doc_id.clone(),
                                before_seq: plan.seq,
                            });
                        }
                        if refresh && !to_refresh.contains(stream) {
                            to_refresh.push(stream.clone());
                        }
                    }
                }
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
            item_ok(action, stream_name, doc_id, plan.seq, plan.outcome.0, plan.outcome.1)
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

    // The accepted replacements' tombstones. The documents are already
    // WAL-durable with the tombstone flag, so a metastore failure here is
    // retried in the background rather than failing acked items; until it
    // lands, the old and new version are both visible (never the reverse).
    if !replacement_tombstones.is_empty() {
        match state.metastore.upsert_tombstones(&replacement_tombstones).await {
            Ok(()) => invalidate_tombstone_caches(&state, &replacement_tombstones),
            Err(e) => {
                warn!(error = %e, count = replacement_tombstones.len(), "replacement tombstones failed; retrying in background");
                let state = state.clone();
                tokio::spawn(async move {
                    let mut backoff = std::time::Duration::from_millis(500);
                    loop {
                        tokio::time::sleep(backoff).await;
                        match state.metastore.upsert_tombstones(&replacement_tombstones).await {
                            Ok(()) => {
                                invalidate_tombstone_caches(&state, &replacement_tombstones);
                                return;
                            }
                            Err(e) => {
                                warn!(error = %e, "replacement tombstones still failing");
                                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                            }
                        }
                    }
                });
            }
        }
    }

    // ?refresh: cut the written streams' batches now and wait (bounded)
    // until they are published, so a search right after this response
    // sees the documents.
    if !to_refresh.is_empty() {
        let wait = async {
            for stream in &to_refresh {
                if let Err(e) = pipeline.flush_stream(stream).await {
                    warn!(stream, error = %e, "refresh flush failed");
                }
            }
        };
        if tokio::time::timeout(REFRESH_WAIT_LIMIT, wait).await.is_err() {
            warn!(
                streams = ?to_refresh,
                "refresh wait exceeded {}s; responding without it",
                REFRESH_WAIT_LIMIT.as_secs()
            );
        }
    }

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
    Ok(json!({
        "took": started.elapsed().as_millis() as u64,
        "errors": errors,
        "items": items,
    }))
}

pub(crate) fn error_response(status: StatusCode, reason: &str) -> Response {
    (
        status,
        Json(json!({
            "error": {"type": "illegal_argument_exception", "reason": reason},
            "status": status.as_u16(),
        })),
    )
        .into_response()
}
