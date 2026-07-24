use axum::extract::{DefaultBodyLimit, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::bulk_api;
use crate::state::AppState;

/// Bulk bodies can be large; cap at 100 MB.
const BULK_BODY_LIMIT: usize = 100 << 20;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/_cluster/health", get(cluster_health))
        .route("/_cat/nodes", get(cat_nodes))
        .route("/_rsearch/stats", get(node_stats))
        .route(
            "/_bulk",
            post(bulk_api::bulk_root).layer(DefaultBodyLimit::max(BULK_BODY_LIMIT)),
        )
        .route(
            "/{index}/_bulk",
            post(bulk_api::bulk_index).layer(DefaultBodyLimit::max(BULK_BODY_LIMIT)),
        )
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "cluster_name": state.cluster_name,
        "node": state.node_id,
        "roles": state.roles.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// OpenSearch-compatible cluster health, backed by the node registry.
async fn cluster_health(State(state): State<AppState>) -> Json<Value> {
    let nodes = state
        .metastore
        .live_nodes(30.0)
        .await
        .map(|n| n.len() as i64)
        .unwrap_or(1)
        .max(1);
    Json(json!({
        "cluster_name": state.cluster_name,
        "status": "green",
        "timed_out": false,
        "number_of_nodes": nodes,
        "number_of_data_nodes": nodes,
        "discovered_master": true,
        "active_primary_shards": 0,
        "active_shards": 0,
        "relocating_shards": 0,
        "initializing_shards": 0,
        "unassigned_shards": 0,
        "delayed_unassigned_shards": 0,
        "number_of_pending_tasks": 0,
        "number_of_in_flight_fetch": 0,
        "task_max_waiting_in_queue_millis": 0,
        "active_shards_percent_as_number": 100.0,
    }))
}

/// Node-local ingest counters (monotonic since process start).
async fn node_stats(State(state): State<AppState>) -> Json<Value> {
    use std::sync::atomic::Ordering;
    let ingest = state.pipeline.as_ref().map(|p| {
        let m = p.metrics();
        json!({
            "docs_enqueued": m.docs_enqueued.load(Ordering::Relaxed),
            "bytes_enqueued": m.bytes_enqueued.load(Ordering::Relaxed),
            "docs_indexed": m.docs_indexed.load(Ordering::Relaxed),
            "splits_published": m.splits_published.load(Ordering::Relaxed),
            "flush_failures": m.flush_failures.load(Ordering::Relaxed),
            "queue_depth": m.queue_depth.load(Ordering::Relaxed),
            "wal_outstanding": p.wal().outstanding(),
            "wal_segments": p.wal().segment_count(),
        })
    });
    Json(json!({
        "node": state.node_id,
        "ingest": ingest,
    }))
}

async fn cat_nodes(State(state): State<AppState>) -> Json<Value> {
    let nodes = state.metastore.list_nodes().await.unwrap_or_default();
    Json(Value::Array(
        nodes
            .into_iter()
            .map(|n| {
                json!({
                    "id": n.id,
                    "roles": n.roles,
                    "address": n.address,
                    "heartbeat_age_secs": n.heartbeat_age_secs,
                    "live": n.heartbeat_age_secs < 30.0,
                })
            })
            .collect(),
    ))
}
