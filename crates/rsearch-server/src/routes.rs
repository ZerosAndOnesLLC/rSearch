use axum::extract::{DefaultBodyLimit, State};
use axum::http::HeaderValue;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::bulk_api;
use crate::search_api;
use crate::state::AppState;

/// Bulk bodies can be large; cap at 100 MB.
const BULK_BODY_LIMIT: usize = 100 << 20;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(search_api::root))
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
        .route(
            "/{index}/_search",
            post(search_api::search).get(search_api::search),
        )
        .route("/_msearch", post(search_api::msearch))
        .route("/{index}/_mapping", get(search_api::get_mapping))
        .route("/{index}", axum::routing::put(search_api::put_index))
        .route("/_cat/indices", get(crate::admin_api::cat_indices))
        .route("/_rsearch/routing_rules", get(crate::admin_api::list_rules))
        .route(
            "/_rsearch/routing_rules/{name}",
            axum::routing::put(crate::admin_api::put_rule)
                .delete(crate::admin_api::delete_rule),
        )
        .route(
            "/_rsearch/streams/{name}/retention",
            axum::routing::put(crate::admin_api::put_retention),
        )
        .route("/_rsearch/login", post(crate::auth_api::login))
        .route("/_rsearch/alerts", get(crate::alerts_api::list_alerts))
        .route(
            "/_rsearch/alerts/{name}",
            axum::routing::put(crate::alerts_api::put_alert).delete(crate::alerts_api::delete_alert),
        )
        .route(
            "/_rsearch/users",
            get(crate::auth_api::list_users),
        )
        .route(
            "/_rsearch/users/{name}",
            axum::routing::put(crate::auth_api::put_user).delete(crate::auth_api::delete_user),
        )
        .route(
            "/_rsearch/api_keys",
            post(crate::auth_api::create_api_key).get(crate::auth_api::list_api_keys),
        )
        .route(
            "/_rsearch/api_keys/{name}",
            axum::routing::delete(crate::auth_api::delete_api_key),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::require,
        ))
        // ES 8.x clients refuse to talk without the product header.
        .layer(axum::middleware::map_response(
            |mut response: axum::response::Response| async {
                response.headers_mut().insert(
                    "x-elastic-product",
                    HeaderValue::from_static("Elasticsearch"),
                );
                response
            },
        ))
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
