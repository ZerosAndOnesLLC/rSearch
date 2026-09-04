use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::HeaderValue;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::bulk_api;
use crate::search_api;
use crate::state::AppState;

/// Bulk bodies can be large; cap at 100 MB.
const BULK_BODY_LIMIT: usize = 100 << 20;

pub fn router(state: AppState) -> Router {
    let cors_origin = Arc::new(
        HeaderValue::from_str(&state.cors_allow_origin)
            .unwrap_or_else(|_| HeaderValue::from_static("*")),
    );
    let router = Router::new()
        .route("/", get(search_api::root))
        .route("/health", get(health))
        .route("/_cluster/health", get(cluster_health))
        .route("/_cat/nodes", get(cat_nodes))
        .route("/_rsearch/stats", get(node_stats))
        .route("/metrics", get(crate::metrics::metrics))
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
        // Scroll API (#72): continue with POST/GET, free with DELETE.
        .route(
            "/_search/scroll",
            post(search_api::scroll_next)
                .get(search_api::scroll_next)
                .delete(search_api::clear_scroll),
        )
        .route(
            "/_search/scroll/{scroll_id}",
            post(search_api::scroll_next_by_path)
                .get(search_api::scroll_next_by_path)
                .delete(search_api::clear_scroll_by_path),
        )
        // Loki-compatible query API subset (#11): lets Grafana's built-in
        // Loki datasource and Logs Drilldown run against rSearch.
        .route("/ready", get(crate::loki_api::ready))
        .route(
            "/loki/api/v1/query_range",
            get(crate::loki_api::query_range).post(crate::loki_api::query_range),
        )
        .route(
            "/loki/api/v1/query",
            get(crate::loki_api::query_instant).post(crate::loki_api::query_instant),
        )
        .route(
            "/loki/api/v1/labels",
            get(crate::loki_api::labels).post(crate::loki_api::labels),
        )
        .route(
            "/loki/api/v1/label/{name}/values",
            get(crate::loki_api::label_values),
        )
        .route(
            "/loki/api/v1/series",
            get(crate::loki_api::series).post(crate::loki_api::series),
        )
        .route(
            "/loki/api/v1/index/volume",
            get(crate::loki_api::volume).post(crate::loki_api::volume),
        )
        .route(
            "/loki/api/v1/index/volume_range",
            get(crate::loki_api::volume_range).post(crate::loki_api::volume_range),
        )
        .route("/loki/api/v1/tail", get(crate::loki_api::tail))
        // Document APIs (document-mode streams, #34): ES clients reach for
        // these before _bulk.
        .route(
            "/{index}/_doc",
            post(crate::doc_api::post_doc),
        )
        .route(
            "/{index}/_doc/{id}",
            axum::routing::put(crate::doc_api::put_doc)
                .post(crate::doc_api::put_doc)
                .get(crate::doc_api::get_doc)
                .head(crate::doc_api::head_doc)
                .delete(crate::doc_api::delete_doc),
        )
        .route(
            "/{index}/_create/{id}",
            axum::routing::put(crate::doc_api::create_doc).post(crate::doc_api::create_doc),
        )
        .route("/{index}/_update/{id}", post(crate::doc_api::update_doc))
        .route("/{index}/_source/{id}", get(crate::doc_api::get_source))
        .route("/{index}/_delete_by_query", post(crate::doc_api::delete_by_query))
        .route(
            "/{index}/_mapping",
            get(search_api::get_mapping).put(search_api::put_mapping),
        )
        .route(
            "/{index}/_count",
            get(search_api::count).post(search_api::count),
        )
        .route("/{index}/_settings", get(search_api::get_settings))
        .route(
            "/{index}",
            axum::routing::put(search_api::put_index)
                .get(search_api::get_index)
                .head(search_api::head_index)
                .delete(search_api::delete_index),
        )
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
        .route(
            "/_rsearch/nodes/{id}/drain",
            post(crate::admin_api::drain_node).delete(crate::admin_api::undrain_node),
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
        // ES 8.x clients refuse to talk without the product header; CORS
        // lets the UI (separate origin) call the API with token auth.
        .layer(axum::middleware::from_fn_with_state(
            cors_origin,
            |axum::extract::State(origin): axum::extract::State<Arc<HeaderValue>>,
             request: axum::extract::Request,
             next: axum::middleware::Next| async move {
                if request.method() == axum::http::Method::OPTIONS {
                    return cors_headers(
                        axum::http::StatusCode::NO_CONTENT.into_response(),
                        &origin,
                    );
                }
                cors_headers(next.run(request).await, &origin)
            },
        ));
    // Peer endpoints authenticate with the cluster token, not user auth,
    // so they merge in outside the middleware stack above.
    let router = if state.internal.is_some() {
        router.merge(crate::internal_api::router())
    } else {
        router
    };
    // Same for the bulk handoff receiver (#19).
    let router = if state.bulk_forward.is_some() {
        router.route(
            "/_rsearch/internal/bulk",
            post(bulk_api::bulk_internal).layer(DefaultBodyLimit::max(BULK_BODY_LIMIT)),
        )
    } else {
        router
    };
    router.with_state(state)
}

fn cors_headers(
    mut response: axum::response::Response,
    origin: &HeaderValue,
) -> axum::response::Response {
    let headers = response.headers_mut();
    headers.insert(
        "x-elastic-product",
        HeaderValue::from_static("Elasticsearch"),
    );
    headers.insert("access-control-allow-origin", origin.clone());
    headers.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("authorization, content-type, x-api-key"),
    );
    response
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
    let bulk_balance = state.bulk_forward.as_ref().map(|f| {
        json!({
            "forwarded": f.forwarded.load(Ordering::Relaxed),
            "forward_fallbacks": f.forward_fallbacks.load(Ordering::Relaxed),
            "received": f.received.load(Ordering::Relaxed),
        })
    });
    Json(json!({
        "node": state.node_id,
        "ingest": ingest,
        "bulk_balance": bulk_balance,
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
                    "draining": n.draining,
                    "draining_since_secs": n.draining_since_secs,
                })
            })
            .collect(),
    ))
}
