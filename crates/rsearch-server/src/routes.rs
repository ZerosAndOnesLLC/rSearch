use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use rsearch_common::config::RsearchConfig;
use rsearch_common::role::Role;

/// Shared server state available to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub cluster_name: String,
    pub node_id: String,
    pub roles: Arc<Vec<Role>>,
}

impl AppState {
    pub fn new(config: &RsearchConfig, roles: &[Role]) -> Self {
        Self {
            cluster_name: config.node.cluster_name.clone(),
            node_id: config.node_id(),
            roles: Arc::new(roles.to_vec()),
        }
    }
}

pub fn router(config: &RsearchConfig, roles: &[Role]) -> Router {
    let state = AppState::new(config, roles);
    Router::new()
        .route("/health", get(health))
        .route("/_cluster/health", get(cluster_health))
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

/// OpenSearch-compatible cluster health. Single-node green stub until the
/// node registry lands (phase 3.3 / 7.x).
async fn cluster_health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "cluster_name": state.cluster_name,
        "status": "green",
        "timed_out": false,
        "number_of_nodes": 1,
        "number_of_data_nodes": 1,
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
