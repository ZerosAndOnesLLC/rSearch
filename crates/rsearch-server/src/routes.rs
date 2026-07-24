use axum::Router;
use axum::routing::get;
use serde_json::json;

use rsearch_common::config::RsearchConfig;
use rsearch_common::role::Role;

pub fn router(config: &RsearchConfig, roles: &[Role]) -> Router {
    let node_id = config.node_id();
    let roles: Vec<String> = roles.iter().map(ToString::to_string).collect();
    Router::new().route(
        "/health",
        get(move || {
            let body = json!({
                "status": "ok",
                "node": node_id,
                "roles": roles,
                "version": env!("CARGO_PKG_VERSION"),
            });
            async move { axum::Json(body) }
        }),
    )
}
