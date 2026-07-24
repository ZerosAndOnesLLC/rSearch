use std::sync::Arc;

use rsearch_common::config::RsearchConfig;
use rsearch_common::role::Role;
use rsearch_ingest::IngestPipeline;
use rsearch_metastore::Metastore;
use rsearch_search::SearchService;

/// Shared server state available to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub cluster_name: String,
    pub node_id: String,
    pub roles: Arc<Vec<Role>>,
    pub metastore: Metastore,
    /// Present only on nodes running the ingest role.
    pub pipeline: Option<IngestPipeline>,
    /// Present only on nodes running the search role.
    pub search: Option<Arc<SearchService>>,
    pub auth: crate::auth::AuthState,
    /// Mirror of control.allow_insecure_webhooks for the alerts API.
    pub allow_insecure_webhooks: bool,
    pub cors_allow_origin: String,
}

impl AppState {
    pub fn new(
        config: &RsearchConfig,
        roles: &[Role],
        metastore: Metastore,
        pipeline: Option<IngestPipeline>,
        search: Option<Arc<SearchService>>,
    ) -> Self {
        Self {
            cluster_name: config.node.cluster_name.clone(),
            node_id: config.node_id(),
            roles: Arc::new(roles.to_vec()),
            metastore,
            pipeline,
            search,
            auth: crate::auth::AuthState::default(),
            allow_insecure_webhooks: config.control.allow_insecure_webhooks,
            cors_allow_origin: config.http.cors_allow_origin.clone(),
        }
    }

    /// Record a security-relevant event to the `rsearch-audit` stream
    /// (when this node ingests) and the process log.
    pub async fn audit(&self, action: &str, subject: &str, detail: &str) {
        tracing::info!(action, subject, detail, "audit");
        if let Some(pipeline) = &self.pipeline {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let doc = serde_json::json!({
                "@timestamp": now_ms,
                "action": action,
                "subject": subject,
                "detail": detail,
                "node": self.node_id,
                "source": "audit",
            });
            let _ = pipeline.ingest_external("rsearch-audit", vec![doc]).await;
        }
    }
}
