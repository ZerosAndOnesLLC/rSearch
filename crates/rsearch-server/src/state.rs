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
    /// Peer-transfer state, present only on replicated-backend nodes.
    pub internal: Option<Arc<crate::internal_api::InternalState>>,
    /// Set when the operator drains this node (learned via heartbeat):
    /// bulk ingest is refused so the WAL empties out ahead of shutdown.
    pub draining: Arc<std::sync::atomic::AtomicBool>,
    /// Repair/drain/leadership counters for /metrics, shared with the
    /// control loop. Present only on nodes running the control role.
    pub control: Option<Arc<crate::metrics::ControlMetrics>>,
    /// Short-TTL cache of all stream names for wildcard resolution — an
    /// `_msearch` refresh resolves `logs-*` once per TTL instead of one
    /// `list_streams` query per header/body pair per viewer.
    pub stream_names: Arc<std::sync::Mutex<Option<(Arc<Vec<String>>, std::time::Instant)>>>,
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
            internal: None,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            control: None,
            stream_names: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// All stream names, cached briefly (see `stream_names`).
    pub async fn cached_stream_names(
        &self,
    ) -> Result<Arc<Vec<String>>, rsearch_metastore::MetastoreError> {
        const TTL: std::time::Duration = std::time::Duration::from_secs(5);
        if let Some((names, at)) = self.stream_names.lock().unwrap().as_ref()
            && at.elapsed() < TTL
        {
            return Ok(names.clone());
        }
        let names: Arc<Vec<String>> = Arc::new(
            self.metastore
                .list_streams()
                .await?
                .into_iter()
                .map(|s| s.name)
                .collect(),
        );
        *self.stream_names.lock().unwrap() = Some((names.clone(), std::time::Instant::now()));
        Ok(names)
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
