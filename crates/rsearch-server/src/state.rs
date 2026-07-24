use std::sync::Arc;

use rsearch_common::config::RsearchConfig;
use rsearch_common::role::Role;
use rsearch_ingest::IngestPipeline;
use rsearch_metastore::Metastore;

/// Shared server state available to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub cluster_name: String,
    pub node_id: String,
    pub roles: Arc<Vec<Role>>,
    pub metastore: Metastore,
    /// Present only on nodes running the ingest role.
    pub pipeline: Option<IngestPipeline>,
}

impl AppState {
    pub fn new(
        config: &RsearchConfig,
        roles: &[Role],
        metastore: Metastore,
        pipeline: Option<IngestPipeline>,
    ) -> Self {
        Self {
            cluster_name: config.node.cluster_name.clone(),
            node_id: config.node_id(),
            roles: Arc::new(roles.to_vec()),
            metastore,
            pipeline,
        }
    }
}
