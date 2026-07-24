mod bulk_api;
mod routes;
mod search_api;
mod state;
mod tls;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use rsearch_common::config::RsearchConfig;
use rsearch_common::role::{Role, parse_roles};
use rsearch_ingest::{IngestPipeline, PipelineConfig, Wal};
use rsearch_metastore::Metastore;
use tracing::{info, warn};

use crate::state::AppState;

#[derive(Parser, Debug)]
#[command(name = "rsearch", about = "FIPS-compliant log search server")]
struct Cli {
    /// Path to a TOML config file.
    #[arg(long, env = "RSEARCH_CONFIG")]
    config: Option<String>,

    /// Comma-separated roles to run: ingest,search,control (or "all").
    /// Overrides the config file.
    #[arg(long)]
    roles: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    rsearch_common::telemetry::init();

    let config = RsearchConfig::load(cli.config.as_deref()).context("loading configuration")?;
    let roles: Vec<Role> = match cli.roles.as_deref() {
        Some(s) => parse_roles(s).map_err(|e| anyhow::anyhow!(e))?,
        None => config.node.roles.clone(),
    };

    info!(
        node_id = %config.node_id(),
        roles = %roles.iter().map(ToString::to_string).collect::<Vec<_>>().join(","),
        bind = %config.http.bind_addr,
        version = env!("CARGO_PKG_VERSION"),
        "starting rsearch"
    );

    let metastore = Metastore::connect(&config.metastore)
        .await
        .context("connecting to metastore")?;
    let storage = rsearch_storage::from_config(&config.storage)
        .await
        .context("initializing storage backend")?;

    // Ingest role: open the WAL (replaying unpublished docs) and start the
    // indexer pipeline.
    let pipeline = if roles.contains(&Role::Ingest) {
        let data_dir = PathBuf::from(&config.node.data_dir);
        let (wal, replayed) = Wal::open(
            data_dir.join("wal"),
            config.ingest.wal_segment_mb << 20,
        )
        .context("opening WAL")?;
        let pipeline = IngestPipeline::new(
            PipelineConfig {
                max_batch_docs: config.ingest.max_batch_docs,
                max_batch_secs: config.ingest.max_batch_secs,
                queue_capacity: config.ingest.queue_capacity,
                work_dir: data_dir.join("staging"),
                memory_budget: config.ingest.memory_budget_mb << 20,
                node_id: config.node_id(),
            },
            storage.clone(),
            metastore.clone(),
            std::sync::Arc::new(wal),
        );
        let replay_count = pipeline
            .replay(replayed)
            .await
            .context("replaying WAL into pipeline")?;
        if replay_count > 0 {
            info!(replay_count, "WAL replay complete");
        }
        Some(pipeline)
    } else {
        None
    };

    // Search role: split cache + stateless search service.
    let search = if roles.contains(&Role::Search) {
        let cache = rsearch_index::SplitCache::new(
            PathBuf::from(&config.node.data_dir).join("cache/splits"),
            config.search.cache_max_mb << 20,
        )
        .context("initializing split cache")?;
        Some(std::sync::Arc::new(rsearch_search::SearchService::new(
            metastore.clone(),
            storage.clone(),
            std::sync::Arc::new(cache),
        )))
    } else {
        None
    };

    // Every node heartbeats its liveness row.
    {
        let metastore = metastore.clone();
        let node_id = config.node_id();
        let role_names: Vec<String> = roles.iter().map(ToString::to_string).collect();
        let address = config.http.bind_addr.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Err(e) = metastore
                    .heartbeat(&node_id, &role_names, Some(&address))
                    .await
                {
                    warn!(error = %e, "heartbeat failed");
                }
            }
        });
    }

    let app = routes::router(AppState::new(&config, &roles, metastore, pipeline, search));

    if config.http.tls.enabled {
        let tls_config = tls::server_config(&config.http.tls.cert_path, &config.http.tls.key_path)?;
        let addr: std::net::SocketAddr = config
            .http
            .bind_addr
            .parse()
            .with_context(|| format!("parsing bind address {}", config.http.bind_addr))?;
        info!(fips = true, "listening on https://{addr}");
        axum_server::bind_rustls(addr, axum_server::tls_rustls::RustlsConfig::from_config(tls_config))
            .serve(app.into_make_service())
            .await
            .context("serving https")?;
    } else {
        let listener = tokio::net::TcpListener::bind(&config.http.bind_addr)
            .await
            .with_context(|| format!("binding {}", config.http.bind_addr))?;
        info!("listening on http://{} (TLS disabled — dev only)", config.http.bind_addr);
        axum::serve(listener, app).await.context("serving http")?;
    }
    Ok(())
}
