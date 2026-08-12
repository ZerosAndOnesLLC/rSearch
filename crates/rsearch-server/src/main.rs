//! rSearch server binary: role-driven node (ingest, search, control)
//! exposing the OpenSearch- and Loki-compatible HTTP APIs.

mod admin_api;
mod alerts_api;
mod auth;
mod auth_api;
mod bulk_api;
mod control;
mod internal_api;
mod loki_api;
mod metrics;
mod placement;
mod routes;
mod search_api;
mod state;
mod webhook;


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

    // An unreachable metastore at startup is retried in-process instead
    // of exiting: a fatal exit turns any Postgres blip or slow dependency
    // start into a docker-level crash loop, and after a host reboot the
    // whole cluster stays down until an operator intervenes (#14). A
    // genuinely wrong DATABASE_URL surfaces as this same warning
    // repeating — the error text carries the cause either way.
    let metastore = {
        let mut delay = Duration::from_secs(1);
        loop {
            match Metastore::connect(&config.metastore).await {
                Ok(metastore) => break metastore,
                Err(e) => {
                    warn!(
                        error = %e,
                        retry_in_secs = delay.as_secs(),
                        "connecting to metastore failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    };
    let storage: std::sync::Arc<dyn rsearch_storage::Storage> =
        if config.storage.backend == "replicated" {
            if config.cluster.internal_token.is_empty() {
                anyhow::bail!(
                    "storage.backend = \"replicated\" requires cluster.internal_token \
                     (generate one with `openssl rand -hex 32`)"
                );
            }
            // A wildcard advertise address makes peers dial themselves:
            // self-pushes would count toward the quorum and the cluster
            // would ack writes with a single physical copy. Refuse to
            // start rather than run with silent zero redundancy.
            let advertise = config.advertise_url();
            let dialable = url::Url::parse(&advertise)
                .ok()
                .and_then(|u| {
                    u.host().map(|h| match h {
                        url::Host::Ipv4(ip) => !ip.is_unspecified(),
                        url::Host::Ipv6(ip) => !ip.is_unspecified(),
                        url::Host::Domain(_) => true,
                    })
                })
                .unwrap_or(false);
            if !dialable {
                anyhow::bail!(
                    "storage.backend = \"replicated\" requires a peer-dialable \
                     node.advertise_addr (got '{advertise}'; 0.0.0.0/[::] is not dialable)"
                );
            }
            let ca = (!config.cluster.peer_ca_file.is_empty())
                .then_some(config.cluster.peer_ca_file.as_str());
            // Re-announce local files whose keys the placement table still
            // knows: a node rejoining after registry expiry has real
            // copies on disk but may have lost its rows. The if-known
            // guard means cluster-deleted objects are never resurrected.
            {
                let fs = rsearch_storage::FsStorage::new(config.storage.root.clone());
                let metastore = metastore.clone();
                let node_id = config.node_id();
                tokio::spawn(async move {
                    use rsearch_storage::Storage;
                    let keys = match fs.list("").await {
                        Ok(keys) => keys,
                        Err(e) => {
                            warn!(error = %e, "rejoin scan: listing local objects failed");
                            return;
                        }
                    };
                    let mut announced = 0u64;
                    for key in keys {
                        let Ok(size) = fs.size(&key).await else { continue };
                        match metastore
                            .record_object_location_if_known(&key, &node_id, size as i64)
                            .await
                        {
                            Ok(true) => announced += 1,
                            Ok(false) => {}
                            Err(e) => {
                                warn!(key, error = %e, "rejoin scan: record failed");
                            }
                        }
                    }
                    if announced > 0 {
                        info!(announced, "rejoin scan: re-announced local object copies");
                    }
                });
            }
            std::sync::Arc::new(rsearch_storage::ReplicatedStorage::new(
                rsearch_storage::FsStorage::new(config.storage.root.clone()),
                std::sync::Arc::new(placement::MetastorePlacement::new(metastore.clone())),
                rsearch_storage::PeerClient::new(&config.cluster.internal_token, ca)
                    .map_err(|e| anyhow::anyhow!(e))
                    .context("building peer client")?,
                config.node_id(),
                config.storage.replication_factor,
                config.storage.effective_write_quorum(),
            ))
        } else {
            rsearch_storage::from_config(&config.storage)
                .await
                .context("initializing storage backend")?
        };

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
        // Load routing rules before any ingest endpoint or input starts,
        // so startup-window docs are routed correctly.
        pipeline
            .warm_routing_rules()
            .await
            .context("loading routing rules")?;
        rsearch_ingest::spawn_inputs(&config.inputs, pipeline.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))
            .context("starting log inputs")?;
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

    // Control role: contend for leadership and run background jobs. The
    // metrics handle is shared with /metrics via AppState.
    let control_metrics = if roles.contains(&Role::Control) {
        let metrics = std::sync::Arc::new(metrics::ControlMetrics::default());
        let plane = control::ControlPlane::new(
            &config,
            metastore.clone(),
            storage.clone(),
            metrics.clone(),
        )
        .context("initializing control plane")?;
        tokio::spawn(plane.run());
        Some(metrics)
    } else {
        None
    };

    // Every node heartbeats its liveness row; the response carries the
    // draining flag so an operator drain reaches the node within one beat.
    let draining_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let metastore = metastore.clone();
        let node_id = config.node_id();
        let role_names: Vec<String> = roles.iter().map(ToString::to_string).collect();
        let address = config.advertise_url();
        let draining_flag = draining_flag.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                match metastore
                    .heartbeat(&node_id, &role_names, Some(&address))
                    .await
                {
                    Ok(draining) => {
                        use std::sync::atomic::Ordering;
                        if draining != draining_flag.swap(draining, Ordering::Relaxed) {
                            if draining {
                                warn!("node is draining: refusing new bulk ingest");
                            } else {
                                info!("drain cancelled: accepting bulk ingest again");
                            }
                        }
                    }
                    Err(e) => warn!(error = %e, "heartbeat failed"),
                }
            }
        });
    }

    let mut state = AppState::new(&config, &roles, metastore, pipeline, search);
    state.draining = draining_flag;
    state.control = control_metrics;
    if config.storage.backend == "replicated" {
        state.internal = Some(std::sync::Arc::new(internal_api::InternalState {
            fs: rsearch_storage::FsStorage::new(config.storage.root.clone()),
            token_digest: rsearch_common::crypto::token_digest(&config.cluster.internal_token),
            metastore: state.metastore.clone(),
            node_id: config.node_id(),
            client: rsearch_storage::PeerClient::new(
                &config.cluster.internal_token,
                (!config.cluster.peer_ca_file.is_empty())
                    .then_some(config.cluster.peer_ca_file.as_str()),
            )
            .map_err(|e| anyhow::anyhow!(e))
            .context("building peer client")?,
        }));
    }
    state.auth.spawn_refresher(state.metastore.clone());
    let app = routes::router(state);

    if config.http.tls.enabled {
        let tls_config = rsearch_common::tls::fips_server_config(&config.http.tls.cert_path, &config.http.tls.key_path)?;
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
