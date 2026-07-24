mod routes;
mod tls;

use anyhow::Context;
use clap::Parser;
use rsearch_common::config::RsearchConfig;
use rsearch_common::role::{Role, parse_roles};
use tracing::info;

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

    let app = routes::router(&config, &roles);
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
