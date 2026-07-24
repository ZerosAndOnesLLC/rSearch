use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Build a rustls server config backed by the aws-lc-rs FIPS provider.
/// Fails hard if the resulting configuration is not FIPS-compliant.
pub fn server_config(cert_path: &str, key_path: &str) -> Result<Arc<ServerConfig>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .with_context(|| format!("reading certificate {cert_path}"))?
        .collect::<std::result::Result<_, _>>()
        .with_context(|| format!("parsing certificate {cert_path}"))?;
    ensure!(!certs.is_empty(), "no certificates found in {cert_path}");
    let key = PrivateKeyDer::from_pem_file(key_path)
        .with_context(|| format!("reading private key {key_path}"))?;

    let provider = Arc::new(rustls::crypto::default_fips_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .context("selecting TLS protocol versions")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building TLS server config")?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    ensure!(
        config.fips(),
        "TLS configuration failed FIPS validation (non-FIPS provider or parameters)"
    );
    Ok(Arc::new(config))
}
