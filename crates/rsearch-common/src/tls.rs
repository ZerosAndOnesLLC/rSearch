use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::{Result, RsearchError};

/// Build a rustls server config backed by the aws-lc-rs FIPS provider.
/// Fails hard if the resulting configuration is not FIPS-compliant.
/// Used by the HTTP API and the syslog/GELF TLS listeners.
pub fn fips_server_config(cert_path: &str, key_path: &str) -> Result<Arc<ServerConfig>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| RsearchError::Config(format!("reading certificate {cert_path}: {e}")))?
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| RsearchError::Config(format!("parsing certificate {cert_path}: {e}")))?;
    if certs.is_empty() {
        return Err(RsearchError::Config(format!(
            "no certificates found in {cert_path}"
        )));
    }
    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| RsearchError::Config(format!("reading private key {key_path}: {e}")))?;

    let provider = Arc::new(rustls::crypto::default_fips_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .map_err(|e| RsearchError::Config(format!("selecting TLS versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| RsearchError::Config(format!("building TLS config: {e}")))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    if !config.fips() {
        return Err(RsearchError::Config(
            "TLS configuration failed FIPS validation".to_string(),
        ));
    }
    Ok(Arc::new(config))
}
