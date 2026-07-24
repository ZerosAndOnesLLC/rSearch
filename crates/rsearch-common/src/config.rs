use serde::{Deserialize, Serialize};

use crate::error::{Result, RsearchError};
use crate::role::Role;

/// Top-level configuration, loaded from an optional TOML file with
/// RSEARCH_-prefixed environment variable overrides (e.g.
/// RSEARCH_HTTP__BIND_ADDR overrides [http] bind_addr).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RsearchConfig {
    pub node: NodeConfig,
    pub http: HttpConfig,
    pub storage: StorageConfig,
    pub metastore: MetastoreConfig,
    pub ingest: IngestConfig,
}

impl Default for RsearchConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            http: HttpConfig::default(),
            storage: StorageConfig::default(),
            metastore: MetastoreConfig::default(),
            ingest: IngestConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Stable node identifier; defaults to the hostname.
    pub id: Option<String>,
    /// Roles this node runs when not overridden on the command line.
    pub roles: Vec<Role>,
    /// Directory for node-local state (WAL, split cache, staging).
    pub data_dir: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: None,
            roles: Role::ALL.to_vec(),
            data_dir: "./data".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    pub bind_addr: String,
    pub tls: TlsConfig,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9200".to_string(),
            tls: TlsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: String::new(),
            key_path: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// "fs" (local filesystem) or "s3" (S3 or any S3-compatible endpoint).
    pub backend: String,
    /// Root directory for the fs backend.
    pub root: String,
    /// Bucket name for the s3 backend.
    pub bucket: String,
    /// Custom endpoint URL for S3-compatible stores (MinIO); empty uses AWS.
    pub endpoint: String,
    /// Use path-style addressing (required by MinIO).
    pub force_path_style: bool,
    /// Use FIPS endpoints when talking to AWS S3.
    pub use_fips_endpoint: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: "fs".to_string(),
            root: "./data/storage".to_string(),
            bucket: String::new(),
            endpoint: String::new(),
            force_path_style: false,
            use_fips_endpoint: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetastoreConfig {
    /// Postgres connection URL; empty means read DATABASE_URL.
    pub database_url: String,
    pub max_connections: u32,
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            max_connections: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    /// Max docs buffered per stream before a split is cut.
    pub max_batch_docs: usize,
    /// Max seconds a batch may age before a split is cut.
    pub max_batch_secs: u64,
    /// Bound on the in-flight ingest queue before 429s are returned.
    pub queue_capacity: usize,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_batch_docs: 500_000,
            max_batch_secs: 30,
            queue_capacity: 64,
        }
    }
}

impl RsearchConfig {
    /// Load configuration: defaults <- optional TOML file <- RSEARCH_ env vars.
    pub fn load(file: Option<&str>) -> Result<Self> {
        let mut builder = ::config::Config::builder();
        if let Some(path) = file {
            builder = builder.add_source(::config::File::with_name(path).required(true));
        }
        builder = builder.add_source(
            ::config::Environment::with_prefix("RSEARCH")
                .prefix_separator("_")
                .separator("__")
                .try_parsing(true),
        );
        let cfg = builder
            .build()
            .map_err(|e| RsearchError::Config(e.to_string()))?;
        // Layer parsed sources over serde defaults so partial files work.
        let mut loaded: RsearchConfig = cfg
            .try_deserialize()
            .map_err(|e| RsearchError::Config(e.to_string()))?;
        if loaded.node.id.is_none() {
            loaded.node.id = hostname();
        }
        Ok(loaded)
    }

    pub fn node_id(&self) -> String {
        self.node
            .id
            .clone()
            .unwrap_or_else(|| "rsearch-node".to_string())
    }
}

fn hostname() -> Option<String> {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_load_without_file() {
        let cfg = RsearchConfig::load(None).unwrap();
        assert_eq!(cfg.http.bind_addr, "0.0.0.0:9200");
        assert_eq!(cfg.storage.backend, "fs");
        assert!(!cfg.http.tls.enabled);
    }
}
