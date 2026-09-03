use serde::{Deserialize, Serialize};

use crate::error::{Result, RsearchError};
use crate::role::Role;

/// Top-level configuration, loaded from an optional TOML file with
/// RSEARCH_-prefixed environment variable overrides (e.g.
/// RSEARCH_HTTP__BIND_ADDR overrides [http] bind_addr).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RsearchConfig {
    /// Node identity, roles, data directory, and advertise address.
    pub node: NodeConfig,
    /// HTTP API bind address, TLS, and CORS settings.
    pub http: HttpConfig,
    /// Object storage backend (fs, s3, or replicated) settings.
    pub storage: StorageConfig,
    /// Postgres metastore connection settings.
    pub metastore: MetastoreConfig,
    /// Ingest batching, queue, and WAL settings.
    pub ingest: IngestConfig,
    /// Search-node settings (split cache budget).
    pub search: SearchConfig,
    /// Control-plane (leader) job settings.
    pub control: ControlConfig,
    /// Syslog/GELF input listener settings.
    pub inputs: InputsConfig,
    /// Node-to-node settings for the replicated storage backend.
    pub cluster: ClusterConfig,
}

impl Default for RsearchConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            http: HttpConfig::default(),
            storage: StorageConfig::default(),
            metastore: MetastoreConfig::default(),
            ingest: IngestConfig::default(),
            search: SearchConfig::default(),
            control: ControlConfig::default(),
            inputs: InputsConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

/// Node-to-node settings for the replicated storage backend.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    /// Shared bearer token authenticating internal peer requests
    /// (object transfer/replication). Required when the replicated
    /// storage backend is enabled; compared in constant time.
    pub internal_token: String,
    /// PEM bundle for a private cluster CA trusted for peer TLS. Empty
    /// uses the public webpki roots — set this when node certificates
    /// are signed by an internal CA.
    pub peer_ca_file: String,
}

/// Built-in log input listeners (syslog and GELF).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InputsConfig {
    /// Syslog (UDP/TCP) listener settings.
    pub syslog: SyslogInputConfig,
    /// GELF (TCP) listener settings.
    pub gelf: GelfInputConfig,
}

/// Syslog input listener (RFC3164/RFC5424 over UDP and TCP).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyslogInputConfig {
    /// Whether the syslog listeners run at all. Default false.
    pub enabled: bool,
    /// UDP bind address; empty disables UDP.
    pub bind_udp: String,
    /// TCP bind address (newline-framed); empty disables TCP.
    pub bind_tcp: String,
    /// TLS for the TCP listener (FIPS provider); both paths set = enabled.
    pub tls_cert_path: String,
    /// TLS private key path for the TCP listener.
    pub tls_key_path: String,
    /// Stream syslog messages are routed to by default.
    pub stream: String,
}

impl Default for SyslogInputConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_udp: "0.0.0.0:5514".to_string(),
            bind_tcp: "0.0.0.0:5514".to_string(),
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            stream: "syslog".to_string(),
        }
    }
}

/// GELF input listener (Graylog Extended Log Format over TCP).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GelfInputConfig {
    /// Whether the GELF listener runs at all. Default false.
    pub enabled: bool,
    /// TCP bind address (null-byte-framed GELF).
    pub bind_tcp: String,
    /// TLS certificate path; both paths set = TLS enabled.
    pub tls_cert_path: String,
    /// TLS private key path for the listener.
    pub tls_key_path: String,
    /// Stream GELF messages are routed to by default.
    pub stream: String,
}

impl Default for GelfInputConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_tcp: "0.0.0.0:12201".to_string(),
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            stream: "gelf".to_string(),
        }
    }
}

/// Leader-run control-plane jobs: merge, GC, orphan sweeps, repair.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlConfig {
    /// Seconds between control-job ticks on the leader.
    pub interval_secs: u64,
    /// Merged splits aim for this size (MB): published splits smaller
    /// than this are merge candidates, and a merge group is cut once its
    /// combined size crosses it, so the output is immediately ineligible
    /// for further merging (#49). Splits at or above it are left alone.
    pub merge_target_mb: i64,
    /// Deprecated older name for the merge-candidate ceiling. The
    /// effective target is `max(merge_target_mb, merge_min_mb)`, so a
    /// config that raised this keeps its behavior; prefer
    /// `merge_target_mb`.
    pub merge_min_mb: i64,
    /// Max splits combined per merge operation.
    pub merge_max_group: usize,
    /// Max merge operations run (serially) per control tick. Merge
    /// capacity must exceed cluster-wide split creation or split count —
    /// and with it query cost — grows without bound (#61); one merge per
    /// tick was break-even on a 4-stream cluster. Raising this drains
    /// backlogs faster at the cost of a longer tick, which delays the
    /// tick's other jobs. Merges stay serial so peak memory stays one
    /// writer budget regardless of this setting.
    pub merges_per_tick: usize,
    /// Seconds a split stays marked_for_delete before storage deletion,
    /// letting in-flight searches finish.
    pub gc_grace_secs: f64,
    /// Allow plaintext http:// alert webhooks (trusted networks only).
    /// Default false: webhooks must be https and must not target
    /// loopback/link-local/private addresses.
    pub allow_insecure_webhooks: bool,
    /// Seconds a split may sit in `staged` before an orphan sweep marks
    /// it for deletion (crash between stage and publish).
    pub staged_orphan_secs: f64,
    /// Replicated backend: a holder whose heartbeat is older than this
    /// counts as gone for replication purposes, making its objects repair
    /// candidates. Deliberately much shorter than dead-node expiry (which
    /// only governs registry row cleanup) — with factor 2, every second
    /// of this window is one failure away from data loss.
    pub repair_stale_secs: f64,
    /// Warn (every control tick) when a node has been draining longer
    /// than this — a long-lived draining flag is usually a forgotten
    /// DELETE /_rsearch/nodes/{id}/drain, and the node silently takes no
    /// writes or repair copies while it lasts.
    pub drain_warn_secs: f64,
    /// Document-mode compaction trigger: a stream whose tombstone count
    /// reaches this has its splits rewritten without the hidden versions.
    pub compact_min_tombstones: i64,
    /// Document-mode compaction trigger: a stream whose oldest tombstone
    /// is older than this is compacted regardless of count — the bound on
    /// how long a deleted document physically survives in storage.
    pub compact_max_age_secs: f64,
    /// Max splits rewritten (or marked up to date) per compaction tick.
    pub compact_splits_per_tick: i64,
    /// Max published splits written under an older layout version that
    /// are rewritten to the current one per control tick (newest data
    /// first), so an upgraded cluster converges on one analyzer and every
    /// split gains the `.keyword` view. 0 disables the pass.
    pub schema_upgrade_splits_per_tick: i64,
    /// Tombstones are purged from the metastore only once no split can
    /// still hold a version they hide *and* they are at least this old —
    /// the age covers documents still buffered on an ingest node whose
    /// split has not been cut yet.
    pub tombstone_purge_grace_secs: f64,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            interval_secs: 15,
            merge_target_mb: 512,
            merge_min_mb: 100,
            merge_max_group: 8,
            merges_per_tick: 4,
            gc_grace_secs: 600.0,
            allow_insecure_webhooks: false,
            staged_orphan_secs: 3600.0,
            repair_stale_secs: 300.0,
            drain_warn_secs: 3600.0,
            compact_min_tombstones: 1_000,
            compact_max_age_secs: 3_600.0,
            compact_splits_per_tick: 8,
            schema_upgrade_splits_per_tick: 2,
            tombstone_purge_grace_secs: 3_600.0,
        }
    }
}

/// Search-node settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchConfig {
    /// Local split-cache budget, in megabytes.
    pub cache_max_mb: u64,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self { cache_max_mb: 4096 }
    }
}

/// Identity and local layout of this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Cluster name reported by the compatibility API.
    pub cluster_name: String,
    /// Stable node identifier; defaults to the hostname.
    pub id: Option<String>,
    /// Roles this node runs when not overridden on the command line.
    pub roles: Vec<Role>,
    /// Directory for node-local state (WAL, split cache, staging).
    pub data_dir: String,
    /// Address other nodes use to reach this node's HTTP API, e.g.
    /// "node1.internal:9200" or a full "https://…" URL. Empty falls back
    /// to http.bind_addr — fine for single-node, but a multi-node cluster
    /// must set it (0.0.0.0 is not dialable by peers).
    pub advertise_addr: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            cluster_name: "rsearch".to_string(),
            id: None,
            roles: Role::ALL.to_vec(),
            data_dir: "./data".to_string(),
            advertise_addr: String::new(),
        }
    }
}

/// HTTP API server settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    /// Listen address for the HTTP API. Default "0.0.0.0:9200".
    pub bind_addr: String,
    /// TLS settings for the HTTP listener.
    pub tls: TlsConfig,
    /// Value for Access-Control-Allow-Origin. Defaults to "*"; set to a
    /// specific origin to restrict browser access (auth is header-based,
    /// so credentials are never auto-attached, but restricting is safer).
    pub cors_allow_origin: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9200".to_string(),
            tls: TlsConfig::default(),
            cors_allow_origin: "*".to_string(),
        }
    }
}

/// TLS material for the HTTP listener (FIPS rustls provider).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    /// Serve HTTPS instead of plain HTTP. Default false.
    pub enabled: bool,
    /// Path to the PEM certificate chain.
    pub cert_path: String,
    /// Path to the PEM private key.
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

/// Object storage settings shared by all backends.
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
    /// Signing region. Any non-empty string works for MinIO/S3-compatible
    /// stores; defaults to us-east-1 when unset.
    pub region: String,
    /// Static credentials for self-hosted / air-gapped deployments
    /// (set here or via RSEARCH_STORAGE__ACCESS_KEY_ID / __SECRET_ACCESS_KEY).
    /// When empty, the AWS credential chain is used (env, profile,
    /// task role, IMDS) — intended for real AWS with IAM roles.
    pub access_key_id: String,
    /// Secret half of the static credentials; see access_key_id.
    pub secret_access_key: String,
    /// Replicated backend: copies kept per object across storage nodes.
    pub replication_factor: usize,
    /// Replicated backend: copies that must succeed before a write is
    /// acknowledged. 0 = auto: min(2, replication_factor). Repair closes
    /// the gap to the full replication factor in the background.
    pub write_quorum: usize,
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
            region: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            replication_factor: 2,
            write_quorum: 0,
        }
    }
}

impl StorageConfig {
    /// Copies required before a write is acknowledged: the configured
    /// quorum clamped to the replication factor, with 0 meaning auto
    /// (min(2, replication_factor)).
    pub fn effective_write_quorum(&self) -> usize {
        let quorum = if self.write_quorum == 0 {
            self.replication_factor.min(2)
        } else {
            self.write_quorum
        };
        quorum.min(self.replication_factor).max(1)
    }
}

/// Postgres metastore connection settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetastoreConfig {
    /// Postgres connection URL; empty means read DATABASE_URL.
    pub database_url: String,
    /// Connection pool size. Default 10.
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

/// Ingest pipeline batching, queueing, and WAL settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    /// Max docs buffered per stream before a split is cut.
    pub max_batch_docs: usize,
    /// Max seconds a batch may age before a split is cut.
    pub max_batch_secs: u64,
    /// Same bound for document-mode streams, which trade smaller splits
    /// for a tighter time-to-searchable (a user saves a record and
    /// searches for it). `?refresh=wait_for` forces an immediate cut.
    pub document_max_batch_secs: u64,
    /// Bound on the in-flight ingest queue (documents per stream) before
    /// per-item 429s are returned.
    pub queue_capacity: usize,
    /// Tantivy writer heap per stream worker, in megabytes.
    pub memory_budget_mb: usize,
    /// WAL segment rotation size, in megabytes.
    pub wal_segment_mb: u64,
    /// Spread `/_bulk` batches round-robin across live ingest peers
    /// instead of indexing every batch on the node the client happens to
    /// hold a connection to. Takes effect only when
    /// `cluster.internal_token` is set (the handoff authenticates with
    /// it); with no token or no live peers, batches stay local.
    pub balance_bulk: bool,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_batch_docs: 500_000,
            max_batch_secs: 30,
            document_max_batch_secs: 5,
            queue_capacity: 100_000,
            memory_budget_mb: 256,
            wal_segment_mb: 64,
            balance_bulk: true,
        }
    }
}

impl RsearchConfig {
    /// Known top-level config sections. Only `RSEARCH_<SECTION>__…` env
    /// vars targeting one of these are consumed, so an unrelated
    /// `RSEARCH_*` variable in the environment never crashes startup
    /// (`deny_unknown_fields` still catches typos *within* a section).
    const SECTIONS: [&str; 9] = [
        "NODE", "HTTP", "STORAGE", "METASTORE", "INGEST", "SEARCH", "CONTROL", "INPUTS",
        "CLUSTER",
    ];

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
                .try_parsing(true)
                // Feed a pre-filtered env map so stray RSEARCH_-prefixed
                // vars that don't target a known section (RSEARCH_TEST_*,
                // RSEARCH_HOME, …) never reach deny_unknown_fields (M14).
                .source(Some(filtered_rsearch_env())),
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

    /// Configured node id, falling back to "rsearch-node" when neither
    /// config nor hostname provided one.
    pub fn node_id(&self) -> String {
        self.node
            .id
            .clone()
            .unwrap_or_else(|| "rsearch-node".to_string())
    }

    /// URL peers use to reach this node, derived from node.advertise_addr
    /// (falling back to http.bind_addr) with the scheme implied by the TLS
    /// setting when the address doesn't carry one. This is what the
    /// heartbeat publishes to the nodes table.
    pub fn advertise_url(&self) -> String {
        let addr = if self.node.advertise_addr.is_empty() {
            &self.http.bind_addr
        } else {
            &self.node.advertise_addr
        };
        if addr.contains("://") {
            addr.clone()
        } else if self.http.tls.enabled {
            format!("https://{addr}")
        } else {
            format!("http://{addr}")
        }
    }
}

/// Collect `RSEARCH_<SECTION>__…` environment variables for the known
/// sections only. Everything else with the `RSEARCH_` prefix is ignored,
/// so unrelated vars can't fail config deserialization.
fn filtered_rsearch_env() -> ::config::Map<String, String> {
    let mut map = ::config::Map::new();
    for (key, value) in std::env::vars() {
        if let Some(rest) = key.strip_prefix("RSEARCH_") {
            let section = rest.split("__").next().unwrap_or("");
            if RsearchConfig::SECTIONS.contains(&section) {
                map.insert(key, value);
            }
        }
    }
    map
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

    #[test]
    fn advertise_url_falls_back_and_derives_scheme() {
        let mut cfg = RsearchConfig::default();
        assert_eq!(cfg.advertise_url(), "http://0.0.0.0:9200");
        cfg.node.advertise_addr = "node1.internal:9200".to_string();
        assert_eq!(cfg.advertise_url(), "http://node1.internal:9200");
        cfg.http.tls.enabled = true;
        assert_eq!(cfg.advertise_url(), "https://node1.internal:9200");
        cfg.node.advertise_addr = "http://behind-proxy:8080".to_string();
        assert_eq!(cfg.advertise_url(), "http://behind-proxy:8080");
    }

    #[test]
    fn write_quorum_auto_and_clamping() {
        let mut storage = StorageConfig::default();
        // Auto: min(2, rf).
        assert_eq!(storage.effective_write_quorum(), 2);
        storage.replication_factor = 1;
        assert_eq!(storage.effective_write_quorum(), 1);
        storage.replication_factor = 3;
        assert_eq!(storage.effective_write_quorum(), 2);
        // Explicit quorum clamps to the replication factor, floor 1.
        storage.write_quorum = 5;
        assert_eq!(storage.effective_write_quorum(), 3);
        storage.write_quorum = 1;
        assert_eq!(storage.effective_write_quorum(), 1);
    }

    #[test]
    fn stray_rsearch_env_var_does_not_crash_load() {
        // A prefixed var that targets no known section must be ignored,
        // not error via deny_unknown_fields (M14). temp-env serializes
        // env-mutating tests and restores the vars afterwards.
        temp_env::with_vars(
            [
                ("RSEARCH_TEST_DATABASE_URL", Some("postgres://x")),
                ("RSEARCH_HOME", Some("/tmp")),
            ],
            || {
                let cfg =
                    RsearchConfig::load(None).expect("stray RSEARCH_ vars must not crash load");
                assert_eq!(cfg.http.bind_addr, "0.0.0.0:9200");
            },
        );
    }
}
