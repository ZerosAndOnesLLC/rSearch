use serde::{Deserialize, Serialize};

/// Lifecycle state of a split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitState {
    /// Uploaded to storage but not yet visible to search.
    Staged,
    /// Live: visible to search queries.
    Published,
    /// Retired; the janitor deletes the object then the row.
    MarkedForDelete,
}

impl SplitState {
    /// The snake_case string stored in the `state` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            SplitState::Staged => "staged",
            SplitState::Published => "published",
            SplitState::MarkedForDelete => "marked_for_delete",
        }
    }
}

impl std::str::FromStr for SplitState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "staged" => Ok(SplitState::Staged),
            "published" => Ok(SplitState::Published),
            "marked_for_delete" => Ok(SplitState::MarkedForDelete),
            other => Err(format!("unknown split state '{other}'")),
        }
    }
}

/// How a stream treats writes to an existing `_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamMode {
    /// Append-only log store: every write is a new document; `delete` and
    /// `update` are rejected. The default.
    Log,
    /// Document index: `index` on an existing `_id` replaces it, `delete`
    /// and `update` work, reads filter tombstoned versions.
    Document,
}

impl StreamMode {
    /// The mode's wire/storage name.
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamMode::Log => "log",
            StreamMode::Document => "document",
        }
    }

    /// Parse the wire/storage name.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "log" => Some(StreamMode::Log),
            "document" => Some(StreamMode::Document),
            _ => None,
        }
    }
}

/// A stream (index) row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StreamRecord {
    /// Primary key; splits reference it via `stream_id`.
    pub id: i64,
    /// Unique stream (index) name.
    pub name: String,
    /// ES-style field mapping JSON the index schema is built from.
    pub mapping: serde_json::Value,
    /// Retention window in hours; None = keep forever.
    pub retention_hours: Option<i32>,
    /// Raw mode string; parse via [`StreamRecord::mode`].
    pub mode: String,
}

impl StreamRecord {
    /// Parsed [`StreamMode`]; unknown strings fall back to `Log`.
    pub fn mode(&self) -> StreamMode {
        StreamMode::parse(&self.mode).unwrap_or(StreamMode::Log)
    }

    /// Whether this stream is a document-mode index.
    pub fn is_document_mode(&self) -> bool {
        self.mode() == StreamMode::Document
    }
}

/// A split (immutable index file) row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SplitRecord {
    /// Primary key.
    pub id: i64,
    /// Globally unique split identifier.
    pub split_id: String,
    /// Owning stream's `StreamRecord::id`.
    pub stream_id: i64,
    /// Raw state string; parse via [`SplitRecord::state`].
    pub state: String,
    /// Object key of the split file in storage.
    pub storage_key: String,
    /// Number of documents in the split.
    pub doc_count: i64,
    /// Split file size in bytes.
    pub size_bytes: i64,
    /// Earliest document timestamp (epoch millis, inclusive).
    pub time_start_millis: i64,
    /// Latest document timestamp (epoch millis, inclusive).
    pub time_end_millis: i64,
    /// Byte length of the split file's footer metadata, so readers can
    /// open the split with ranged reads of just the tail.
    pub footer_len: i64,
    /// Id of the node that built the split, when known.
    pub created_by: Option<String>,
    /// Lowest `_seq` in the split; None for legacy splits without ids.
    pub seq_min: Option<i64>,
    /// Highest `_seq` in the split; None for legacy splits without ids.
    pub seq_max: Option<i64>,
    /// Highest tombstone `seq` applied when the split was built (0 for
    /// ingest-built splits: nothing applied yet).
    pub tombstone_seq_applied: i64,
    /// Split layout version it was built under (see
    /// `rsearch_index::CURRENT_SCHEMA_VERSION`); 0 for rows registered
    /// before the column existed.
    pub schema_version: i32,
}

/// A split to register (see `Metastore::stage_split`).
#[derive(Debug, Clone)]
pub struct NewSplit<'a> {
    /// Globally unique split identifier.
    pub split_id: &'a str,
    /// Owning stream id.
    pub stream_id: i64,
    /// Object key in storage.
    pub storage_key: &'a str,
    /// Documents in the split.
    pub doc_count: i64,
    /// Split file size.
    pub size_bytes: i64,
    /// Earliest document timestamp, epoch millis.
    pub time_start_millis: i64,
    /// Latest document timestamp, epoch millis.
    pub time_end_millis: i64,
    /// Footer metadata length.
    pub footer_len: i64,
    /// Building node id.
    pub created_by: Option<&'a str>,
    /// Lowest `_seq` (None when the split has no ids).
    pub seq_min: Option<i64>,
    /// Highest `_seq` (None when the split has no ids).
    pub seq_max: Option<i64>,
    /// Highest tombstone seq applied while building.
    pub tombstone_seq_applied: i64,
    /// Split layout version it was built under.
    pub schema_version: i32,
}

impl SplitRecord {
    /// Parsed [`SplitState`]; unknown strings fall back to `Staged`.
    pub fn state(&self) -> SplitState {
        self.state.parse().unwrap_or(SplitState::Staged)
    }
}

/// Per-stream rollup over published splits (for `_cat/indices`).
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct StreamStats {
    /// Stream name.
    pub name: String,
    /// Retention window in hours; None = keep forever.
    pub retention_hours: Option<i32>,
    /// Stream mode (`log` | `document`).
    pub mode: String,
    /// Number of published splits.
    pub split_count: i64,
    /// Total documents across published splits.
    pub doc_count: i64,
    /// Total split bytes across published splits.
    pub size_bytes: i64,
}

/// A storage object with fewer live copies than the replication factor.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UnderReplicatedKey {
    /// Object key that needs more copies.
    pub storage_key: String,
    /// Copies on nodes whose heartbeat is within the staleness threshold.
    pub live_holders: i64,
}

/// A registered cluster node (for `_cat/nodes` and placement).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NodeRecord {
    /// Unique node id.
    pub id: String,
    /// Roles the node serves (e.g. ingest, search, control).
    pub roles: Vec<String>,
    /// Advertised peer address; None if the node never announced one.
    pub address: Option<String>,
    /// Seconds since the node's last heartbeat.
    pub heartbeat_age_secs: f64,
    /// Draining nodes keep serving reads but receive no new object copies;
    /// the control leader copies their objects off (12.6).
    pub draining: bool,
    /// Seconds since the drain began; None when not draining. Surfaces
    /// long-lived (possibly forgotten) draining flags (#4).
    pub draining_since_secs: Option<f64>,
}
