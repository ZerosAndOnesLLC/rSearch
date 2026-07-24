use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitState {
    Staged,
    Published,
    MarkedForDelete,
}

impl SplitState {
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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StreamRecord {
    pub id: i64,
    pub name: String,
    pub mapping: serde_json::Value,
    pub retention_hours: Option<i32>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SplitRecord {
    pub id: i64,
    pub split_id: String,
    pub stream_id: i64,
    pub state: String,
    pub storage_key: String,
    pub doc_count: i64,
    pub size_bytes: i64,
    pub time_start_millis: i64,
    pub time_end_millis: i64,
    pub footer_len: i64,
    pub created_by: Option<String>,
}

impl SplitRecord {
    pub fn state(&self) -> SplitState {
        self.state.parse().unwrap_or(SplitState::Staged)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NodeRecord {
    pub id: String,
    pub roles: Vec<String>,
    pub address: Option<String>,
    /// Seconds since the node's last heartbeat.
    pub heartbeat_age_secs: f64,
}
