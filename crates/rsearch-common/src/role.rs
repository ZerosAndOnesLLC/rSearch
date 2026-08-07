use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A function a node performs. One process can hold any combination; a
/// single node running all three is a complete cluster of one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Accepts writes: runs the WAL, batching, and split building.
    Ingest,
    /// Serves queries: downloads and caches splits, executes searches.
    Search,
    /// Runs leader-elected cluster jobs: merge, GC, repair, alerts.
    Control,
}

impl Role {
    /// Every role, in canonical order; the default role set for a node.
    pub const ALL: [Role; 3] = [Role::Ingest, Role::Search, Role::Control];
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Ingest => write!(f, "ingest"),
            Role::Search => write!(f, "search"),
            Role::Control => write!(f, "control"),
        }
    }
}

impl FromStr for Role {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "ingest" => Ok(Role::Ingest),
            "search" => Ok(Role::Search),
            "control" => Ok(Role::Control),
            other => Err(format!(
                "unknown role '{other}' (expected ingest, search, or control)"
            )),
        }
    }
}

/// Parse a comma-separated role list, e.g. "ingest,search,control" or "all".
pub fn parse_roles(s: &str) -> Result<Vec<Role>, String> {
    if s.trim().eq_ignore_ascii_case("all") {
        return Ok(Role::ALL.to_vec());
    }
    let mut roles = Vec::new();
    for part in s.split(',') {
        let role = part.parse::<Role>()?;
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    if roles.is_empty() {
        return Err("at least one role is required".to_string());
    }
    Ok(roles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_keyword() {
        assert_eq!(parse_roles("all").unwrap(), Role::ALL.to_vec());
    }

    #[test]
    fn parses_list_and_dedupes() {
        assert_eq!(
            parse_roles("ingest, search,ingest").unwrap(),
            vec![Role::Ingest, Role::Search]
        );
    }

    #[test]
    fn rejects_unknown() {
        assert!(parse_roles("ingest,bogus").is_err());
    }
}
