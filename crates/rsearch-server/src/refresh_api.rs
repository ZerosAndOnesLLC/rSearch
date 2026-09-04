//! `POST|GET /{index}/_refresh` and `/_refresh` (#80): cut the named
//! document-mode streams' buffered batches on every ingest node and
//! return once those splits are published, so a search issued after the
//! response sees every write acknowledged before the call. It is the
//! flush `?refresh=` performs on a write, reachable through a client's
//! index-management API (`indices.refresh`) when the caller does not own
//! the original write. Log streams are acknowledged without a cut, the
//! same policy `?refresh=` applies: a shipper cannot force a split per
//! request.
//!
//! Buffers are node-local, so the handling node flushes its own pipeline
//! and asks every live ingest peer to do the same over the internal API
//! (cluster-token auth), then answers OpenSearch's `_shards` summary with
//! one shard per index.

use std::collections::HashSet;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use rsearch_common::crypto;
use rsearch_metastore::{Metastore, StreamRecord};
use rsearch_storage::{INTERNAL_TOKEN_HEADER, PeerClient};
use tracing::warn;

use crate::bulk_api::REFRESH_WAIT_LIMIT;
use crate::search_api::{es_error, index_not_found};
use crate::state::AppState;

/// A peer is asked to flush only if it heartbeated this recently; an
/// older entry is a node that is gone, and waiting on it would only
/// delay the response.
const PEER_STALE_SECS: f64 = 30.0;

/// Ingest peers a refresh fans out to. Present on every node with a
/// cluster token, whatever its roles: a search-only node must still be
/// able to reach the buffers, which live on the ingest nodes.
pub struct RefreshPeers {
    metastore: Metastore,
    client: PeerClient,
    node_id: String,
    /// Digest of the cluster token; inbound flush requests are checked
    /// against it (digest-to-digest, as the bulk handoff receiver does).
    pub token_digest: String,
}

impl RefreshPeers {
    pub fn new(
        metastore: Metastore,
        client: PeerClient,
        node_id: String,
        token_digest: String,
    ) -> Self {
        Self {
            metastore,
            client,
            node_id,
            token_digest,
        }
    }

    /// Live ingest nodes other than this one, with an advertised address.
    /// Draining nodes are included: their buffers still hold documents.
    async fn ingest_peers(&self) -> Vec<(String, String)> {
        match self.metastore.list_nodes().await {
            Ok(nodes) => nodes
                .into_iter()
                .filter(|n| {
                    n.id != self.node_id
                        && n.heartbeat_age_secs < PEER_STALE_SECS
                        && n.roles.iter().any(|r| r == "ingest")
                })
                .filter_map(|n| n.address.map(|addr| (n.id, addr)))
                .collect(),
            Err(e) => {
                warn!(error = %e, "refresh: listing ingest nodes failed");
                Vec::new()
            }
        }
    }

    /// Ask one peer to flush `streams`; returns the names it could not
    /// flush (every name when the peer is unreachable or refuses).
    async fn flush_on(&self, id: &str, addr: &str, streams: &[String]) -> Vec<String> {
        let all = || streams.to_vec();
        let mut url = match url::Url::parse(addr) {
            Ok(url) => url,
            Err(e) => {
                warn!(peer = id, addr, error = %e, "refresh: bad peer address");
                return all();
            }
        };
        url.set_path("/_rsearch/internal/refresh");
        let body = json!({"streams": streams}).to_string();
        match self.client.post_raw(url.as_str(), body.into()).await {
            Ok((200, bytes)) => serde_json::from_slice::<InternalRefreshReply>(&bytes)
                .map(|r| r.failed)
                .unwrap_or_else(|_| all()),
            Ok((status, _)) => {
                warn!(peer = id, status, "refresh: peer refused flush");
                all()
            }
            Err(e) => {
                warn!(peer = id, error = %e, "refresh: peer flush failed");
                all()
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct InternalRefreshReply {
    failed: Vec<String>,
}

#[derive(serde::Deserialize)]
struct InternalRefreshBody {
    streams: Vec<String>,
}

/// `POST /_rsearch/internal/refresh` — flush the named streams' buffers
/// on this node. Cluster-token authenticated; registered on ingest nodes
/// only, since only they hold buffers.
pub async fn refresh_internal(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let Some(peers) = state.refresh_peers.as_ref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let presented = headers
        .get(INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if presented.is_empty() || crypto::token_digest(presented) != peers.token_digest {
        return (StatusCode::UNAUTHORIZED, "invalid cluster token").into_response();
    }
    // Parsed by hand: the peer client sends no content-type, which the
    // `Json` extractor would reject with 415.
    let body: InternalRefreshBody = match serde_json::from_slice(&body) {
        Ok(body) => body,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let failed = match state.pipeline.as_ref() {
        Some(pipeline) => flush_local(pipeline, &body.streams).await,
        // Not an ingest node: nothing is buffered here.
        None => Vec::new(),
    };
    Json(InternalRefreshReply { failed }).into_response()
}

/// `POST|GET /_refresh`.
pub async fn refresh_all(
    State(state): State<AppState>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
) -> Response {
    refresh(state, "_all", identity).await
}

/// `POST|GET /{index}/_refresh`.
pub async fn refresh_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    identity: Option<axum::Extension<crate::auth::Identity>>,
) -> Response {
    refresh(state, &index, identity).await
}

async fn refresh(
    state: AppState,
    expression: &str,
    identity: Option<axum::Extension<crate::auth::Identity>>,
) -> Response {
    let streams = match state.metastore.list_streams().await {
        Ok(streams) => streams,
        Err(e) => {
            return es_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &e.to_string(),
            );
        }
    };
    let targets = match resolve_targets(&streams, expression) {
        Ok(targets) => targets,
        Err(missing) => return index_not_found(&missing),
    };
    // A glob or list must stay inside the caller's stream scope: the
    // middleware matched the raw expression, not what it expands to.
    if let Some(axum::Extension(identity)) = &identity
        && !identity.is_admin
        && let Some(outside) = targets.iter().find(|t| !identity.allows_stream(&t.name))
    {
        return es_error(
            StatusCode::FORBIDDEN,
            "security_exception",
            &format!(
                "identity '{}' is not permitted to refresh index [{}]",
                identity.name, outside.name
            ),
        );
    }
    let to_flush: Vec<String> = targets
        .iter()
        .filter(|s| s.is_document_mode())
        .map(|s| s.name.clone())
        .collect();
    let failed = if to_flush.is_empty() {
        0
    } else {
        match tokio::time::timeout(REFRESH_WAIT_LIMIT, flush_everywhere(&state, &to_flush)).await {
            Ok(failed) => failed.len(),
            Err(_) => {
                warn!(
                    streams = ?to_flush,
                    "refresh wait exceeded {}s; reporting the indices as failed",
                    REFRESH_WAIT_LIMIT.as_secs()
                );
                to_flush.len()
            }
        }
    };
    let total = targets.len();
    Json(json!({
        "_shards": {"total": total, "successful": total - failed, "failed": failed}
    }))
    .into_response()
}

/// Expand an index expression (name, comma list, globs, `_all`/`*`/empty
/// for everything) against the live streams. A named index that does not
/// exist is the error (OpenSearch's 404); a glob matching nothing is fine.
fn resolve_targets<'a>(
    streams: &'a [StreamRecord],
    expression: &str,
) -> Result<Vec<&'a StreamRecord>, String> {
    let expression = expression.trim();
    let mut targets: Vec<&StreamRecord> = Vec::new();
    if expression.is_empty() || expression == "_all" || expression == "*" {
        targets.extend(streams.iter());
    } else {
        for part in expression
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            if part.contains('*') {
                targets.extend(
                    streams
                        .iter()
                        .filter(|s| crate::auth::stream_glob_matches(part, &s.name)),
                );
            } else if let Some(stream) = streams.iter().find(|s| s.name == part) {
                targets.push(stream);
            } else {
                return Err(part.to_string());
            }
        }
    }
    targets.sort_by(|a, b| a.name.cmp(&b.name));
    targets.dedup_by(|a, b| a.name == b.name);
    Ok(targets)
}

/// Flush `streams` on this node and on every live ingest peer, in
/// parallel; returns the names that failed anywhere.
async fn flush_everywhere(state: &AppState, streams: &[String]) -> HashSet<String> {
    let local = async {
        match state.pipeline.as_ref() {
            Some(pipeline) => flush_local(pipeline, streams).await,
            None => Vec::new(),
        }
    };
    let remote = async {
        let Some(peers) = state.refresh_peers.as_ref() else {
            return Vec::new();
        };
        let targets = peers.ingest_peers().await;
        let peers: Arc<RefreshPeers> = peers.clone();
        let calls = targets
            .iter()
            .map(|(id, addr)| peers.flush_on(id, addr, streams));
        futures::future::join_all(calls)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
    };
    let (local, remote) = tokio::join!(local, remote);
    local.into_iter().chain(remote).collect()
}

/// Flush each stream's buffer on this node; returns the names whose
/// flush errored (an unbuffered stream is a successful no-op).
async fn flush_local(pipeline: &rsearch_ingest::IngestPipeline, streams: &[String]) -> Vec<String> {
    let mut failed = Vec::new();
    for stream in streams {
        if let Err(e) = pipeline.flush_stream(stream).await {
            warn!(stream, error = %e, "refresh flush failed");
            failed.push(stream.clone());
        }
    }
    failed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(name: &str) -> StreamRecord {
        StreamRecord {
            id: 0,
            name: name.to_string(),
            mapping: json!({}),
            retention_hours: None,
            mode: "document".to_string(),
        }
    }

    fn names(targets: Vec<&StreamRecord>) -> Vec<&str> {
        targets.into_iter().map(|s| s.name.as_str()).collect()
    }

    #[test]
    fn resolves_index_expressions() {
        let streams = vec![stream("app-a"), stream("app-b"), stream("keep")];
        assert_eq!(names(resolve_targets(&streams, "keep").unwrap()), ["keep"]);
        assert_eq!(
            names(resolve_targets(&streams, "app-*").unwrap()),
            ["app-a", "app-b"]
        );
        assert_eq!(
            names(resolve_targets(&streams, "keep,app-a,keep").unwrap()),
            ["app-a", "keep"]
        );
        for all in ["_all", "*", "", " "] {
            assert_eq!(
                names(resolve_targets(&streams, all).unwrap()),
                ["app-a", "app-b", "keep"],
                "{all:?}"
            );
        }
        assert!(resolve_targets(&streams, "nomatch-*").unwrap().is_empty());
        assert_eq!(resolve_targets(&streams, "nope").unwrap_err(), "nope");
        assert_eq!(resolve_targets(&streams, "keep,nope").unwrap_err(), "nope");
    }
}
