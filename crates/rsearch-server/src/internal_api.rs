//! Internal peer endpoints for the replicated storage backend: object
//! transfer between nodes plus leader-instructed replication. Mounted only
//! when `storage.backend = "replicated"`, outside user auth — requests
//! authenticate with the shared cluster token instead.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

use rsearch_common::crypto;
use rsearch_metastore::Metastore;
use rsearch_storage::{FsStorage, PeerClient, Storage, StorageError, INTERNAL_TOKEN_HEADER};

use crate::state::AppState;

/// State for the peer endpoints, present only on replicated-backend nodes.
pub struct InternalState {
    /// This node's local object root.
    pub fs: FsStorage,
    /// SHA-256 digest of the shared cluster token; requests are checked
    /// digest-to-digest so the comparison never runs on secret bytes.
    pub token_digest: String,
    pub metastore: Metastore,
    pub node_id: String,
    pub client: PeerClient,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/_rsearch/internal/objects/{*key}",
            get(get_object)
                .put(put_object)
                .delete(delete_object)
                // Split transfers are far larger than the default 2 MB
                // body cap; the streamed write bounds memory, not disk.
                .layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route("/_rsearch/internal/replicate", post(replicate))
}

/// Constant-time-equivalent token check: hash the presented token and
/// compare digests (a timing oracle on the digest reveals nothing about
/// the token itself).
fn authorize(state: &InternalState, headers: &HeaderMap) -> Result<(), Response> {
    let presented = headers
        .get(INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !presented.is_empty() && crypto::token_digest(presented) == state.token_digest {
        return Ok(());
    }
    Err((StatusCode::UNAUTHORIZED, "invalid cluster token").into_response())
}

fn internal(state: &AppState) -> Result<&Arc<InternalState>, Response> {
    state.internal.as_ref().ok_or_else(|| {
        (StatusCode::NOT_FOUND, "replicated backend not enabled").into_response()
    })
}

fn storage_error(key: &str, e: &StorageError) -> Response {
    match e {
        StorageError::NotFound(_) => {
            (StatusCode::NOT_FOUND, format!("object not found: {key}")).into_response()
        }
        StorageError::InvalidKey(_) => {
            (StatusCode::BAD_REQUEST, format!("invalid key: {key}")).into_response()
        }
        other => {
            warn!(key, error = %other, "internal object op failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "storage error").into_response()
        }
    }
}

/// Parse a single `bytes=start-end` (inclusive) range header. Oversized
/// ranges are clamped to the file by `FsStorage::get_range`, and the
/// checked add rejects the u64::MAX edge instead of overflowing.
fn parse_range(headers: &HeaderMap) -> Option<std::ops::Range<u64>> {
    let value = headers.get(header::RANGE)?.to_str().ok()?;
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = end.parse().ok()?;
    let end_exclusive = end.checked_add(1)?;
    (end_exclusive > start).then_some(start..end_exclusive)
}

async fn get_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let internal = match internal(&state) {
        Ok(i) => i,
        Err(r) => return r,
    };
    if let Err(r) = authorize(internal, &headers) {
        return r;
    }
    if let Some(range) = parse_range(&headers) {
        return match internal.fs.get_range(&key, range).await {
            Ok(bytes) => (StatusCode::PARTIAL_CONTENT, bytes).into_response(),
            Err(e) => storage_error(&key, &e),
        };
    }
    match internal.fs.open_read(&key).await {
        Ok((file, len)) => {
            let stream = tokio_util::io::ReaderStream::new(file);
            let mut response = axum::body::Body::from_stream(stream).into_response();
            response
                .headers_mut()
                .insert(header::CONTENT_LENGTH, len.into());
            response
        }
        Err(e) => storage_error(&key, &e),
    }
}

async fn put_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
    request: Request,
) -> Response {
    let internal = match internal(&state) {
        Ok(i) => i,
        Err(r) => return r,
    };
    if let Err(r) = authorize(internal, request.headers()) {
        return r;
    }
    let stream = request.into_body().into_data_stream();
    let size = match internal.fs.put_stream(&key, stream).await {
        Ok(size) => size,
        Err(e) => return storage_error(&key, &e),
    };
    // The receiver records its own placement: the row appears only once
    // the copy is durably on disk.
    if let Err(e) = internal
        .metastore
        .record_object_location(&key, &internal.node_id, size as i64)
        .await
    {
        warn!(key, error = %e, "recording object location failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "placement record failed").into_response();
    }
    axum::Json(json!({ "key": key, "size": size })).into_response()
}

async fn delete_object(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let internal = match internal(&state) {
        Ok(i) => i,
        Err(r) => return r,
    };
    if let Err(r) = authorize(internal, &headers) {
        return r;
    }
    if let Err(e) = internal.fs.delete(&key).await {
        return storage_error(&key, &e);
    }
    if let Err(e) = internal
        .metastore
        .remove_object_location(&key, &internal.node_id)
        .await
    {
        warn!(key, error = %e, "removing object location failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "placement record failed").into_response();
    }
    axum::Json(json!({ "key": key, "deleted": true })).into_response()
}

#[derive(Deserialize)]
struct ReplicateRequest {
    key: String,
    source_addr: String,
}

/// Pull an object from another node into the local root — the leader
/// calls this on a repair/drain target.
async fn replicate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::extract::Json<ReplicateRequest>,
) -> Response {
    let internal = match internal(&state) {
        Ok(i) => i,
        Err(r) => return r,
    };
    if let Err(r) = authorize(internal, &headers) {
        return r;
    }
    let ReplicateRequest { key, source_addr } = body.0;
    // Only registered node addresses may serve as pull sources — without
    // this, any token holder could use the endpoint as an SSRF primitive
    // that forwards the cluster token to arbitrary internal hosts.
    match internal.metastore.list_nodes().await {
        Ok(nodes) => {
            if !nodes.iter().any(|n| n.address.as_deref() == Some(source_addr.as_str())) {
                return (
                    StatusCode::BAD_REQUEST,
                    "source_addr is not a registered node address",
                )
                    .into_response();
            }
        }
        Err(e) => {
            warn!(error = %e, "listing nodes for replicate source check failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "node lookup failed").into_response();
        }
    }
    let size = match internal
        .client
        .download_to(&source_addr, &key, &internal.fs)
        .await
    {
        Ok(size) => size,
        Err(e) => {
            warn!(key, source = %source_addr, error = %e, "replicate pull failed");
            return storage_error(&key, &e);
        }
    };
    // Guarded record: a pull that outlives the leader's timeout can land
    // after the object was deleted cluster-wide — never resurrect
    // placement for it, and don't keep the freshly-pulled file either.
    match internal
        .metastore
        .record_object_location_if_known(&key, &internal.node_id, size as i64)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            let _ = internal.fs.delete(&key).await;
            warn!(key, "replicate finished for a deleted object; copy discarded");
            return (StatusCode::CONFLICT, "object no longer exists").into_response();
        }
        Err(e) => {
            warn!(key, error = %e, "recording object location failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "placement record failed")
                .into_response();
        }
    }
    info!(key, source = %source_addr, size, "replicated object from peer");
    axum::Json(json!({ "key": key, "size": size })).into_response()
}
