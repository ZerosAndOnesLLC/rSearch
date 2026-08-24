//! ES document APIs (`/{index}/_doc/{id}` and friends) for document-mode
//! streams. Writes are one-item bulk requests under the hood, so they
//! share routing, peer handoff, WAL durability, and tombstone semantics
//! with `_bulk`; reads go through the document lookup (newest live
//! version by `_id`).

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use rsearch_search::{SearchError, SearchRequest};

use crate::bulk_api::{error_response, execute_bulk, parse_refresh};
use crate::state::AppState;

/// Query parameters the document routes understand.
#[derive(serde::Deserialize, Default)]
pub struct DocParams {
    /// `create` makes PUT/POST `_doc/{id}` behave like `_create/{id}`.
    op_type: Option<String>,
    /// `true` | `wait_for` — respond only once the write is searchable.
    refresh: Option<String>,
}

impl DocParams {
    fn refresh(&self) -> bool {
        parse_refresh(self.refresh.as_deref())
    }
}

/// Render the ES action line + body for a one-item bulk request.
fn one_item_body(action: &str, index: &str, id: Option<&str>, doc: Option<&Value>) -> String {
    let mut meta = json!({"_index": index});
    if let Some(id) = id {
        meta["_id"] = json!(id);
    }
    let mut body = json!({ action: meta }).to_string();
    body.push('\n');
    if let Some(doc) = doc {
        body.push_str(&doc.to_string());
        body.push('\n');
    }
    body
}

/// Run a one-item bulk and unwrap the item into the ES single-document
/// response shape (the item body, with its `status` as the HTTP status).
async fn run_one(state: AppState, body: String, refresh: bool) -> Response {
    let result = match execute_bulk(state, None, body, refresh).await {
        Ok(result) => result,
        Err(response) => return response,
    };
    let Some(item) = result["items"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(Value::as_object)
        .and_then(|o| o.values().next())
        .cloned()
    else {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "bulk returned no item");
    };
    let status = item["status"]
        .as_u64()
        .and_then(|s| StatusCode::from_u16(s as u16).ok())
        .unwrap_or(StatusCode::OK);
    let mut body = item;
    if let Some(obj) = body.as_object_mut() {
        obj.remove("status");
    }
    (status, Json(body)).into_response()
}

fn parse_doc(body: &str) -> Result<Value, Response> {
    match serde_json::from_str::<Value>(body) {
        Ok(doc) if doc.is_object() => Ok(doc),
        Ok(_) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "document must be a JSON object",
        )),
        Err(e) => Err(error_response(
            StatusCode::BAD_REQUEST,
            &format!("document is not valid JSON: {e}"),
        )),
    }
}

/// PUT/POST /{index}/_doc/{id} — index (replace) a document; with
/// `?op_type=create`, create-only.
pub async fn put_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    Query(params): Query<DocParams>,
    body: String,
) -> Response {
    let doc = match parse_doc(&body) {
        Ok(doc) => doc,
        Err(response) => return response,
    };
    let action = match params.op_type.as_deref() {
        Some("create") => "create",
        Some("index") | None => "index",
        Some(other) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("unsupported op_type [{other}]"),
            );
        }
    };
    run_one(
        state,
        one_item_body(action, &index, Some(&id), Some(&doc)),
        params.refresh(),
    )
    .await
}

/// POST /{index}/_doc — index a document under a generated id.
pub async fn post_doc(
    State(state): State<AppState>,
    Path(index): Path<String>,
    Query(params): Query<DocParams>,
    body: String,
) -> Response {
    let doc = match parse_doc(&body) {
        Ok(doc) => doc,
        Err(response) => return response,
    };
    run_one(
        state,
        one_item_body("index", &index, None, Some(&doc)),
        params.refresh(),
    )
    .await
}

/// PUT/POST /{index}/_create/{id} — create-only write.
pub async fn create_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    Query(params): Query<DocParams>,
    body: String,
) -> Response {
    let doc = match parse_doc(&body) {
        Ok(doc) => doc,
        Err(response) => return response,
    };
    run_one(
        state,
        one_item_body("create", &index, Some(&id), Some(&doc)),
        params.refresh(),
    )
    .await
}

/// POST /{index}/_update/{id} — partial update (`doc`, `doc_as_upsert`,
/// `upsert`; no scripts).
pub async fn update_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    Query(params): Query<DocParams>,
    body: String,
) -> Response {
    let body = match parse_doc(&body) {
        Ok(doc) => doc,
        Err(response) => return response,
    };
    run_one(
        state,
        one_item_body("update", &index, Some(&id), Some(&body)),
        params.refresh(),
    )
    .await
}

/// DELETE /{index}/_doc/{id}
pub async fn delete_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    Query(params): Query<DocParams>,
) -> Response {
    // A delete is a tombstone: visible on this node at once, so there is
    // no buffer to cut — but honoring the flag keeps clients uniform.
    run_one(
        state,
        one_item_body("delete", &index, Some(&id), None),
        params.refresh(),
    )
    .await
}

fn lookup_error(e: SearchError, index: &str) -> Response {
    match e {
        SearchError::Metastore(rsearch_metastore::MetastoreError::StreamNotFound(_)) => {
            crate::search_api::index_not_found(index)
        }
        SearchError::BadRequest(reason) => error_response(StatusCode::BAD_REQUEST, &reason),
        other => error_response(StatusCode::INTERNAL_SERVER_ERROR, &other.to_string()),
    }
}

/// GET /{index}/_doc/{id}
pub async fn get_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    let Some(lookup) = &state.doc_lookup else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "this node runs neither the search nor the ingest role",
        );
    };
    match lookup.get_document(&index, &id).await {
        Ok(Some(found)) => Json(json!({
            "_index": index,
            "_id": id,
            "_version": found.version,
            "found": true,
            "_source": found.source,
        }))
        .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"_index": index, "_id": id, "found": false})),
        )
            .into_response(),
        Err(e) => lookup_error(e, &index),
    }
}

/// HEAD /{index}/_doc/{id}
pub async fn head_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    let Some(lookup) = &state.doc_lookup else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match lookup.get_document(&index, &id).await {
        Ok(Some(_)) => StatusCode::OK.into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(SearchError::Metastore(rsearch_metastore::MetastoreError::StreamNotFound(_))) => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /{index}/_source/{id} — just the source.
pub async fn get_source(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    let Some(lookup) = &state.doc_lookup else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "this node runs neither the search nor the ingest role",
        );
    };
    match lookup.get_document(&index, &id).await {
        Ok(Some(found)) => Json(found.source).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"_index": index, "_id": id, "found": false})),
        )
            .into_response(),
        Err(e) => lookup_error(e, &index),
    }
}

/// Max documents deleted per search round of `_delete_by_query` (the
/// result window); rounds repeat until the query is dry.
const DELETE_BY_QUERY_PAGE: usize = 10_000;
/// Safety bound on rounds (100M documents).
const DELETE_BY_QUERY_MAX_ROUNDS: usize = 10_000;

/// POST /{index}/_delete_by_query — tombstone every live document the
/// query matches. Each round searches (ids only) and deletes a page; the
/// tombstones take effect on this node immediately, so the next round
/// sees only what's left.
pub async fn delete_by_query(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: String,
) -> Response {
    let started = std::time::Instant::now();
    let Some(lookup) = state.doc_lookup.clone() else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "this node runs neither the search nor the ingest role",
        );
    };
    let body: Value = if body.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                return error_response(StatusCode::BAD_REQUEST, &format!("invalid body: {e}"));
            }
        }
    };
    let query = body
        .get("query")
        .cloned()
        .unwrap_or_else(|| json!({"match_all": {}}));
    let mut deleted = 0usize;
    let mut rounds = 0usize;
    // Progress guard: a round whose ids were all seen in the previous round
    // means the tombstones are not taking effect (a node ahead in sequence,
    // or ids this node cannot hide); stop rather than spin.
    let mut previous: std::collections::HashSet<String> = std::collections::HashSet::new();
    loop {
        rounds += 1;
        if rounds > DELETE_BY_QUERY_MAX_ROUNDS {
            break;
        }
        let request = SearchRequest {
            stream: index.clone(),
            query: query.clone(),
            from: 0,
            size: DELETE_BY_QUERY_PAGE,
            sort_desc: true,
            aggs: None,
            include_source: false,
            track_total_hits: Some(0),
            search_after: None,
        };
        let response = match lookup.search(request).await {
            Ok(response) => response,
            Err(e) => return lookup_error(e, &index),
        };
        let ids: Vec<String> = response["hits"]["hits"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|h| h["_id"].as_str().map(str::to_string))
            .collect();
        if ids.is_empty() {
            break;
        }
        if !ids.is_empty() && ids.iter().all(|id| previous.contains(id)) {
            return error_response(
                StatusCode::CONFLICT,
                "delete_by_query made no progress: the matched documents are not being hidden \
                 (retry, or check cluster clock synchronization)",
            );
        }
        previous = ids.iter().cloned().collect();
        let mut bulk = String::new();
        for id in &ids {
            bulk.push_str(&one_item_body("delete", &index, Some(id), None));
        }
        let result = match execute_bulk(state.clone(), None, bulk, false).await {
            Ok(result) => result,
            Err(response) => return response,
        };
        let mut failures = Vec::new();
        for item in result["items"].as_array().into_iter().flatten() {
            let body = item.get("delete").cloned().unwrap_or(Value::Null);
            if body["status"].as_u64().unwrap_or(500) < 300 {
                deleted += 1;
            } else {
                failures.push(body);
            }
        }
        if !failures.is_empty() {
            // A log-mode stream (or a metastore error) fails every item
            // the same way; report the first reason as the request error.
            let reason = failures[0]["error"]["reason"]
                .as_str()
                .unwrap_or("delete failed")
                .to_string();
            return error_response(StatusCode::BAD_REQUEST, &reason);
        }
        if ids.len() < DELETE_BY_QUERY_PAGE {
            break;
        }
    }
    Json(json!({
        "took": started.elapsed().as_millis() as u64,
        "timed_out": false,
        "total": deleted,
        "deleted": deleted,
        "batches": rounds,
        "version_conflicts": 0,
        "noops": 0,
        "failures": [],
    }))
    .into_response()
}
