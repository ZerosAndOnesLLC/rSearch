//! Request authentication and authorization.
//!
//! Identities come from HTTP Basic credentials, session tokens (Bearer),
//! or API keys (Bearer / X-Api-Key). Every route classifies into an
//! action — ingest, search, or admin — plus an optional stream scope.
//!
//! Bootstrap mode: while zero users exist, requests are allowed (with a
//! startup warning) so the first admin can be created; creating that
//! user immediately arms enforcement cluster-wide.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use tracing::warn;

use rsearch_common::crypto;

use crate::state::AppState;

/// Cached "any users exist?" flag; refreshed by a background task.
#[derive(Clone, Default)]
pub struct AuthState {
    pub enforced: Arc<AtomicBool>,
}

impl AuthState {
    pub fn spawn_refresher(&self, metastore: rsearch_metastore::Metastore) {
        let enforced = self.enforced.clone();
        tokio::spawn(async move {
            let mut warned = false;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                match metastore.count_users().await {
                    Ok(0) => {
                        if !warned {
                            warn!(
                                "no users exist — auth is in bootstrap mode; create the \
                                 first admin via PUT /_rsearch/users/<name>"
                            );
                            warned = true;
                        }
                        enforced.store(false, Ordering::Relaxed);
                    }
                    Ok(_) => enforced.store(true, Ordering::Relaxed),
                    Err(e) => warn!(error = %e, "auth refresh failed"),
                }
            }
        });
    }
}

/// The authenticated principal, attached as a request extension.
#[derive(Clone, Debug)]
pub struct Identity {
    pub name: String,
    pub is_admin: bool,
    pub actions: HashSet<String>,
    pub streams: Vec<String>,
}

impl Identity {
    fn allows_stream(&self, stream: &str) -> bool {
        self.streams.iter().any(|s| s == "*" || s == stream)
    }
}

#[derive(Debug, PartialEq)]
enum Action {
    Open,
    Ingest(Option<String>),
    Search(Option<String>),
    Admin,
}

/// Classify a request path+method into the action it needs.
fn classify(method: &str, path: &str) -> Action {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, segments.as_slice()) {
        // Open surface: handshake, health, login.
        (_, [""]) | (_, ["health"]) | (_, ["_cluster", "health"]) => Action::Open,
        ("POST", ["_rsearch", "login"]) => Action::Open,
        // Ingest
        ("POST", ["_bulk"]) => Action::Ingest(None),
        ("POST", [index, "_bulk"]) => Action::Ingest(Some(index.to_string())),
        // Search / read
        (_, [index, "_search"]) => Action::Search(Some(index.to_string())),
        ("POST", ["_msearch"]) => Action::Search(None),
        ("GET", [index, "_mapping"]) => Action::Search(Some(index.to_string())),
        ("GET", ["_cat", ..]) | ("GET", ["_rsearch", "stats"]) => Action::Search(None),
        // Everything else that mutates: admin.
        _ => Action::Admin,
    }
}

async fn authenticate(state: &AppState, headers: &axum::http::HeaderMap) -> Option<Identity> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let api_key_header = headers.get("x-api-key").and_then(|v| v.to_str().ok());

    // Session or API-key token.
    for token in [bearer, api_key_header].into_iter().flatten() {
        let hash = crypto::token_digest(token);
        if let Ok(Some(user)) = state.metastore.session_user(&hash).await {
            return Some(user_identity(user));
        }
        if let Ok(Some(key)) = state.metastore.api_key_by_hash(&hash).await {
            return Some(Identity {
                name: format!("apikey:{}", key.name),
                is_admin: key.actions.iter().any(|a| a == "admin"),
                actions: key.actions.into_iter().collect(),
                streams: key.streams,
            });
        }
    }

    // HTTP Basic.
    if let Some(basic) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        && let Some(decoded) = crypto::b64_decode(basic.trim())
        && let Ok(text) = String::from_utf8(decoded)
        && let Some((username, password)) = text.split_once(':')
        && let Ok(Some(user)) = state.metastore.get_user(username).await
    {
        let hash = user.password_hash.clone();
        let password = password.to_string();
        let ok = tokio::task::spawn_blocking(move || crypto::verify_password(&password, &hash))
            .await
            .unwrap_or(false);
        if ok {
            return Some(user_identity(user));
        }
    }
    None
}

fn user_identity(user: rsearch_metastore::UserRecord) -> Identity {
    let is_admin = user.role == "admin";
    Identity {
        name: user.username,
        is_admin,
        actions: if is_admin {
            ["ingest", "search", "admin"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            ["ingest", "search"].iter().map(|s| s.to_string()).collect()
        },
        streams: user.streams,
    }
}

pub async fn require(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let action = classify(request.method().as_str(), request.uri().path());
    if action == Action::Open {
        return next.run(request).await;
    }
    // Bootstrap: no users yet.
    if !state.auth.enforced.load(Ordering::Relaxed) {
        return next.run(request).await;
    }

    let headers = request.headers().clone();
    let Some(identity) = authenticate(&state, &headers).await else {
        return unauthorized("authentication required");
    };

    let allowed = match &action {
        Action::Open => true,
        Action::Admin => identity.is_admin,
        Action::Ingest(stream) => {
            identity.actions.contains("ingest")
                && match stream {
                    Some(stream) => identity.allows_stream(stream),
                    // Un-scoped /_bulk needs global stream access.
                    None => identity.streams.iter().any(|s| s == "*"),
                }
        }
        Action::Search(stream) => {
            identity.actions.contains("search")
                && match stream {
                    Some(stream) => identity.allows_stream(stream),
                    None => identity.streams.iter().any(|s| s == "*"),
                }
        }
    };
    if !allowed {
        return forbidden(&format!(
            "identity '{}' is not permitted to perform this action",
            identity.name
        ));
    }
    request.extensions_mut().insert(identity);
    next.run(request).await
}

fn unauthorized(reason: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [("www-authenticate", "Basic realm=\"rsearch\"")],
        Json(json!({
            "error": {"type": "security_exception", "reason": reason},
            "status": 401,
        })),
    )
        .into_response()
}

fn forbidden(reason: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": {"type": "security_exception", "reason": reason},
            "status": 403,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_routes() {
        assert_eq!(classify("GET", "/"), Action::Open);
        assert_eq!(classify("POST", "/_rsearch/login"), Action::Open);
        assert_eq!(classify("POST", "/_bulk"), Action::Ingest(None));
        assert_eq!(
            classify("POST", "/app-logs/_bulk"),
            Action::Ingest(Some("app-logs".into()))
        );
        assert_eq!(
            classify("POST", "/app-logs/_search"),
            Action::Search(Some("app-logs".into()))
        );
        assert_eq!(classify("GET", "/_cat/indices"), Action::Search(None));
        assert_eq!(classify("PUT", "/app-logs"), Action::Admin);
        assert_eq!(classify("PUT", "/_rsearch/users/alice"), Action::Admin);
        assert_eq!(
            classify("PUT", "/_rsearch/routing_rules/x"),
            Action::Admin
        );
    }
}
