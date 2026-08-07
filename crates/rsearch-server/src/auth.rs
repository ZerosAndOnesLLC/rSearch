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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
/// Fails CLOSED: enforcement is on until a user count *successfully*
/// returns zero, so a startup race or a metastore blip never disables auth.
#[derive(Clone)]
pub struct AuthState {
    pub enforced: Arc<AtomicBool>,
    /// token-digest -> (identity, inserted_at). Short-TTL cache that keeps
    /// shipper auth off the DB hot path (no per-request session lookup or
    /// `last_used_at` write). Also holds verified Basic credentials (keyed
    /// by a digest of user+password) so shippers don't pay a full PBKDF2
    /// per request. Cleared on user/key mutation.
    cache: Arc<Mutex<std::collections::HashMap<String, (Identity, Instant)>>>,
    /// Digests of tokens that recently failed both DB lookups. Keeps a
    /// misconfigured shipper (or a token-spraying client) from hammering
    /// the metastore with two queries per request. Cleared on mutation
    /// alongside the positive cache, so a just-created key is usable
    /// immediately.
    negative: Arc<Mutex<std::collections::HashMap<String, Instant>>>,
}

const TOKEN_CACHE_TTL: Duration = Duration::from_secs(30);
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(5);

impl Default for AuthState {
    fn default() -> Self {
        Self {
            enforced: Arc::new(AtomicBool::new(true)),
            cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
            negative: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }
}

impl AuthState {
    fn cache_get(&self, hash: &str) -> Option<Identity> {
        let mut cache = self.cache.lock().unwrap();
        if let Some((identity, at)) = cache.get(hash) {
            if at.elapsed() < TOKEN_CACHE_TTL {
                return Some(identity.clone());
            }
            cache.remove(hash);
        }
        None
    }

    fn cache_put(&self, hash: String, identity: Identity) {
        let mut cache = self.cache.lock().unwrap();
        // Bounded to keep a token flood from growing it without limit.
        if cache.len() > 10_000 {
            cache.clear();
        }
        cache.insert(hash, (identity, Instant::now()));
    }

    fn negative_get(&self, hash: &str) -> bool {
        let mut negative = self.negative.lock().unwrap();
        if let Some(at) = negative.get(hash) {
            if at.elapsed() < NEGATIVE_CACHE_TTL {
                return true;
            }
            negative.remove(hash);
        }
        false
    }

    fn negative_put(&self, hash: String) {
        let mut negative = self.negative.lock().unwrap();
        // Bounded to keep a token flood from growing it without limit.
        if negative.len() > 10_000 {
            negative.clear();
        }
        negative.insert(hash, Instant::now());
    }

    /// Invalidate the whole token cache — called after any user/key change
    /// so revocations take effect within one request rather than one TTL.
    pub fn invalidate(&self) {
        self.cache.lock().unwrap().clear();
        self.negative.lock().unwrap().clear();
    }
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
                    // Only bootstrap-mode (auth off) after a *successful*
                    // count of zero.
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
                    Ok(_) => {
                        warned = false;
                        enforced.store(true, Ordering::Relaxed);
                    }
                    // On error, leave enforcement as-is (starts true) — never
                    // open auth because the metastore is unreachable.
                    Err(e) => warn!(error = %e, "auth refresh failed; enforcement unchanged"),
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
        // Prometheus scrape — same read level as stats; scrape configs
        // pass a session or API-key token as a bearer credential.
        ("GET", ["metrics"]) => Action::Search(None),
        // Everything else that mutates: admin.
        _ => Action::Admin,
    }
}

/// `Ok(Some)` — authenticated. `Ok(None)` — definitely no valid
/// credentials (→ 401). `Err(())` — a metastore error prevented deciding
/// (→ 503): shippers treat 401 as fatal and stop, so a DB blip must not
/// masquerade as bad credentials.
async fn authenticate(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<Option<Identity>, ()> {
    let mut backend_error = false;
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let api_key_header = headers.get("x-api-key").and_then(|v| v.to_str().ok());

    // Session or API-key token.
    for token in [bearer, api_key_header].into_iter().flatten() {
        let hash = crypto::token_digest(token);
        if let Some(identity) = state.auth.cache_get(&hash) {
            return Ok(Some(identity));
        }
        if state.auth.negative_get(&hash) {
            continue;
        }
        let session = state.metastore.session_user(&hash).await;
        let session_missed = matches!(session, Ok(None));
        if let Ok(Some(user)) = session {
            let identity = user_identity(user);
            state.auth.cache_put(hash, identity.clone());
            return Ok(Some(identity));
        }
        let api_key = state.metastore.api_key_by_hash(&hash).await;
        let api_key_missed = matches!(api_key, Ok(None));
        if let Ok(Some(key)) = api_key {
            let identity = Identity {
                name: format!("apikey:{}", key.name),
                is_admin: key.actions.iter().any(|a| a == "admin"),
                actions: key.actions.into_iter().collect(),
                streams: key.streams,
            };
            state.auth.cache_put(hash, identity.clone());
            return Ok(Some(identity));
        }
        // Only a definite miss goes in the negative cache — a metastore
        // error must not make a valid token unusable for the TTL.
        if session_missed && api_key_missed {
            state.auth.negative_put(hash);
        } else {
            backend_error = true;
        }
    }

    // HTTP Basic. Always run a PBKDF2 verify — against the real hash when
    // the user exists, against a fixed dummy hash when it doesn't — so the
    // absent-user path costs the same and can't be used to enumerate
    // usernames by timing.
    if let Some(basic) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        && let Some(decoded) = crypto::b64_decode(basic.trim())
        && let Ok(text) = String::from_utf8(decoded)
        && let Some((username, password)) = text.split_once(':')
    {
        // Verified credentials are cached (same TTL and invalidation as
        // tokens) so shippers using Basic don't pay a full PBKDF2 per
        // request. Failures are never cached: first contact with any
        // username always costs one full verify, preserving the
        // timing defense below.
        let basic_hash = crypto::token_digest(&format!("basic\0{username}\0{password}"));
        if let Some(identity) = state.auth.cache_get(&basic_hash) {
            return Ok(Some(identity));
        }
        let user = match state.metastore.get_user(username).await {
            Ok(user) => user,
            Err(_) => {
                backend_error = true;
                None
            }
        };
        let hash = user
            .as_ref()
            .map(|u| u.password_hash.clone())
            .unwrap_or_else(crypto::dummy_password_hash);
        let password = password.to_string();
        let ok = tokio::task::spawn_blocking(move || crypto::verify_password(&password, &hash))
            .await
            .unwrap_or(false);
        if ok && let Some(user) = user {
            let identity = user_identity(user);
            state.auth.cache_put(basic_hash, identity.clone());
            return Ok(Some(identity));
        }
    }
    if backend_error { Err(()) } else { Ok(None) }
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
    let identity = match authenticate(&state, &headers).await {
        Ok(Some(identity)) => identity,
        Ok(None) => return unauthorized("authentication required"),
        // Backend down ≠ bad credentials: return a retryable 5xx so
        // shippers back off instead of treating it as a fatal 401.
        Err(()) => return service_unavailable("authentication backend unavailable"),
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

fn service_unavailable(reason: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": {"type": "service_unavailable_exception", "reason": reason},
            "status": 503,
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
