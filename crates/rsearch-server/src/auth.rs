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
    /// Whether this identity's stream scope covers `stream`.
    pub fn allows_stream(&self, stream: &str) -> bool {
        self.streams.iter().any(|s| stream_glob_matches(s, stream))
    }
}

/// Stream-scope matching: an entry is an exact name or a glob where `*`
/// matches any run of characters (`acme-*`, `*-audit`, `tenant-*-logs`),
/// so a multi-tenant application can be scoped to a prefix it derives
/// index names from instead of needing `*` or an up-front list.
pub fn stream_glob_matches(pattern: &str, stream: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == stream;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !stream.starts_with(first) {
        return false;
    }
    let mut rest = &stream[first.len()..];
    // Middle pieces must appear in order; the final piece must end the
    // name (and not overlap what the earlier pieces consumed).
    for piece in &parts[1..parts.len() - 1] {
        match rest.find(piece) {
            Some(at) => rest = &rest[at + piece.len()..],
            None => return false,
        }
    }
    rest.ends_with(last) && rest.len() >= last.len()
}

#[derive(Debug, PartialEq)]
enum Action {
    Open,
    Ingest(Option<String>),
    Search(Option<String>),
    /// Search on a stream named by server-side state (a scroll context)
    /// rather than the path; the handler checks the stream scope.
    SearchContext,
    Admin,
}

/// Classify a request path+method into the action it needs.
fn classify(method: &str, path: &str) -> Action {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, segments.as_slice()) {
        // Open surface: handshake, health, login. `/ready` is the Loki
        // readiness probe — health-equivalent, no data exposure.
        (_, [""]) | (_, ["health"]) | (_, ["_cluster", "health"]) => Action::Open,
        (_, ["ready"]) => Action::Open,
        ("POST", ["_rsearch", "login"]) => Action::Open,
        // Loki-compatible read API (#11): selectors may span any stream,
        // so like /_msearch it needs global search access.
        (_, ["loki", "api", "v1", ..]) => Action::Search(None),
        // Ingest
        ("POST", ["_bulk"]) => Action::Ingest(None),
        ("POST", [index, "_bulk"]) => Action::Ingest(Some(index.to_string())),
        // Document APIs (#34): writes are stream-scoped ingest, reads are
        // stream-scoped search — so an application key never needs admin.
        ("PUT" | "POST" | "DELETE", [index, "_doc", ..])
        | ("PUT" | "POST", [index, "_create", _])
        | ("POST", [index, "_update", _])
        | ("POST", [index, "_delete_by_query"]) => Action::Ingest(Some(index.to_string())),
        ("GET" | "HEAD", [index, "_doc", _]) | ("GET", [index, "_source", _]) => {
            Action::Search(Some(index.to_string()))
        }
        // Search / read
        (_, [index, "_search"]) | (_, [index, "_count"]) => {
            Action::Search(Some(index.to_string()))
        }
        // Mapping updates are index setup, the same level as PUT /{index}.
        ("PUT", [index, "_mapping"]) => Action::Ingest(Some(index.to_string())),
        ("POST", ["_msearch"]) => Action::Search(None),
        // Scroll continuation/clear (#72): the stream is in the stored
        // context, not the path — the handler checks it against the
        // identity's scope.
        (_, ["_search", "scroll", ..]) => Action::SearchContext,
        ("GET", [index, "_mapping"]) | ("GET", [index, "_settings"]) => {
            Action::Search(Some(index.to_string()))
        }

        ("GET", ["_cat", ..]) | ("GET", ["_rsearch", "stats"]) => Action::Search(None),
        // Prometheus scrape — same read level as stats; scrape configs
        // pass a session or API-key token as a bearer credential.
        ("GET", ["metrics"]) => Action::Search(None),
        // Single-segment index paths, after every reserved top-level route
        // above. Index creation / mapping update is an ingest-level action:
        // an ingest key scoped to a stream already creates it implicitly
        // via _bulk, so PUT /{index} (mode + mapping) needs no more.
        // DELETE /{index} is the drop half of an application's
        // drop-and-rebuild reindex, so it sits at the same level; the
        // handler re-checks every stream a wildcard or list resolves to
        // against the identity's scope.
        ("PUT" | "DELETE", [index]) if !index.starts_with('_') => {
            Action::Ingest(Some(index.to_string()))
        }
        ("GET" | "HEAD", [index]) if !index.starts_with('_') => {
            Action::Search(Some(index.to_string()))
        }
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
        Action::SearchContext => identity.actions.contains("search"),
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
    fn stream_globs() {
        let id = |streams: &[&str]| Identity {
            name: "k".into(),
            is_admin: false,
            actions: HashSet::new(),
            streams: streams.iter().map(|s| s.to_string()).collect(),
        };
        assert!(id(&["*"]).allows_stream("anything"));
        assert!(id(&["items"]).allows_stream("items"));
        assert!(!id(&["items"]).allows_stream("items-2"));
        assert!(id(&["acme-*"]).allows_stream("acme-orders"));
        assert!(id(&["acme-*"]).allows_stream("acme-"));
        assert!(!id(&["acme-*"]).allows_stream("acmeorders"));
        assert!(!id(&["acme-*"]).allows_stream("other-acme-x"));
        assert!(id(&["*-audit"]).allows_stream("acme-audit"));
        assert!(!id(&["*-audit"]).allows_stream("audit"));
        assert!(id(&["t-*-logs"]).allows_stream("t-1-logs"));
        assert!(id(&["t-*-logs"]).allows_stream("t-a-b-logs"));
        assert!(!id(&["t-*-logs"]).allows_stream("t-logs"));
        assert!(!id(&["t-*-logs"]).allows_stream("t-1-logsx"));
        assert!(id(&["a*b*c"]).allows_stream("a1b2c"));
        assert!(!id(&["a*b*c"]).allows_stream("acb"));
        assert!(id(&["x", "acme-*"]).allows_stream("acme-y"));
    }

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
        // Loki-compatible surface: everything under /loki/api/v1 needs
        // global search access; /ready is health-equivalent and open.
        assert_eq!(classify("GET", "/loki/api/v1/query_range"), Action::Search(None));
        assert_eq!(classify("POST", "/loki/api/v1/query"), Action::Search(None));
        assert_eq!(
            classify("GET", "/loki/api/v1/label/level/values"),
            Action::Search(None)
        );
        assert_eq!(classify("GET", "/loki/api/v1/tail"), Action::Search(None));
        assert_eq!(classify("GET", "/loki/api/v1/index/volume"), Action::Search(None));
        assert_eq!(classify("GET", "/ready"), Action::Open);
        // Index create/describe are stream-scoped ingest/search actions so
        // an application can hold a least-privilege key (#34).
        assert_eq!(classify("PUT", "/app-logs"), Action::Ingest(Some("app-logs".into())));
        assert_eq!(classify("GET", "/app-logs"), Action::Search(Some("app-logs".into())));
        assert_eq!(classify("HEAD", "/app-logs"), Action::Search(Some("app-logs".into())));
        assert_eq!(
            classify("GET", "/app-logs/_settings"),
            Action::Search(Some("app-logs".into()))
        );
        // Document APIs.
        assert_eq!(classify("PUT", "/recs/_doc/1"), Action::Ingest(Some("recs".into())));
        assert_eq!(classify("POST", "/recs/_doc"), Action::Ingest(Some("recs".into())));
        assert_eq!(classify("DELETE", "/recs/_doc/1"), Action::Ingest(Some("recs".into())));
        assert_eq!(classify("POST", "/recs/_update/1"), Action::Ingest(Some("recs".into())));
        assert_eq!(classify("PUT", "/recs/_create/1"), Action::Ingest(Some("recs".into())));
        assert_eq!(
            classify("POST", "/recs/_delete_by_query"),
            Action::Ingest(Some("recs".into()))
        );
        assert_eq!(classify("GET", "/recs/_doc/1"), Action::Search(Some("recs".into())));
        assert_eq!(classify("HEAD", "/recs/_doc/1"), Action::Search(Some("recs".into())));
        assert_eq!(classify("GET", "/recs/_source/1"), Action::Search(Some("recs".into())));
        // Reserved top-level paths never fall into the index arms.
        assert_eq!(classify("PUT", "/_rsearch"), Action::Admin);
        assert_eq!(classify("GET", "/_rsearch"), Action::Admin);
        assert_eq!(classify("GET", "/metrics"), Action::Search(None));
        assert_eq!(classify("PUT", "/_rsearch/users/alice"), Action::Admin);
        assert_eq!(
            classify("PUT", "/_rsearch/routing_rules/x"),
            Action::Admin
        );
    }
}
