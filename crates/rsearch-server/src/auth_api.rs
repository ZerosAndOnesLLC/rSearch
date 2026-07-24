use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use rsearch_common::crypto;

use crate::state::AppState;

const SESSION_TTL_SECS: f64 = 24.0 * 3600.0;

fn err(status: StatusCode, reason: &str) -> Response {
    (
        status,
        Json(json!({"error": {"type": "security_exception", "reason": reason},
                    "status": status.as_u16()})),
    )
        .into_response()
}

/// POST /_rsearch/login {"username": ..., "password": ...} -> {token}
pub async fn login(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let (Some(username), Some(password)) = (
        body.get("username").and_then(Value::as_str),
        body.get("password").and_then(Value::as_str),
    ) else {
        return err(StatusCode::BAD_REQUEST, "'username' and 'password' required");
    };
    let Ok(Some(user)) = state.metastore.get_user(username).await else {
        return err(StatusCode::UNAUTHORIZED, "invalid credentials");
    };
    let hash = user.password_hash.clone();
    let password_owned = password.to_string();
    let ok = tokio::task::spawn_blocking(move || crypto::verify_password(&password_owned, &hash))
        .await
        .unwrap_or(false);
    if !ok {
        state.audit("login_failed", username, "").await;
        return err(StatusCode::UNAUTHORIZED, "invalid credentials");
    }
    let Ok(token) = crypto::generate_token() else {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "token generation failed");
    };
    if let Err(e) = state
        .metastore
        .create_session(&crypto::token_digest(&token), user.id, SESSION_TTL_SECS)
        .await
    {
        return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }
    state.audit("login", username, "").await;
    Json(json!({"token": token, "expires_in_secs": SESSION_TTL_SECS as u64,
                "role": user.role})).into_response()
}

/// PUT /_rsearch/users/{name} {"password": ..., "role"?: "admin"|"user",
/// "streams"?: [...]}. The first user created is always an admin.
pub async fn put_user(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let Some(password) = body.get("password").and_then(Value::as_str) else {
        return err(StatusCode::BAD_REQUEST, "'password' is required");
    };
    if password.len() < 12 {
        return err(StatusCode::BAD_REQUEST, "password must be at least 12 characters");
    }
    let first_user = state.metastore.count_users().await.unwrap_or(0) == 0;
    let role = if first_user {
        "admin"
    } else {
        body.get("role").and_then(Value::as_str).unwrap_or("user")
    };
    if !matches!(role, "admin" | "user") {
        return err(StatusCode::BAD_REQUEST, "role must be 'admin' or 'user'");
    }
    let streams: Vec<String> = body
        .get("streams")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_else(|| vec!["*".to_string()]);

    let password_owned = password.to_string();
    let hash = match tokio::task::spawn_blocking(move || crypto::hash_password(&password_owned))
        .await
    {
        Ok(Ok(hash)) => hash,
        _ => return err(StatusCode::INTERNAL_SERVER_ERROR, "hashing failed"),
    };
    match state.metastore.upsert_user(&name, &hash, role, &streams).await {
        Ok(user) => {
            // Arm enforcement immediately — don't wait for the refresher.
            state
                .auth
                .enforced
                .store(true, std::sync::atomic::Ordering::Relaxed);
            state.auth.invalidate();
            state.audit("user_upserted", &name, role).await;
            Json(json!({"acknowledged": true, "username": user.username,
                        "role": user.role, "streams": user.streams,
                        "bootstrap_admin": first_user}))
            .into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// GET /_rsearch/users
pub async fn list_users(State(state): State<AppState>) -> Response {
    match state.metastore.list_users().await {
        Ok(users) => Json(json!({
            "users": users.into_iter().map(|u| json!({
                "username": u.username, "role": u.role, "streams": u.streams,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// DELETE /_rsearch/users/{name}
pub async fn delete_user(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.metastore.delete_user(&name).await {
        Ok(true) => {
            state.auth.invalidate();
            state.audit("user_deleted", &name, "").await;
            Json(json!({"acknowledged": true})).into_response()
        }
        Ok(false) => err(StatusCode::NOT_FOUND, &format!("no user '{name}'")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// POST /_rsearch/api_keys {"name": ..., "actions": ["ingest"],
/// "streams"?: [...]} -> the key, shown exactly once.
pub async fn create_api_key(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let Some(name) = body.get("name").and_then(Value::as_str) else {
        return err(StatusCode::BAD_REQUEST, "'name' is required");
    };
    let actions: Vec<String> = body
        .get("actions")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if actions.is_empty()
        || !actions
            .iter()
            .all(|a| matches!(a.as_str(), "ingest" | "search" | "admin"))
    {
        return err(
            StatusCode::BAD_REQUEST,
            "'actions' must be a non-empty subset of [ingest, search, admin]",
        );
    }
    let streams: Vec<String> = body
        .get("streams")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_else(|| vec!["*".to_string()]);
    let Ok(secret) = crypto::generate_token() else {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "token generation failed");
    };
    let key = format!("rsk_{secret}");
    match state
        .metastore
        .create_api_key(name, &crypto::token_digest(&key), &actions, &streams)
        .await
    {
        Ok(record) => {
            state.audit("api_key_created", name, &actions.join(",")).await;
            Json(json!({"acknowledged": true, "name": record.name, "key": key,
                        "actions": record.actions, "streams": record.streams}))
            .into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// GET /_rsearch/api_keys
pub async fn list_api_keys(State(state): State<AppState>) -> Response {
    match state.metastore.list_api_keys().await {
        Ok(keys) => Json(json!({"api_keys": keys})).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// DELETE /_rsearch/api_keys/{name}
pub async fn delete_api_key(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.metastore.delete_api_key(&name).await {
        Ok(true) => {
            state.auth.invalidate();
            state.audit("api_key_deleted", &name, "").await;
            Json(json!({"acknowledged": true})).into_response()
        }
        Ok(false) => err(StatusCode::NOT_FOUND, &format!("no api key '{name}'")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}
