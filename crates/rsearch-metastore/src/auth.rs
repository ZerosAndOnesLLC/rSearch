//! Users, sessions, and API keys.

use sqlx::Row;

use crate::error::MetastoreResult;
use crate::metastore::Metastore;

/// A console user row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UserRecord {
    /// Primary key; sessions reference it via `user_id`.
    pub id: i64,
    /// Unique login name.
    pub username: String,
    /// Hashed password (never the plaintext).
    pub password_hash: String,
    /// admin | user
    pub role: String,
    /// Streams the user may access; `"*"` grants all streams.
    pub streams: Vec<String>,
}

/// An API key row. The key itself is never stored — only its hash —
/// so this record is safe to serialize in listings.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct ApiKeyRecord {
    /// Primary key.
    pub id: i64,
    /// Unique key name (upsert key).
    pub name: String,
    /// Granted actions: subset of ingest, search, admin.
    pub actions: Vec<String>,
    /// Streams the key may access; `"*"` grants all streams.
    pub streams: Vec<String>,
}

const USER_COLUMNS: &str = "id, username, password_hash, role, streams";

impl Metastore {
    /// Total number of users (used for first-run admin bootstrap).
    pub async fn count_users(&self) -> MetastoreResult<i64> {
        let row = sqlx::query("SELECT count(*) AS n FROM users")
            .fetch_one(self.pool())
            .await?;
        Ok(row.get::<i64, _>("n"))
    }

    /// Create or update (by username) a user.
    pub async fn upsert_user(
        &self,
        username: &str,
        password_hash: &str,
        role: &str,
        streams: &[String],
    ) -> MetastoreResult<UserRecord> {
        let query = format!(
            "INSERT INTO users (username, password_hash, role, streams)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash,
                 role = EXCLUDED.role, streams = EXCLUDED.streams
             RETURNING {USER_COLUMNS}"
        );
        Ok(sqlx::query_as::<_, UserRecord>(sqlx::AssertSqlSafe(query))
            .bind(username)
            .bind(password_hash)
            .bind(role)
            .bind(streams)
            .fetch_one(self.pool())
            .await?)
    }

    /// Look up a user by username.
    pub async fn get_user(&self, username: &str) -> MetastoreResult<Option<UserRecord>> {
        let query = format!("SELECT {USER_COLUMNS} FROM users WHERE username = $1");
        Ok(sqlx::query_as::<_, UserRecord>(sqlx::AssertSqlSafe(query))
            .bind(username)
            .fetch_optional(self.pool())
            .await?)
    }

    /// All users, ordered by username.
    pub async fn list_users(&self) -> MetastoreResult<Vec<UserRecord>> {
        let query = format!("SELECT {USER_COLUMNS} FROM users ORDER BY username");
        Ok(sqlx::query_as::<_, UserRecord>(sqlx::AssertSqlSafe(query))
            .fetch_all(self.pool())
            .await?)
    }

    /// Delete a user by username; false if no such user existed.
    pub async fn delete_user(&self, username: &str) -> MetastoreResult<bool> {
        let result = sqlx::query("DELETE FROM users WHERE username = $1")
            .bind(username)
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // ---- sessions ----

    /// Store a login session keyed by token hash, expiring after
    /// `ttl_secs`. Also sweeps already-expired sessions.
    pub async fn create_session(
        &self,
        token_hash: &str,
        user_id: i64,
        ttl_secs: f64,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + make_interval(secs => $3))",
        )
        .bind(token_hash)
        .bind(user_id)
        .bind(ttl_secs)
        .execute(self.pool())
        .await?;
        // Opportunistic cleanup of expired sessions.
        sqlx::query("DELETE FROM sessions WHERE expires_at < now()")
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Resolve a session token hash to its user; None when the token
    /// is unknown or the session has expired.
    pub async fn session_user(&self, token_hash: &str) -> MetastoreResult<Option<UserRecord>> {
        Ok(sqlx::query_as::<_, UserRecord>(
            "SELECT u.id, u.username, u.password_hash, u.role, u.streams
             FROM sessions s JOIN users u ON u.id = s.user_id
             WHERE s.token_hash = $1 AND s.expires_at > now()",
        )
        .bind(token_hash)
        .fetch_optional(self.pool())
        .await?)
    }

    // ---- api keys ----

    /// Create or update (by name) an API key, storing only the hash.
    pub async fn create_api_key(
        &self,
        name: &str,
        key_hash: &str,
        actions: &[String],
        streams: &[String],
    ) -> MetastoreResult<ApiKeyRecord> {
        Ok(sqlx::query_as::<_, ApiKeyRecord>(
            "INSERT INTO api_keys (name, key_hash, actions, streams)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE SET key_hash = EXCLUDED.key_hash,
                 actions = EXCLUDED.actions, streams = EXCLUDED.streams
             RETURNING id, name, actions, streams",
        )
        .bind(name)
        .bind(key_hash)
        .bind(actions)
        .bind(streams)
        .fetch_one(self.pool())
        .await?)
    }

    /// Resolve an API key by its hash, stamping `last_used_at` at most
    /// once per minute.
    pub async fn api_key_by_hash(&self, key_hash: &str) -> MetastoreResult<Option<ApiKeyRecord>> {
        // Throttle the last_used_at write to at most once per minute per
        // key so shipper auth doesn't generate a row write (+ dead tuples)
        // on every request. The UPDATE matches only when the stamp is
        // stale; a plain SELECT resolves the key otherwise.
        let updated = sqlx::query_as::<_, ApiKeyRecord>(
            "UPDATE api_keys
             SET last_used_at = now()
             WHERE key_hash = $1
               AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')
             RETURNING id, name, actions, streams",
        )
        .bind(key_hash)
        .fetch_optional(self.pool())
        .await?;
        if updated.is_some() {
            return Ok(updated);
        }
        Ok(sqlx::query_as::<_, ApiKeyRecord>(
            "SELECT id, name, actions, streams FROM api_keys WHERE key_hash = $1",
        )
        .bind(key_hash)
        .fetch_optional(self.pool())
        .await?)
    }

    /// All API keys, ordered by name.
    pub async fn list_api_keys(&self) -> MetastoreResult<Vec<ApiKeyRecord>> {
        Ok(sqlx::query_as::<_, ApiKeyRecord>(
            "SELECT id, name, actions, streams FROM api_keys ORDER BY name",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// Delete an API key by name; false if no such key existed.
    pub async fn delete_api_key(&self, name: &str) -> MetastoreResult<bool> {
        let result = sqlx::query("DELETE FROM api_keys WHERE name = $1")
            .bind(name)
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
