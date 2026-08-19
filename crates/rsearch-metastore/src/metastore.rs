use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use rsearch_common::config::MetastoreConfig;

use crate::error::{MetastoreError, MetastoreResult};
use crate::types::{NewSplit, SplitRecord, SplitState, StreamMode, StreamRecord, StreamStats};

pub(crate) const SPLIT_COLUMNS: &str = "id, split_id, stream_id, state, storage_key, doc_count, \
     size_bytes, time_start_millis, time_end_millis, footer_len, created_by, \
     seq_min, seq_max, tombstone_seq_applied";

/// Postgres-backed metadata store shared by every node role: streams,
/// splits, nodes, placement, auth, routing rules, and alerts. Cloning
/// is cheap (shared pool).
#[derive(Clone)]
pub struct Metastore {
    pool: PgPool,
    /// Per-node held-bytes totals for replication target ranking. The
    /// backing SUM..GROUP BY over object_locations is O(table), so it is
    /// refreshed at most once per TTL instead of on every placement call
    /// (see `replication_targets`). Shared across clones.
    pub(crate) held_bytes: std::sync::Arc<
        std::sync::Mutex<Option<(std::collections::HashMap<String, i64>, std::time::Instant)>>,
    >,
}

impl Metastore {
    /// Connect and run pending migrations (embedded in the binary, so
    /// deployments never need external migration tooling).
    pub async fn connect(cfg: &MetastoreConfig) -> MetastoreResult<Self> {
        let url = if cfg.database_url.is_empty() {
            std::env::var("DATABASE_URL").unwrap_or_default()
        } else {
            cfg.database_url.clone()
        };
        if url.is_empty() {
            return Err(MetastoreError::Database(sqlx::Error::Configuration(
                "metastore.database_url (or DATABASE_URL) must be set".into(),
            )));
        }
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .connect(&url)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self {
            pool,
            held_bytes: Default::default(),
        })
    }

    /// The underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ---- streams ----

    /// Fetch-or-create a stream by name.
    pub async fn ensure_stream(&self, name: &str) -> MetastoreResult<StreamRecord> {
        let row = sqlx::query_as::<_, StreamRecord>(
            "INSERT INTO streams (name) VALUES ($1)
             ON CONFLICT (name) DO UPDATE SET updated_at = now()
             RETURNING id, name, mapping, retention_hours, mode",
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Look up a stream by name; `StreamNotFound` if missing.
    pub async fn get_stream(&self, name: &str) -> MetastoreResult<StreamRecord> {
        sqlx::query_as::<_, StreamRecord>(
            "SELECT id, name, mapping, retention_hours, mode FROM streams WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| MetastoreError::StreamNotFound(name.to_string()))
    }

    /// Per-stream stats over published splits (for `_cat/indices`).
    pub async fn stream_stats(&self) -> MetastoreResult<Vec<StreamStats>> {
        Ok(sqlx::query_as::<_, StreamStats>(
            "SELECT st.name, st.retention_hours, st.mode,
                    COUNT(s.id) FILTER (WHERE s.state = 'published') AS split_count,
                    COALESCE(SUM(s.doc_count) FILTER (WHERE s.state = 'published'), 0)::bigint AS doc_count,
                    COALESCE(SUM(s.size_bytes) FILTER (WHERE s.state = 'published'), 0)::bigint AS size_bytes
             FROM streams st
             LEFT JOIN splits s ON s.stream_id = st.id
             GROUP BY st.id
             ORDER BY st.name",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// Look up a stream by id; `StreamNotFound` if missing.
    pub async fn get_stream_by_id(&self, id: i64) -> MetastoreResult<StreamRecord> {
        sqlx::query_as::<_, StreamRecord>(
            "SELECT id, name, mapping, retention_hours, mode FROM streams WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| MetastoreError::StreamNotFound(format!("id={id}")))
    }

    /// All streams, ordered by name.
    pub async fn list_streams(&self) -> MetastoreResult<Vec<StreamRecord>> {
        Ok(sqlx::query_as::<_, StreamRecord>(
            "SELECT id, name, mapping, retention_hours, mode FROM streams ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// Replace a stream's mapping JSON (PUT /{index}); existing splits
    /// keep the schema they were built with.
    pub async fn update_stream_mapping(
        &self,
        name: &str,
        mapping: &serde_json::Value,
    ) -> MetastoreResult<()> {
        let result = sqlx::query(
            "UPDATE streams SET mapping = $2, updated_at = now() WHERE name = $1",
        )
        .bind(name)
        .bind(mapping)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(MetastoreError::StreamNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Fetch-or-create a stream with an explicit mode. An existing stream
    /// keeps its mode; the caller compares and decides (mode is fixed once
    /// the stream holds data — see [`Metastore::set_stream_mode`]).
    pub async fn ensure_stream_with_mode(
        &self,
        name: &str,
        mode: StreamMode,
    ) -> MetastoreResult<StreamRecord> {
        let row = sqlx::query_as::<_, StreamRecord>(
            "INSERT INTO streams (name, mode) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET updated_at = now()
             RETURNING id, name, mapping, retention_hours, mode",
        )
        .bind(name)
        .bind(mode.as_str())
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Change a stream's mode. Only allowed while the stream holds no
    /// splits (a log stream's documents have no tombstone semantics to
    /// retrofit); otherwise `StreamModeFixed`.
    pub async fn set_stream_mode(&self, name: &str, mode: StreamMode) -> MetastoreResult<()> {
        let result = sqlx::query(
            "UPDATE streams SET mode = $2, updated_at = now()
             WHERE name = $1
               AND NOT EXISTS (SELECT 1 FROM splits WHERE splits.stream_id = streams.id)",
        )
        .bind(name)
        .bind(mode.as_str())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            // Distinguish "no such stream" from "has data".
            self.get_stream(name).await?;
            return Err(MetastoreError::StreamModeFixed(name.to_string()));
        }
        Ok(())
    }

    /// Set (or clear, with None) a stream's retention window in hours.
    pub async fn set_stream_retention(
        &self,
        name: &str,
        retention_hours: Option<i32>,
    ) -> MetastoreResult<()> {
        let result = sqlx::query(
            "UPDATE streams SET retention_hours = $2, updated_at = now() WHERE name = $1",
        )
        .bind(name)
        .bind(retention_hours)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(MetastoreError::StreamNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Delete a stream row by name (idempotent).
    pub async fn delete_stream(&self, name: &str) -> MetastoreResult<()> {
        sqlx::query("DELETE FROM streams WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---- split lifecycle ----

    /// Register a freshly-uploaded split in `staged` state.
    pub async fn stage_split(&self, split: &NewSplit<'_>) -> MetastoreResult<()> {
        sqlx::query(
            "INSERT INTO splits (split_id, stream_id, storage_key, doc_count, size_bytes,
                                 time_start_millis, time_end_millis, footer_len, created_by,
                                 seq_min, seq_max, tombstone_seq_applied)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(split.split_id)
        .bind(split.stream_id)
        .bind(split.storage_key)
        .bind(split.doc_count)
        .bind(split.size_bytes)
        .bind(split.time_start_millis)
        .bind(split.time_end_millis)
        .bind(split.footer_len)
        .bind(split.created_by)
        .bind(split.seq_min)
        .bind(split.seq_max)
        .bind(split.tombstone_seq_applied)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// staged → published. Fails if the split is missing or already moved.
    pub async fn publish_split(&self, split_id: &str) -> MetastoreResult<()> {
        let result = sqlx::query(
            "UPDATE splits SET state = 'published', updated_at = now()
             WHERE split_id = $1 AND state = 'staged'",
        )
        .bind(split_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(MetastoreError::SplitStateConflict(split_id.to_string()));
        }
        Ok(())
    }

    /// Any state → marked_for_delete (idempotent).
    pub async fn mark_splits_for_delete(&self, split_ids: &[String]) -> MetastoreResult<u64> {
        let result = sqlx::query(
            "UPDATE splits SET state = 'marked_for_delete', updated_at = now()
             WHERE split_id = ANY($1) AND state <> 'marked_for_delete'",
        )
        .bind(split_ids)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Remove the metastore row (after the object is deleted from storage).
    pub async fn delete_split_row(&self, split_id: &str) -> MetastoreResult<()> {
        sqlx::query("DELETE FROM splits WHERE split_id = $1")
            .bind(split_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Published splits of a stream overlapping [start, end] millis
    /// (inclusive); pass None for an unbounded side. Bounded by `limit` so
    /// an unbounded-time query on a long-retention stream can't load every
    /// split row (callers request cap+1 to detect truncation).
    pub async fn splits_for_query(
        &self,
        stream_id: i64,
        time_start_millis: Option<i64>,
        time_end_millis: Option<i64>,
        limit: i64,
    ) -> MetastoreResult<Vec<SplitRecord>> {
        let query = format!(
            "SELECT {SPLIT_COLUMNS} FROM splits
             WHERE stream_id = $1 AND state = 'published'
               AND time_end_millis >= $2 AND time_start_millis <= $3
             ORDER BY time_start_millis
             LIMIT $4"
        );
        Ok(sqlx::query_as::<_, SplitRecord>(sqlx::AssertSqlSafe(query))
            .bind(stream_id)
            .bind(time_start_millis.unwrap_or(i64::MIN))
            .bind(time_end_millis.unwrap_or(i64::MAX))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?)
    }

    /// Up to `limit` splits in `state`, oldest update first (so the
    /// janitor and publisher drain backlogs in order).
    pub async fn splits_in_state(
        &self,
        state: SplitState,
        limit: i64,
    ) -> MetastoreResult<Vec<SplitRecord>> {
        let query = format!(
            "SELECT {SPLIT_COLUMNS} FROM splits WHERE state = $1
             ORDER BY updated_at LIMIT $2"
        );
        Ok(sqlx::query_as::<_, SplitRecord>(sqlx::AssertSqlSafe(query))
            .bind(state.as_str())
            .bind(limit)
            .fetch_all(&self.pool)
            .await?)
    }

    /// Look up a split by its split id.
    pub async fn get_split(&self, split_id: &str) -> MetastoreResult<Option<SplitRecord>> {
        let query = format!("SELECT {SPLIT_COLUMNS} FROM splits WHERE split_id = $1");
        Ok(sqlx::query_as::<_, SplitRecord>(sqlx::AssertSqlSafe(query))
            .bind(split_id)
            .fetch_optional(&self.pool)
            .await?)
    }
}
