use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use rsearch_common::config::MetastoreConfig;

use crate::error::{MetastoreError, MetastoreResult};
use crate::types::{SplitRecord, SplitState, StreamRecord};

const SPLIT_COLUMNS: &str = "id, split_id, stream_id, state, storage_key, doc_count, \
     size_bytes, time_start_millis, time_end_millis, footer_len, created_by";

#[derive(Clone)]
pub struct Metastore {
    pool: PgPool,
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
        sqlx::migrate!("../../migrations").run(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ---- streams ----

    /// Fetch-or-create a stream by name.
    pub async fn ensure_stream(&self, name: &str) -> MetastoreResult<StreamRecord> {
        let row = sqlx::query_as::<_, StreamRecord>(
            "INSERT INTO streams (name) VALUES ($1)
             ON CONFLICT (name) DO UPDATE SET updated_at = now()
             RETURNING id, name, mapping, retention_hours",
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_stream(&self, name: &str) -> MetastoreResult<StreamRecord> {
        sqlx::query_as::<_, StreamRecord>(
            "SELECT id, name, mapping, retention_hours FROM streams WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| MetastoreError::StreamNotFound(name.to_string()))
    }

    pub async fn get_stream_by_id(&self, id: i64) -> MetastoreResult<StreamRecord> {
        sqlx::query_as::<_, StreamRecord>(
            "SELECT id, name, mapping, retention_hours FROM streams WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| MetastoreError::StreamNotFound(format!("id={id}")))
    }

    pub async fn list_streams(&self) -> MetastoreResult<Vec<StreamRecord>> {
        Ok(sqlx::query_as::<_, StreamRecord>(
            "SELECT id, name, mapping, retention_hours FROM streams ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?)
    }

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

    pub async fn delete_stream(&self, name: &str) -> MetastoreResult<()> {
        sqlx::query("DELETE FROM streams WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---- split lifecycle ----

    /// Register a freshly-uploaded split in `staged` state.
    #[allow(clippy::too_many_arguments)]
    pub async fn stage_split(
        &self,
        split_id: &str,
        stream_id: i64,
        storage_key: &str,
        doc_count: i64,
        size_bytes: i64,
        time_start_millis: i64,
        time_end_millis: i64,
        footer_len: i64,
        created_by: Option<&str>,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "INSERT INTO splits (split_id, stream_id, storage_key, doc_count, size_bytes,
                                 time_start_millis, time_end_millis, footer_len, created_by)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(split_id)
        .bind(stream_id)
        .bind(storage_key)
        .bind(doc_count)
        .bind(size_bytes)
        .bind(time_start_millis)
        .bind(time_end_millis)
        .bind(footer_len)
        .bind(created_by)
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
    /// (inclusive); pass None for an unbounded side.
    pub async fn splits_for_query(
        &self,
        stream_id: i64,
        time_start_millis: Option<i64>,
        time_end_millis: Option<i64>,
    ) -> MetastoreResult<Vec<SplitRecord>> {
        let query = format!(
            "SELECT {SPLIT_COLUMNS} FROM splits
             WHERE stream_id = $1 AND state = 'published'
               AND time_end_millis >= $2 AND time_start_millis <= $3
             ORDER BY time_start_millis"
        );
        Ok(sqlx::query_as::<_, SplitRecord>(sqlx::AssertSqlSafe(query))
            .bind(stream_id)
            .bind(time_start_millis.unwrap_or(i64::MIN))
            .bind(time_end_millis.unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await?)
    }

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

    pub async fn get_split(&self, split_id: &str) -> MetastoreResult<Option<SplitRecord>> {
        let query = format!("SELECT {SPLIT_COLUMNS} FROM splits WHERE split_id = $1");
        Ok(sqlx::query_as::<_, SplitRecord>(sqlx::AssertSqlSafe(query))
            .bind(split_id)
            .fetch_optional(&self.pool)
            .await?)
    }
}
