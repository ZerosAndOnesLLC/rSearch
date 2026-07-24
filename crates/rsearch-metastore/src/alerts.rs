//! Alert definitions and run-state updates.

use crate::error::{MetastoreError, MetastoreResult};
use crate::metastore::Metastore;

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct AlertRecord {
    pub id: i64,
    pub name: String,
    pub stream: String,
    pub query: serde_json::Value,
    pub condition_op: String,
    pub threshold: i64,
    pub window_secs: i64,
    pub interval_secs: i64,
    pub webhook_url: String,
    pub enabled: bool,
    pub last_status: Option<String>,
    pub last_count: Option<i64>,
}

const ALERT_COLUMNS: &str = "id, name, stream, query, condition_op, threshold, \
     window_secs, interval_secs, webhook_url, enabled, last_status, last_count";

impl Metastore {
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_alert(
        &self,
        name: &str,
        stream: &str,
        query: &serde_json::Value,
        condition_op: &str,
        threshold: i64,
        window_secs: i64,
        interval_secs: i64,
        webhook_url: &str,
        enabled: bool,
    ) -> MetastoreResult<AlertRecord> {
        if !matches!(condition_op, "gt" | "lt") {
            return Err(MetastoreError::Database(sqlx::Error::Configuration(
                "condition_op must be 'gt' or 'lt'".into(),
            )));
        }
        let query_sql = format!(
            "INSERT INTO alerts (name, stream, query, condition_op, threshold,
                                 window_secs, interval_secs, webhook_url, enabled)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (name) DO UPDATE SET stream = EXCLUDED.stream,
                 query = EXCLUDED.query, condition_op = EXCLUDED.condition_op,
                 threshold = EXCLUDED.threshold, window_secs = EXCLUDED.window_secs,
                 interval_secs = EXCLUDED.interval_secs,
                 webhook_url = EXCLUDED.webhook_url, enabled = EXCLUDED.enabled
             RETURNING {ALERT_COLUMNS}"
        );
        Ok(sqlx::query_as::<_, AlertRecord>(sqlx::AssertSqlSafe(query_sql))
            .bind(name)
            .bind(stream)
            .bind(query)
            .bind(condition_op)
            .bind(threshold)
            .bind(window_secs)
            .bind(interval_secs)
            .bind(webhook_url)
            .bind(enabled)
            .fetch_one(self.pool())
            .await?)
    }

    pub async fn list_alerts(&self) -> MetastoreResult<Vec<AlertRecord>> {
        let query = format!("SELECT {ALERT_COLUMNS} FROM alerts ORDER BY name");
        Ok(sqlx::query_as::<_, AlertRecord>(sqlx::AssertSqlSafe(query))
            .fetch_all(self.pool())
            .await?)
    }

    pub async fn delete_alert(&self, name: &str) -> MetastoreResult<bool> {
        let result = sqlx::query("DELETE FROM alerts WHERE name = $1")
            .bind(name)
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Enabled alerts whose interval has elapsed since their last run.
    pub async fn due_alerts(&self) -> MetastoreResult<Vec<AlertRecord>> {
        let query = format!(
            "SELECT {ALERT_COLUMNS} FROM alerts
             WHERE enabled
               AND (last_run_at IS NULL
                    OR last_run_at + make_interval(secs => interval_secs::float8) < now())
             ORDER BY name"
        );
        Ok(sqlx::query_as::<_, AlertRecord>(sqlx::AssertSqlSafe(query))
            .fetch_all(self.pool())
            .await?)
    }

    pub async fn record_alert_run(
        &self,
        name: &str,
        status: &str,
        count: Option<i64>,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "UPDATE alerts SET last_run_at = now(), last_status = $2, last_count = $3
             WHERE name = $1",
        )
        .bind(name)
        .bind(status)
        .bind(count)
        .execute(self.pool())
        .await?;
        Ok(())
    }
}
