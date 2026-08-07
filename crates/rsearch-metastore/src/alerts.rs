//! Alert definitions and run-state updates.

use crate::error::{MetastoreError, MetastoreResult};
use crate::metastore::Metastore;

/// An alert definition: periodically run `query` over the trailing
/// window and fire a webhook when the hit count crosses `threshold`.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct AlertRecord {
    /// Primary key.
    pub id: i64,
    /// Unique alert name (upsert key).
    pub name: String,
    /// Stream (index) the query runs against.
    pub stream: String,
    /// ES query DSL body executed for each run.
    pub query: serde_json::Value,
    /// gt | lt — how the hit count is compared to `threshold`.
    pub condition_op: String,
    /// Hit-count threshold the comparison uses.
    pub threshold: i64,
    /// Trailing time window each run queries, in seconds.
    pub window_secs: i64,
    /// Minimum seconds between runs.
    pub interval_secs: i64,
    /// URL POSTed to when the condition is met.
    pub webhook_url: String,
    /// Disabled alerts are never scheduled.
    pub enabled: bool,
    /// Status recorded by the most recent run; None before the first.
    pub last_status: Option<String>,
    /// Hit count from the most recent run; None before the first.
    pub last_count: Option<i64>,
}

const ALERT_COLUMNS: &str = "id, name, stream, query, condition_op, threshold, \
     window_secs, interval_secs, webhook_url, enabled, last_status, last_count";

impl Metastore {
    /// Create or update (by name) an alert definition. Rejects any
    /// `condition_op` other than gt or lt.
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

    /// All alerts, ordered by name.
    pub async fn list_alerts(&self) -> MetastoreResult<Vec<AlertRecord>> {
        let query = format!("SELECT {ALERT_COLUMNS} FROM alerts ORDER BY name");
        Ok(sqlx::query_as::<_, AlertRecord>(sqlx::AssertSqlSafe(query))
            .fetch_all(self.pool())
            .await?)
    }

    /// Delete an alert by name; false if no such alert existed.
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

    /// Stamp an alert's last run: time, status, and hit count (None
    /// when the run failed before counting).
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
