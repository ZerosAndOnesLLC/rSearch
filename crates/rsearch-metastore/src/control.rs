//! Metastore operations for the control plane: advisory-lock leadership,
//! transactional split swaps (merge), retention, and GC queries.

use sqlx::Row;

use crate::error::{MetastoreError, MetastoreResult};
use crate::metastore::Metastore;
use crate::types::SplitRecord;

/// Cluster-wide advisory lock key for control leadership.
pub const CONTROL_LEADER_LOCK: i64 = 0x7273_6561_7263_6801; // "rsearch\x01"

impl Metastore {
    /// Try to become leader on a dedicated connection. The lock lives for
    /// the connection's lifetime — hold the returned connection to stay
    /// leader; drop it to abdicate.
    pub async fn try_acquire_leadership(
        &self,
    ) -> MetastoreResult<Option<sqlx::pool::PoolConnection<sqlx::Postgres>>> {
        let mut conn = self.pool().acquire().await?;
        let row = sqlx::query("SELECT pg_try_advisory_lock($1) AS locked")
            .bind(CONTROL_LEADER_LOCK)
            .fetch_one(&mut *conn)
            .await?;
        let locked: bool = row.get("locked");
        Ok(if locked { Some(conn) } else { None })
    }

    /// Liveness probe for the leadership connection.
    pub async fn leadership_alive(
        conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    ) -> bool {
        sqlx::query("SELECT 1").execute(&mut **conn).await.is_ok()
    }

    /// Explicitly release leadership. The advisory lock is session-scoped,
    /// so a healthy `PoolConnection` returned to the pool would keep the
    /// lock held forever (no node could become leader again). Unlock, then
    /// detach the connection so it is closed rather than reused (M7).
    pub async fn release_leadership(
        conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
    ) {
        let mut conn = conn;
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(CONTROL_LEADER_LOCK)
            .execute(&mut *conn)
            .await;
        // Detach so the connection is dropped (closed), not pooled with a
        // possibly-lingering session lock.
        let detached = conn.detach();
        drop(detached);
    }

    /// Atomically publish a merged split and mark its sources for delete.
    /// Rolls back entirely if the new split isn't in `staged` state.
    pub async fn swap_splits(
        &self,
        old_split_ids: &[String],
        new_split_id: &str,
    ) -> MetastoreResult<()> {
        let mut tx = self.pool().begin().await?;
        let published = sqlx::query(
            "UPDATE splits SET state = 'published', updated_at = now()
             WHERE split_id = $1 AND state = 'staged'",
        )
        .bind(new_split_id)
        .execute(&mut *tx)
        .await?;
        if published.rows_affected() == 0 {
            return Err(MetastoreError::SplitStateConflict(new_split_id.to_string()));
        }
        sqlx::query(
            "UPDATE splits SET state = 'marked_for_delete', updated_at = now()
             WHERE split_id = ANY($1)",
        )
        .bind(old_split_ids)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Published splits smaller than `max_size_bytes`, grouped by stream,
    /// ordered by time — merge candidates.
    pub async fn small_published_splits(
        &self,
        max_size_bytes: i64,
        limit: i64,
    ) -> MetastoreResult<Vec<SplitRecord>> {
        Ok(sqlx::query_as::<_, SplitRecord>(
            "SELECT id, split_id, stream_id, state, storage_key, doc_count, size_bytes,
                    time_start_millis, time_end_millis, footer_len, created_by
             FROM splits
             WHERE state = 'published' AND size_bytes < $1
             ORDER BY stream_id, time_start_millis
             LIMIT $2",
        )
        .bind(max_size_bytes)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    /// Published splits past their stream's retention window.
    pub async fn expired_splits(&self, now_millis: i64) -> MetastoreResult<Vec<String>> {
        let rows = sqlx::query(
            "SELECT s.split_id FROM splits s
             JOIN streams st ON st.id = s.stream_id
             WHERE s.state = 'published'
               AND st.retention_hours IS NOT NULL
               AND s.time_end_millis < $1 - (st.retention_hours::bigint * 3600000)",
        )
        .bind(now_millis)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.iter().map(|r| r.get::<String, _>("split_id")).collect())
    }

    /// Splits stuck in `staged` past `older_than_secs` — a crash between
    /// stage_split and publish leaves these behind. Returned so the
    /// control leader can mark them for deletion (M5).
    pub async fn stale_staged_splits(
        &self,
        older_than_secs: f64,
        limit: i64,
    ) -> MetastoreResult<Vec<SplitRecord>> {
        Ok(sqlx::query_as::<_, SplitRecord>(
            "SELECT id, split_id, stream_id, state, storage_key, doc_count, size_bytes,
                    time_start_millis, time_end_millis, footer_len, created_by
             FROM splits
             WHERE state = 'staged'
               AND created_at < now() - make_interval(secs => $1)
             ORDER BY created_at
             LIMIT $2",
        )
        .bind(older_than_secs)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    /// Splits marked for delete whose grace period has elapsed.
    pub async fn gc_candidates(
        &self,
        grace_secs: f64,
        limit: i64,
    ) -> MetastoreResult<Vec<SplitRecord>> {
        Ok(sqlx::query_as::<_, SplitRecord>(
            "SELECT id, split_id, stream_id, state, storage_key, doc_count, size_bytes,
                    time_start_millis, time_end_millis, footer_len, created_by
             FROM splits
             WHERE state = 'marked_for_delete'
               AND updated_at < now() - make_interval(secs => $1)
             ORDER BY updated_at
             LIMIT $2",
        )
        .bind(grace_secs)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}
