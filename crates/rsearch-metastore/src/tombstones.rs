//! Document-mode tombstones: per-(stream, `_id`) "hide versions older than
//! `before_seq`" markers, applied at query time and made physical by
//! compaction (phase 14).

use crate::error::MetastoreResult;
use crate::metastore::Metastore;

/// One tombstone row.
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct TombstoneRecord {
    /// Monotonic tombstone ordinal (re-issued when a row is updated), the
    /// cursor incremental readers page by.
    pub seq: i64,
    /// The document id the tombstone hides versions of.
    pub doc_id: String,
    /// Versions with `_seq` strictly below this are hidden.
    pub before_seq: i64,
}

/// A tombstone to write: (stream id, document id, before_seq).
#[derive(Debug, Clone)]
pub struct NewTombstone {
    /// Owning stream id.
    pub stream_id: i64,
    /// Document id.
    pub doc_id: String,
    /// Hide versions with `_seq` below this.
    pub before_seq: i64,
}

impl Metastore {
    /// Upsert a batch of tombstones in one statement. An existing row for
    /// the same (stream, doc) is raised to a greater `before_seq` and
    /// re-sequenced so readers paging by `seq` see the change; a lower or
    /// equal bound leaves it untouched.
    pub async fn upsert_tombstones(&self, items: &[NewTombstone]) -> MetastoreResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let stream_ids: Vec<i64> = items.iter().map(|t| t.stream_id).collect();
        let doc_ids: Vec<String> = items.iter().map(|t| t.doc_id.clone()).collect();
        let before_seqs: Vec<i64> = items.iter().map(|t| t.before_seq).collect();
        // Within one statement the same (stream, doc) may appear twice
        // (two writes to one id in a batch); ON CONFLICT can't update a row
        // twice, so collapse duplicates to their max before_seq first.
        sqlx::query(
            "INSERT INTO doc_tombstones (stream_id, doc_id, before_seq)
             SELECT stream_id, doc_id, MAX(before_seq)
             FROM UNNEST($1::bigint[], $2::text[], $3::bigint[]) AS t(stream_id, doc_id, before_seq)
             GROUP BY stream_id, doc_id
             ON CONFLICT (stream_id, doc_id) DO UPDATE
               SET before_seq = EXCLUDED.before_seq,
                   seq = DEFAULT,
                   created_at = now()
               WHERE EXCLUDED.before_seq > doc_tombstones.before_seq",
        )
        .bind(&stream_ids)
        .bind(&doc_ids)
        .bind(&before_seqs)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Tombstones of a stream with `seq > after_seq`, ascending, up to
    /// `limit` — the incremental page readers use to extend their cache.
    pub async fn tombstones_since(
        &self,
        stream_id: i64,
        after_seq: i64,
        limit: i64,
    ) -> MetastoreResult<Vec<TombstoneRecord>> {
        Ok(sqlx::query_as::<_, TombstoneRecord>(
            "SELECT seq, doc_id, before_seq FROM doc_tombstones
             WHERE stream_id = $1 AND seq > $2
             ORDER BY seq
             LIMIT $3",
        )
        .bind(stream_id)
        .bind(after_seq)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    /// Number of tombstone rows for a stream.
    pub async fn tombstone_count(&self, stream_id: i64) -> MetastoreResult<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM doc_tombstones WHERE stream_id = $1",
            )
            .bind(stream_id)
            .fetch_one(self.pool())
            .await?,
        )
    }
}

/// Per-stream tombstone rollup for the compaction job.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StreamTombstoneStats {
    /// Stream id.
    pub stream_id: i64,
    /// Tombstone rows for the stream.
    pub count: i64,
    /// Age of the oldest row, seconds.
    pub oldest_age_secs: f64,
    /// Highest tombstone seq.
    pub max_seq: i64,
}

impl Metastore {
    /// Tombstone rollups for every stream that has any, oldest first.
    pub async fn tombstone_stats(&self) -> MetastoreResult<Vec<StreamTombstoneStats>> {
        Ok(sqlx::query_as::<_, StreamTombstoneStats>(
            "SELECT stream_id, COUNT(*) AS count,
                    EXTRACT(EPOCH FROM (now() - MIN(created_at)))::float8 AS oldest_age_secs,
                    MAX(seq) AS max_seq
             FROM doc_tombstones
             GROUP BY stream_id
             ORDER BY MIN(created_at)",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// Published splits of a stream that carry ids and have not yet
    /// applied tombstones up to `through_seq`, oldest first.
    pub async fn splits_needing_compaction(
        &self,
        stream_id: i64,
        through_seq: i64,
        limit: i64,
    ) -> MetastoreResult<Vec<crate::types::SplitRecord>> {
        let query = format!(
            "SELECT {} FROM splits
             WHERE stream_id = $1 AND state = 'published'
               AND seq_min IS NOT NULL AND tombstone_seq_applied < $2
             ORDER BY tombstone_seq_applied, time_start_millis
             LIMIT $3",
            crate::metastore::SPLIT_COLUMNS
        );
        Ok(
            sqlx::query_as::<_, crate::types::SplitRecord>(sqlx::AssertSqlSafe(query))
                .bind(stream_id)
                .bind(through_seq)
                .bind(limit)
                .fetch_all(self.pool())
                .await?,
        )
    }

    /// Record that a split holds no version hidden by any tombstone up to
    /// `through_seq` (verified by the compaction job), so those tombstones
    /// no longer need to be applied to it or kept for it.
    pub async fn mark_tombstones_applied(
        &self,
        split_id: &str,
        through_seq: i64,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "UPDATE splits SET tombstone_seq_applied = GREATEST(tombstone_seq_applied, $2),
                               updated_at = now()
             WHERE split_id = $1",
        )
        .bind(split_id)
        .bind(through_seq)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Delete tombstones older than `grace_secs` that no staged/published
    /// split can still hold a hidden version for. Returns rows removed.
    pub async fn purge_tombstones(&self, grace_secs: f64, limit: i64) -> MetastoreResult<u64> {
        let result = sqlx::query(
            "DELETE FROM doc_tombstones t
             WHERE t.seq IN (
                 SELECT c.seq FROM doc_tombstones c
                 WHERE c.created_at < now() - make_interval(secs => $1)
                   AND NOT EXISTS (
                       SELECT 1 FROM splits s
                       WHERE s.stream_id = c.stream_id
                         AND s.state IN ('staged', 'published')
                         AND s.seq_min IS NOT NULL
                         AND s.seq_min < c.before_seq
                         AND s.tombstone_seq_applied < c.seq
                   )
                 LIMIT $2
             )",
        )
        .bind(grace_secs)
        .bind(limit)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }
}
