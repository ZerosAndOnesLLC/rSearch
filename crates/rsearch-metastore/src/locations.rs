use crate::error::MetastoreResult;
use crate::metastore::Metastore;
use crate::types::{NodeRecord, UnderReplicatedKey};

impl Metastore {
    /// Record that `node_id` holds a copy of `storage_key`. Idempotent:
    /// re-pushing the same (immutable) object refreshes the row.
    pub async fn record_object_location(
        &self,
        storage_key: &str,
        node_id: &str,
        size_bytes: i64,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "INSERT INTO object_locations (storage_key, node_id, size_bytes)
             VALUES ($1, $2, $3)
             ON CONFLICT (storage_key, node_id)
             DO UPDATE SET size_bytes = EXCLUDED.size_bytes",
        )
        .bind(storage_key)
        .bind(node_id)
        .bind(size_bytes)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Record a copy only if the object is still known to the placement
    /// table (some row for the key exists). Used by late-completing
    /// transfers — a replicate pull that outlives the leader's timeout,
    /// or a rejoining node re-announcing local files — so they can never
    /// resurrect placement for an object that was deleted cluster-wide.
    /// Returns whether the row was recorded.
    pub async fn record_object_location_if_known(
        &self,
        storage_key: &str,
        node_id: &str,
        size_bytes: i64,
    ) -> MetastoreResult<bool> {
        let result = sqlx::query(
            "INSERT INTO object_locations (storage_key, node_id, size_bytes)
             SELECT $1, $2, $3
             WHERE EXISTS (SELECT 1 FROM object_locations WHERE storage_key = $1)
             ON CONFLICT (storage_key, node_id)
             DO UPDATE SET size_bytes = EXCLUDED.size_bytes",
        )
        .bind(storage_key)
        .bind(node_id)
        .bind(size_bytes)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Remove one node's copy record for an object.
    pub async fn remove_object_location(
        &self,
        storage_key: &str,
        node_id: &str,
    ) -> MetastoreResult<()> {
        sqlx::query("DELETE FROM object_locations WHERE storage_key = $1 AND node_id = $2")
            .bind(storage_key)
            .bind(node_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Remove every copy record for an object (object deleted cluster-wide).
    pub async fn remove_object_locations(&self, storage_key: &str) -> MetastoreResult<()> {
        sqlx::query("DELETE FROM object_locations WHERE storage_key = $1")
            .bind(storage_key)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Nodes holding a copy of `storage_key` that have heartbeated within
    /// `stale_after_secs` — the candidates a reader may fetch from.
    pub async fn live_holders_of(
        &self,
        storage_key: &str,
        stale_after_secs: f64,
    ) -> MetastoreResult<Vec<NodeRecord>> {
        Ok(sqlx::query_as::<_, NodeRecord>(
            "SELECT n.id, n.roles, n.address, n.draining,
                    EXTRACT(EPOCH FROM (now() - n.last_heartbeat))::float8 AS heartbeat_age_secs,
                    EXTRACT(EPOCH FROM (now() - n.draining_since))::float8 AS draining_since_secs
             FROM object_locations ol
             JOIN nodes n ON n.id = ol.node_id
             WHERE ol.storage_key = $1
               AND n.last_heartbeat > now() - make_interval(secs => $2)
             ORDER BY n.id",
        )
        .bind(storage_key)
        .bind(stale_after_secs)
        .fetch_all(self.pool())
        .await?)
    }

    /// Storage keys recorded on one node, oldest first. Drives drain
    /// (copy everything off) and dead-node purge.
    pub async fn locations_on_node(
        &self,
        node_id: &str,
        limit: i64,
    ) -> MetastoreResult<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT storage_key FROM object_locations
             WHERE node_id = $1 ORDER BY created_at LIMIT $2",
        )
        .bind(node_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(|(key,)| key).collect())
    }

    /// Up to `limit` live nodes to receive new object copies, excluding
    /// `exclude` (typically the writer and existing holders), preferring
    /// nodes holding the fewest total bytes so fresh/empty nodes absorb
    /// writes first. With `include_draining`, draining nodes are eligible
    /// too, ranked after non-draining ones — repair's last resort when
    /// nothing else can take a copy (#4); the drain job moves such copies
    /// off again later.
    pub async fn replication_targets(
        &self,
        stale_after_secs: f64,
        exclude: &[String],
        limit: i64,
        include_draining: bool,
    ) -> MetastoreResult<Vec<NodeRecord>> {
        Ok(sqlx::query_as::<_, NodeRecord>(
            "SELECT n.id, n.roles, n.address, n.draining,
                    EXTRACT(EPOCH FROM (now() - n.last_heartbeat))::float8 AS heartbeat_age_secs,
                    EXTRACT(EPOCH FROM (now() - n.draining_since))::float8 AS draining_since_secs
             FROM nodes n
             LEFT JOIN (
                 SELECT node_id, SUM(size_bytes)::bigint AS bytes
                 FROM object_locations GROUP BY node_id
             ) held ON held.node_id = n.id
             WHERE n.last_heartbeat > now() - make_interval(secs => $1)
               AND n.id <> ALL($2)
               AND (NOT n.draining OR $4)
             ORDER BY n.draining ASC, COALESCE(held.bytes, 0) ASC, n.id
             LIMIT $3",
        )
        .bind(stale_after_secs)
        .bind(exclude)
        .bind(limit)
        .bind(include_draining)
        .fetch_all(self.pool())
        .await?)
    }

    /// Recorded size of an object, if any copy is registered.
    pub async fn object_size(&self, storage_key: &str) -> MetastoreResult<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT MAX(size_bytes) FROM object_locations WHERE storage_key = $1
             GROUP BY storage_key",
        )
        .bind(storage_key)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|(size,)| size))
    }

    /// Distinct keys under a prefix; the placement table is authoritative
    /// for cluster-wide listing.
    pub async fn object_keys_with_prefix(&self, prefix: &str) -> MetastoreResult<Vec<String>> {
        let pattern = format!(
            "{}%",
            prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
        );
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT storage_key FROM object_locations
             WHERE storage_key LIKE $1 ORDER BY storage_key",
        )
        .bind(pattern)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(|(key,)| key).collect())
    }

    /// Keys with fewer than `replication_factor` copies on live nodes
    /// (heartbeat within `stale_after_secs`), most endangered first.
    /// Keys whose holders are all dead surface with live_holders = 0.
    ///
    /// Keys whose newest placement row is younger than `min_age_secs` are
    /// skipped: a fresh write may still be pushing its remaining replicas
    /// (the quorum ack detaches the stragglers), and repairing mid-push
    /// would double-transfer the object and race the write's rollback.
    pub async fn under_replicated_keys(
        &self,
        replication_factor: i64,
        stale_after_secs: f64,
        min_age_secs: f64,
        limit: i64,
    ) -> MetastoreResult<Vec<UnderReplicatedKey>> {
        Ok(sqlx::query_as::<_, UnderReplicatedKey>(
            "SELECT ol.storage_key,
                    COUNT(n.id) FILTER (
                        WHERE n.last_heartbeat > now() - make_interval(secs => $2)
                    )::int8 AS live_holders
             FROM object_locations ol
             LEFT JOIN nodes n ON n.id = ol.node_id
             GROUP BY ol.storage_key
             HAVING COUNT(n.id) FILTER (
                        WHERE n.last_heartbeat > now() - make_interval(secs => $2)
                    ) < $1
                AND MAX(ol.created_at) < now() - make_interval(secs => $3)
             ORDER BY live_holders ASC, ol.storage_key
             LIMIT $4",
        )
        .bind(replication_factor)
        .bind(stale_after_secs)
        .bind(min_age_secs)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}
