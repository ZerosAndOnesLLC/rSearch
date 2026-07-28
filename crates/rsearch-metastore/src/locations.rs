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
            "SELECT n.id, n.roles, n.address,
                    EXTRACT(EPOCH FROM (now() - n.last_heartbeat))::float8 AS heartbeat_age_secs
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

    /// Keys with fewer than `replication_factor` copies on live nodes
    /// (heartbeat within `stale_after_secs`), most endangered first.
    /// Keys whose holders are all dead surface with live_holders = 0.
    pub async fn under_replicated_keys(
        &self,
        replication_factor: i64,
        stale_after_secs: f64,
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
             ORDER BY live_holders ASC, ol.storage_key
             LIMIT $3",
        )
        .bind(replication_factor)
        .bind(stale_after_secs)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}
