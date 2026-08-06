use crate::error::MetastoreResult;
use crate::metastore::Metastore;
use crate::types::NodeRecord;

impl Metastore {
    /// Upsert this node's liveness row. Called periodically by every node.
    /// Returns the node's draining flag so the node learns it was asked to
    /// drain without a separate poll (the upsert never resets the flag).
    pub async fn heartbeat(
        &self,
        node_id: &str,
        roles: &[String],
        address: Option<&str>,
    ) -> MetastoreResult<bool> {
        let row: (bool,) = sqlx::query_as(
            "INSERT INTO nodes (id, roles, address)
             VALUES ($1, $2, $3)
             ON CONFLICT (id) DO UPDATE
             SET roles = EXCLUDED.roles,
                 address = EXCLUDED.address,
                 last_heartbeat = now()
             RETURNING draining",
        )
        .bind(node_id)
        .bind(roles)
        .bind(address)
        .fetch_one(self.pool())
        .await?;
        Ok(row.0)
    }

    /// Flip a node's draining flag; false if the node is unknown. The
    /// drain start time is stamped on false→true, kept on repeated drain
    /// requests, and cleared on undrain.
    pub async fn set_node_draining(&self, node_id: &str, draining: bool) -> MetastoreResult<bool> {
        let result = sqlx::query(
            "UPDATE nodes
             SET draining = $2,
                 draining_since = CASE
                     WHEN NOT $2 THEN NULL
                     WHEN draining THEN draining_since
                     ELSE now()
                 END
             WHERE id = $1",
        )
        .bind(node_id)
        .bind(draining)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// All registered nodes with heartbeat age; callers decide staleness.
    pub async fn list_nodes(&self) -> MetastoreResult<Vec<NodeRecord>> {
        Ok(sqlx::query_as::<_, NodeRecord>(
            "SELECT id, roles, address, draining,
                    EXTRACT(EPOCH FROM (now() - last_heartbeat))::float8 AS heartbeat_age_secs,
                    EXTRACT(EPOCH FROM (now() - draining_since))::float8 AS draining_since_secs
             FROM nodes ORDER BY id",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// Nodes heartbeating within the last `stale_after_secs`.
    pub async fn live_nodes(&self, stale_after_secs: f64) -> MetastoreResult<Vec<NodeRecord>> {
        Ok(sqlx::query_as::<_, NodeRecord>(
            "SELECT id, roles, address, draining,
                    EXTRACT(EPOCH FROM (now() - last_heartbeat))::float8 AS heartbeat_age_secs,
                    EXTRACT(EPOCH FROM (now() - draining_since))::float8 AS draining_since_secs
             FROM nodes
             WHERE last_heartbeat > now() - make_interval(secs => $1)
             ORDER BY id",
        )
        .bind(stale_after_secs)
        .fetch_all(self.pool())
        .await?)
    }

    /// Remove nodes that have not heartbeated for `expire_after_secs`,
    /// along with their object placement rows — EXCEPT rows that are a
    /// key's only remaining copies. Purging those would permanently erase
    /// the object from the placement table (unreachable even after the
    /// node's volume returns intact); keeping them means the key stays
    /// visible as 0-live-holders, and a rejoining node revives its rows
    /// simply by heartbeating again. Rows are purged only when another
    /// copy survives on a non-expired node — i.e. repair actually did
    /// replace them, so a returning node can't resurrect stale placement.
    pub async fn expire_dead_nodes(&self, expire_after_secs: f64) -> MetastoreResult<u64> {
        let mut tx = self.pool().begin().await?;
        let expired: Vec<(String,)> = sqlx::query_as(
            "DELETE FROM nodes WHERE last_heartbeat < now() - make_interval(secs => $1)
             RETURNING id",
        )
        .bind(expire_after_secs)
        .fetch_all(&mut *tx)
        .await?;
        if !expired.is_empty() {
            let ids: Vec<String> = expired.iter().map(|(id,)| id.clone()).collect();
            sqlx::query(
                "DELETE FROM object_locations ol
                 WHERE ol.node_id = ANY($1)
                   AND EXISTS (
                       SELECT 1 FROM object_locations survivor
                       WHERE survivor.storage_key = ol.storage_key
                         AND NOT (survivor.node_id = ANY($1))
                         AND survivor.node_id IN (SELECT id FROM nodes)
                   )",
            )
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(expired.len() as u64)
    }
}
