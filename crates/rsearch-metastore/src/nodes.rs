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

    /// Flip a node's draining flag; false if the node is unknown.
    pub async fn set_node_draining(&self, node_id: &str, draining: bool) -> MetastoreResult<bool> {
        let result = sqlx::query("UPDATE nodes SET draining = $2 WHERE id = $1")
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
                    EXTRACT(EPOCH FROM (now() - last_heartbeat))::float8 AS heartbeat_age_secs
             FROM nodes ORDER BY id",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// Nodes heartbeating within the last `stale_after_secs`.
    pub async fn live_nodes(&self, stale_after_secs: f64) -> MetastoreResult<Vec<NodeRecord>> {
        Ok(sqlx::query_as::<_, NodeRecord>(
            "SELECT id, roles, address, draining,
                    EXTRACT(EPOCH FROM (now() - last_heartbeat))::float8 AS heartbeat_age_secs
             FROM nodes
             WHERE last_heartbeat > now() - make_interval(secs => $1)
             ORDER BY id",
        )
        .bind(stale_after_secs)
        .fetch_all(self.pool())
        .await?)
    }

    /// Remove nodes that have not heartbeated for `expire_after_secs`,
    /// along with their object placement rows (an expired node's copies
    /// are unreachable; repair has had ample time to replace them, and a
    /// returning node must not resurrect stale placement).
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
            sqlx::query("DELETE FROM object_locations WHERE node_id = ANY($1)")
                .bind(&ids)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(expired.len() as u64)
    }
}
