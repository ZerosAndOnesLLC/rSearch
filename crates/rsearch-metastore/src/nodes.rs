use crate::error::MetastoreResult;
use crate::metastore::Metastore;
use crate::types::NodeRecord;

impl Metastore {
    /// Upsert this node's liveness row. Called periodically by every node.
    pub async fn heartbeat(
        &self,
        node_id: &str,
        roles: &[String],
        address: Option<&str>,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "INSERT INTO nodes (id, roles, address)
             VALUES ($1, $2, $3)
             ON CONFLICT (id) DO UPDATE
             SET roles = EXCLUDED.roles,
                 address = EXCLUDED.address,
                 last_heartbeat = now()",
        )
        .bind(node_id)
        .bind(roles)
        .bind(address)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// All registered nodes with heartbeat age; callers decide staleness.
    pub async fn list_nodes(&self) -> MetastoreResult<Vec<NodeRecord>> {
        Ok(sqlx::query_as::<_, NodeRecord>(
            "SELECT id, roles, address,
                    EXTRACT(EPOCH FROM (now() - last_heartbeat))::float8 AS heartbeat_age_secs
             FROM nodes ORDER BY id",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// Nodes heartbeating within the last `stale_after_secs`.
    pub async fn live_nodes(&self, stale_after_secs: f64) -> MetastoreResult<Vec<NodeRecord>> {
        Ok(sqlx::query_as::<_, NodeRecord>(
            "SELECT id, roles, address,
                    EXTRACT(EPOCH FROM (now() - last_heartbeat))::float8 AS heartbeat_age_secs
             FROM nodes
             WHERE last_heartbeat > now() - make_interval(secs => $1)
             ORDER BY id",
        )
        .bind(stale_after_secs)
        .fetch_all(self.pool())
        .await?)
    }

    /// Remove nodes that have not heartbeated for `expire_after_secs`.
    pub async fn expire_dead_nodes(&self, expire_after_secs: f64) -> MetastoreResult<u64> {
        let result = sqlx::query(
            "DELETE FROM nodes WHERE last_heartbeat < now() - make_interval(secs => $1)",
        )
        .bind(expire_after_secs)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }
}
