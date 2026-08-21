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
    ///
    /// Returns `None` when the object is unknown (nothing recorded),
    /// `Some(true)` when a missing row was inserted, and `Some(false)`
    /// when an existing row was merely refreshed — callers gating on
    /// "still known" treat any `Some`, while the reconcile sweep counts
    /// only real inserts (`xmax = 0` distinguishes an inserted row from
    /// one rewritten by the conflict update).
    pub async fn record_object_location_if_known(
        &self,
        storage_key: &str,
        node_id: &str,
        size_bytes: i64,
    ) -> MetastoreResult<Option<bool>> {
        let row: Option<(bool,)> = sqlx::query_as(
            "INSERT INTO object_locations (storage_key, node_id, size_bytes)
             SELECT $1, $2, $3
             WHERE EXISTS (SELECT 1 FROM object_locations WHERE storage_key = $1)
             ON CONFLICT (storage_key, node_id)
             DO UPDATE SET size_bytes = EXCLUDED.size_bytes
             RETURNING (xmax = 0)",
        )
        .bind(storage_key)
        .bind(node_id)
        .bind(size_bytes)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|(inserted,)| inserted))
    }

    /// Whether any node other than `node_id` has a copy record for the
    /// object. The reconcile verify checks this before deleting a
    /// phantom row: removing the last record means no copy is known
    /// anywhere and the object may be lost — worth a louder signal than
    /// an ordinary phantom cleanup.
    pub async fn other_holders_exist(
        &self,
        storage_key: &str,
        node_id: &str,
    ) -> MetastoreResult<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM object_locations
                            WHERE storage_key = $1 AND node_id <> $2)",
        )
        .bind(storage_key)
        .bind(node_id)
        .fetch_one(self.pool())
        .await?;
        Ok(exists)
    }

    /// Whether the cluster still tracks `storage_key` at all — any
    /// placement row *or* split row referencing it counts. Deliberately
    /// broader than the `if_known` insert guard above: this gates local
    /// file deletion in the orphan reconcile sweep (#16), where a split
    /// row without location rows (however it arose) must protect the last
    /// physical copy rather than let it be swept.
    pub async fn object_known(&self, storage_key: &str) -> MetastoreResult<bool> {
        let known: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM object_locations WHERE storage_key = $1)
                 OR EXISTS (SELECT 1 FROM splits WHERE storage_key = $1)",
        )
        .bind(storage_key)
        .fetch_one(self.pool())
        .await?;
        Ok(known)
    }

    /// Distinct keys that have placement rows but no `splits` row of any
    /// state (#50): a crash between the storage put and the split-row
    /// insert — or a partial cluster delete — leaves rows and files that
    /// no other collector visits. GC walks `splits`, and the node-local
    /// reconcile sweep skips any key that still has a placement row, so
    /// the row keeps the file alive and the file keeps the row alive.
    /// Keys whose newest row is younger than `min_age_secs` are skipped:
    /// a fresh flush lands its placement rows moments before staging the
    /// split row.
    pub async fn stray_object_keys(
        &self,
        min_age_secs: f64,
        limit: i64,
    ) -> MetastoreResult<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT ol.storage_key FROM object_locations ol
             WHERE NOT EXISTS (SELECT 1 FROM splits s WHERE s.storage_key = ol.storage_key)
             GROUP BY ol.storage_key
             HAVING MAX(ol.created_at) < now() - make_interval(secs => $1)
             ORDER BY ol.storage_key
             LIMIT $2",
        )
        .bind(min_age_secs)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(|(key,)| key).collect())
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

    /// Storage keys recorded for `node_id` whose rows are older than
    /// `min_age_secs`, keyset-paginated: pass the last key of the previous
    /// page as `after_key` (empty string to start). Drives the
    /// metastore->disk reconcile verify (#44), which checks each recorded
    /// copy against the node's actual disk; the age floor keeps rows from
    /// any in-flight write out of the scan.
    pub async fn stale_locations_on_node(
        &self,
        node_id: &str,
        min_age_secs: f64,
        after_key: &str,
        limit: i64,
    ) -> MetastoreResult<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT storage_key FROM object_locations
             WHERE node_id = $1
               AND storage_key > $2
               AND created_at < now() - make_interval(secs => $3)
             ORDER BY storage_key LIMIT $4",
        )
        .bind(node_id)
        .bind(after_key)
        .bind(min_age_secs)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(|(key,)| key).collect())
    }

    /// Refresh interval for the per-node held-bytes totals used to rank
    /// replication targets. A few seconds of staleness only skews
    /// placement slightly; the aggregation it avoids is a full scan of
    /// object_locations per call.
    const HELD_BYTES_TTL: std::time::Duration = std::time::Duration::from_secs(5);

    /// Total recorded bytes per node, cached for [`Self::HELD_BYTES_TTL`].
    async fn node_held_bytes(&self) -> MetastoreResult<std::collections::HashMap<String, i64>> {
        if let Some((map, at)) = self.held_bytes.lock().unwrap().as_ref()
            && at.elapsed() < Self::HELD_BYTES_TTL
        {
            return Ok(map.clone());
        }
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT node_id, SUM(size_bytes)::bigint FROM object_locations GROUP BY node_id",
        )
        .fetch_all(self.pool())
        .await?;
        let map: std::collections::HashMap<String, i64> = rows.into_iter().collect();
        *self.held_bytes.lock().unwrap() = Some((map.clone(), std::time::Instant::now()));
        Ok(map)
    }

    /// Up to `limit` live nodes to receive new object copies, excluding
    /// `exclude` (typically the writer and existing holders), preferring
    /// nodes holding the fewest total bytes so fresh/empty nodes absorb
    /// writes first. With `include_draining`, draining nodes are eligible
    /// too, ranked after non-draining ones — repair's last resort when
    /// nothing else can take a copy (#4); the drain job moves such copies
    /// off again later.
    ///
    /// The held-bytes ranking comes from a short-TTL cache rather than an
    /// O(object_locations) aggregation per call — this runs on every
    /// replicated write and repair/drain key.
    pub async fn replication_targets(
        &self,
        stale_after_secs: f64,
        exclude: &[String],
        limit: i64,
        include_draining: bool,
    ) -> MetastoreResult<Vec<NodeRecord>> {
        let mut candidates = sqlx::query_as::<_, NodeRecord>(
            "SELECT n.id, n.roles, n.address, n.draining,
                    EXTRACT(EPOCH FROM (now() - n.last_heartbeat))::float8 AS heartbeat_age_secs,
                    EXTRACT(EPOCH FROM (now() - n.draining_since))::float8 AS draining_since_secs
             FROM nodes n
             WHERE n.last_heartbeat > now() - make_interval(secs => $1)
               AND n.id <> ALL($2)
               AND (NOT n.draining OR $3)",
        )
        .bind(stale_after_secs)
        .bind(exclude)
        .bind(include_draining)
        .fetch_all(self.pool())
        .await?;
        let held = self.node_held_bytes().await?;
        candidates.sort_by(|a, b| {
            let a_bytes = held.get(&a.id).copied().unwrap_or(0);
            let b_bytes = held.get(&b.id).copied().unwrap_or(0);
            a.draining
                .cmp(&b.draining)
                .then_with(|| a_bytes.cmp(&b_bytes))
                .then_with(|| a.id.cmp(&b.id))
        });
        candidates.truncate(limit.max(0) as usize);
        Ok(candidates)
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
