//! Local object reconciliation for the replicated backend (#16).
//!
//! A node that was down while the leader GC'd splits comes back holding
//! files the cluster no longer tracks: the metadata rows were deleted but
//! the peer DELETE never reached this node, and nothing cleaned the files
//! up afterwards — they accumulated forever. The reconcile sweep walks
//! the local object root and, per key:
//!
//! - **known placement** → re-announce our copy (the original rejoin-scan
//!   behavior: a node returning after registry expiry has real copies on
//!   disk but may have lost its rows);
//! - **tracked split with no placement rows** → resurrect a placement row
//!   unguarded, restoring availability (and letting GC delete a
//!   marked-for-delete split through its normal path);
//! - **unknown everywhere** → an orphan; delete the local file once it is
//!   older than a grace window, so an in-flight transfer whose row hasn't
//!   landed yet is never swept.
//!
//! The sweep also runs the opposite direction (#44): every placement row
//! recorded for this node is verified against local disk, and rows whose
//! file is gone are deleted. A node whose data volume was replaced keeps
//! its identity and heartbeats normally, so nothing else ever questions
//! its rows — the phantom copies count as healthy and mask real
//! under-replication from the repair job. Dropping the row is what lets
//! repair see the key and restore the factor. Safe because every write
//! path lands the file before inserting its row (local put, peer push,
//! replicate pull, and this sweep's own re-announce), backed by an age
//! floor on the rows scanned.
//!
//! Runs at startup (the rejoin case) and then periodically, which also
//! catches deletes missed while merely partitioned rather than down.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rsearch_metastore::Metastore;
use rsearch_storage::{FsStorage, Storage};
use tracing::{info, warn};

/// Reconcile activity counters for `/metrics`, present on
/// replicated-backend nodes.
#[derive(Default)]
pub struct ReconcileMetrics {
    /// Local copies re-announced to the placement table (#16).
    pub copies_announced: AtomicU64,
    /// Orphaned local object files deleted (#16).
    pub orphans_swept: AtomicU64,
    /// Placement rows removed because the recorded file was gone (#44).
    pub phantom_rows_removed: AtomicU64,
}

/// Never sweep a file younger than this: a freshly transferred object's
/// placement row lands moments after the file, and peer transfers time
/// out after 600s — an hour comfortably outlives any in-flight write.
const ORPHAN_MIN_AGE: Duration = Duration::from_secs(3600);
/// Sweep cadence after the startup pass.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(3600);
/// Only verify placement rows older than this against disk. Rows are
/// written strictly after their file, so any age floor suffices; an hour
/// is pure defense in depth (a future write path that inverts the order,
/// clock skew) at no cost — a phantom row from a replaced volume is
/// either already old or ages into the scan within one sweep interval.
const VERIFY_MIN_ROW_AGE: Duration = Duration::from_secs(3600);
/// Placement rows fetched per verify page (keyset-paginated).
const VERIFY_BATCH: i64 = 1000;

/// Spawn the reconcile loop: one pass immediately (rejoin), then hourly.
pub fn spawn(
    fs: FsStorage,
    metastore: Metastore,
    node_id: String,
    metrics: Arc<ReconcileMetrics>,
) {
    tokio::spawn(async move {
        loop {
            reconcile(&fs, &metastore, &node_id, &metrics).await;
            verify_placements(&fs, &metastore, &node_id, &metrics).await;
            tokio::time::sleep(RECONCILE_INTERVAL).await;
        }
    });
}

async fn reconcile(
    fs: &FsStorage,
    metastore: &Metastore,
    node_id: &str,
    metrics: &ReconcileMetrics,
) {
    let keys = match fs.list("").await {
        Ok(keys) => keys,
        Err(e) => {
            warn!(error = %e, "reconcile: listing local objects failed");
            return;
        }
    };
    let mut announced = 0u64;
    let mut swept = 0u64;
    let mut swept_bytes = 0u64;
    for key in keys {
        let Ok(size) = fs.size(&key).await else { continue };
        match metastore
            .record_object_location_if_known(&key, node_id, size as i64)
            .await
        {
            Ok(true) => announced += 1,
            Ok(false) => {
                if sweep_orphan(fs, metastore, node_id, &key, size).await {
                    swept += 1;
                    swept_bytes += size;
                }
            }
            Err(e) => warn!(key, error = %e, "reconcile: record failed"),
        }
    }
    metrics.copies_announced.fetch_add(announced, Ordering::Relaxed);
    metrics.orphans_swept.fetch_add(swept, Ordering::Relaxed);
    if announced > 0 {
        info!(announced, "reconcile: re-announced local object copies");
    }
    if swept > 0 {
        info!(swept, swept_bytes, "reconcile: deleted orphaned local objects");
    }
}

/// Metastore->disk direction (#44): delete this node's placement rows
/// whose file is not on local disk, so under-replication stops hiding
/// behind them and repair can restore the factor. Only a definitive
/// NotFound removes a row — any other storage error keeps it, since
/// deleting on a transient fault could strip the last record of a real
/// copy (even then the next sweep's disk walk would resurrect it).
async fn verify_placements(
    fs: &FsStorage,
    metastore: &Metastore,
    node_id: &str,
    metrics: &ReconcileMetrics,
) {
    let mut after = String::new();
    let mut removed = 0u64;
    loop {
        let keys = match metastore
            .stale_locations_on_node(
                node_id,
                VERIFY_MIN_ROW_AGE.as_secs_f64(),
                &after,
                VERIFY_BATCH,
            )
            .await
        {
            Ok(keys) => keys,
            Err(e) => {
                warn!(error = %e, "reconcile: listing own placement rows failed");
                break;
            }
        };
        let Some(last) = keys.last() else { break };
        after = last.clone();
        for key in &keys {
            match fs.exists(key).await {
                Ok(true) => {}
                Ok(false) => match metastore.remove_object_location(key, node_id).await {
                    Ok(()) => {
                        removed += 1;
                        warn!(key, "reconcile: removed phantom placement row (file not on disk)");
                    }
                    Err(e) => warn!(key, error = %e, "reconcile: phantom row delete failed"),
                },
                Err(e) => warn!(key, error = %e, "reconcile: disk check failed; keeping row"),
            }
        }
        if keys.len() < VERIFY_BATCH as usize {
            break;
        }
    }
    metrics.phantom_rows_removed.fetch_add(removed, Ordering::Relaxed);
    if removed > 0 {
        info!(
            removed,
            "reconcile: removed placement rows for files missing from local disk"
        );
    }
}

/// Handle a key with no placement rows anywhere. Returns whether the
/// local file was deleted.
async fn sweep_orphan(
    fs: &FsStorage,
    metastore: &Metastore,
    node_id: &str,
    key: &str,
    size: u64,
) -> bool {
    // The guarded insert refuses when object_locations is empty for the
    // key; a split row referencing it means the object still matters —
    // resurrect our placement row so search/repair (or GC, for a
    // marked-for-delete split) can reach this copy again.
    match metastore.object_known(key).await {
        Ok(true) => {
            match metastore
                .record_object_location(key, node_id, size as i64)
                .await
            {
                Ok(()) => {
                    info!(key, "reconcile: restored placement for tracked split");
                }
                Err(e) => warn!(key, error = %e, "reconcile: placement restore failed"),
            }
            return false;
        }
        Ok(false) => {}
        Err(e) => {
            warn!(key, error = %e, "reconcile: metastore lookup failed; keeping file");
            return false;
        }
    }
    // Unknown everywhere: an orphan unless it is too young to judge.
    match fs.modified(key).await {
        Ok(modified) => {
            let age = std::time::SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::ZERO);
            if age < ORPHAN_MIN_AGE {
                return false;
            }
        }
        Err(_) => return false,
    }
    match fs.delete(key).await {
        Ok(()) => {
            info!(key, size, "reconcile: deleted orphaned object file");
            true
        }
        Err(e) => {
            warn!(key, error = %e, "reconcile: orphan delete failed");
            false
        }
    }
}
