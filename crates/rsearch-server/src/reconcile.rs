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
//! Runs at startup (the rejoin case) and then periodically, which also
//! catches deletes missed while merely partitioned rather than down.

use std::time::Duration;

use rsearch_metastore::Metastore;
use rsearch_storage::{FsStorage, Storage};
use tracing::{info, warn};

/// Never sweep a file younger than this: a freshly transferred object's
/// placement row lands moments after the file, and peer transfers time
/// out after 600s — an hour comfortably outlives any in-flight write.
const ORPHAN_MIN_AGE: Duration = Duration::from_secs(3600);
/// Sweep cadence after the startup pass.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(3600);

/// Spawn the reconcile loop: one pass immediately (rejoin), then hourly.
pub fn spawn(fs: FsStorage, metastore: Metastore, node_id: String) {
    tokio::spawn(async move {
        loop {
            reconcile(&fs, &metastore, &node_id).await;
            tokio::time::sleep(RECONCILE_INTERVAL).await;
        }
    });
}

async fn reconcile(fs: &FsStorage, metastore: &Metastore, node_id: &str) {
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
    if announced > 0 {
        info!(announced, "reconcile: re-announced local object copies");
    }
    if swept > 0 {
        info!(swept, swept_bytes, "reconcile: deleted orphaned local objects");
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
