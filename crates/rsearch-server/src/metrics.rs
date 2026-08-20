//! Prometheus exposition: `GET /metrics` renders node-local ingest/WAL
//! and bulk-handoff counters, cluster node gauges, and (on control
//! nodes) repair/drain activity in text format 0.0.4. Hand-rolled —
//! every value is already an atomic or one cheap metastore query, so no
//! metrics crate is needed.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

/// Control-plane activity counters, shared between the control loop and
/// the HTTP handler. Present only on nodes running the control role;
/// series stay at zero on nodes that never hold leadership.
#[derive(Default)]
pub struct ControlMetrics {
    /// 1 while this node holds control leadership.
    pub leader: AtomicBool,
    /// Under-replicated keys seen by the last repair scan (capped at the
    /// scan batch size — treat as "pressure", not an exact backlog).
    pub repair_pending_keys: AtomicU64,
    pub repair_copies_restored: AtomicU64,
    pub repair_failures: AtomicU64,
    pub drain_copies_moved: AtomicU64,
    pub drain_failures: AtomicU64,
    /// Document-mode compaction: splits rewritten without hidden versions.
    pub compactions: AtomicU64,
    /// Documents physically removed by compaction rewrites.
    pub compacted_docs: AtomicU64,
    /// Tombstone rows purged from the metastore.
    pub tombstones_purged: AtomicU64,
    /// Tombstone rows pending (as of the last compaction scan).
    pub tombstones_pending: AtomicU64,
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

pub async fn metrics(State(state): State<AppState>) -> Response {
    let mut out = String::with_capacity(4096);

    gauge(
        &mut out,
        "rsearch_node_draining",
        "1 while this node is draining (refusing bulk ingest ahead of decommission).",
        state.draining.load(Ordering::Relaxed) as u64,
    );

    if let Some(pipeline) = &state.pipeline {
        let m = pipeline.metrics();
        counter(
            &mut out,
            "rsearch_ingest_enqueued_docs_total",
            "Documents accepted into the ingest pipeline.",
            m.docs_enqueued.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_ingest_enqueued_bytes_total",
            "Source bytes accepted into the ingest pipeline.",
            m.bytes_enqueued.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_ingest_indexed_docs_total",
            "Documents indexed into published splits.",
            m.docs_indexed.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_ingest_published_splits_total",
            "Splits built, uploaded, and published.",
            m.splits_published.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_ingest_flush_failures_total",
            "Failed flush attempts (each is retried; docs stay WAL-durable).",
            m.flush_failures.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "rsearch_ingest_queue_depth",
            "Documents buffered in worker queues, not yet flushed to a split.",
            m.queue_depth.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "rsearch_wal_outstanding_records",
            "WAL records not yet confirmed into a published split (replayed on restart).",
            pipeline.wal().outstanding(),
        );
        gauge(
            &mut out,
            "rsearch_wal_segments",
            "Live WAL segment files on disk.",
            pipeline.wal().segment_count() as u64,
        );
    }

    if let Some(reconcile) = &state.reconcile {
        counter(
            &mut out,
            "rsearch_reconcile_copies_announced_total",
            "Placement rows inserted by the reconcile sweep for local files the table had lost track of.",
            reconcile.copies_announced.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_reconcile_orphans_swept_total",
            "Orphaned local object files deleted by the reconcile sweep.",
            reconcile.orphans_swept.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_reconcile_phantom_placements_removed_total",
            "Placement rows removed because the recorded file was missing from local disk.",
            reconcile.phantom_rows_removed.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_reconcile_last_copy_placements_removed_total",
            "Phantom rows removed that were the object's last recorded copy — the object may be lost; alert on any increase.",
            reconcile.last_copy_rows_removed.load(Ordering::Relaxed),
        );
    }

    if let Some(control) = &state.control {
        gauge(
            &mut out,
            "rsearch_control_leader",
            "1 while this node holds control leadership.",
            control.leader.load(Ordering::Relaxed) as u64,
        );
        gauge(
            &mut out,
            "rsearch_repair_pending_keys",
            "Under-replicated keys seen by the last repair scan (capped at the scan batch size).",
            control.repair_pending_keys.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_repair_copies_restored_total",
            "Object copies restored by the repair job.",
            control.repair_copies_restored.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_repair_failures_total",
            "Repair copy attempts that failed or timed out.",
            control.repair_failures.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_drain_copies_moved_total",
            "Object copies moved off draining nodes by the drain job.",
            control.drain_copies_moved.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_drain_failures_total",
            "Drain copy attempts that failed or timed out.",
            control.drain_failures.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_compactions_total",
            "Document-mode splits rewritten without their tombstoned versions.",
            control.compactions.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_compacted_docs_total",
            "Document versions physically removed by compaction.",
            control.compacted_docs.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "rsearch_tombstones_pending",
            "Tombstone rows in the metastore as of the last compaction scan.",
            control.tombstones_pending.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_tombstones_purged_total",
            "Tombstone rows purged once no split could still hold a hidden version.",
            control.tombstones_purged.load(Ordering::Relaxed),
        );
    }

    if let Some(forwarder) = &state.bulk_forward {
        counter(
            &mut out,
            "rsearch_bulk_forwarded_total",
            "Bulk batches handed off to an ingest peer for balancing.",
            forwarder.forwarded.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_bulk_forward_fallbacks_total",
            "Bulk handoffs that fell back to local ingest.",
            forwarder.forward_fallbacks.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "rsearch_bulk_received_total",
            "Bulk batches accepted from a peer handoff (counted at acceptance, before local indexing).",
            forwarder.received.load(Ordering::Relaxed),
        );
    }

    match state.metastore.list_nodes().await {
        Ok(nodes) => {
            let live = nodes.iter().filter(|n| n.heartbeat_age_secs < 30.0).count();
            let draining = nodes.iter().filter(|n| n.draining).count();
            gauge(
                &mut out,
                "rsearch_cluster_nodes",
                "Nodes registered in the metastore (including dead, pre-expiry).",
                nodes.len() as u64,
            );
            gauge(
                &mut out,
                "rsearch_cluster_nodes_live",
                "Nodes that heartbeated within the last 30s.",
                live as u64,
            );
            gauge(
                &mut out,
                "rsearch_cluster_nodes_draining",
                "Nodes with the draining flag set (excluded from writes and new copies).",
                draining as u64,
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("metastore unavailable: {e}"),
            )
                .into_response();
        }
    }

    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        out,
    )
        .into_response()
}
