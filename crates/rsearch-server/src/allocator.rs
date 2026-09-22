//! Process allocator: jemalloc (issue #90). glibc malloc gives each
//! thread that allocates an arena of its own, up to 8 x cores of them,
//! and rarely returns an arena's freed pages to the OS. A control node
//! running merges on short-lived blocking threads grew into every arena
//! and held gigabytes it no longer used. jemalloc purges freed pages on a
//! decay timer, from background threads so an idle process still shrinks.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Heap figures for `/metrics`, in bytes.
pub struct HeapStats {
    /// Bytes the process has allocated and not freed.
    pub allocated: u64,
    /// Bytes in physically resident pages the allocator maps — the
    /// allocator's share of RSS.
    pub resident: u64,
    /// Bytes mapped but returned to the OS, kept for reuse.
    pub retained: u64,
}

/// Start jemalloc's background purge threads. Without them, freed pages
/// are only purged when some thread next allocates.
pub fn enable_background_purge() {
    #[cfg(not(target_env = "msvc"))]
    if let Err(e) = tikv_jemalloc_ctl::background_thread::write(true) {
        tracing::warn!(error = %e, "jemalloc background purge threads unavailable");
    }
}

/// Current heap figures, or None when the allocator cannot report them.
pub fn heap_stats() -> Option<HeapStats> {
    #[cfg(not(target_env = "msvc"))]
    {
        use tikv_jemalloc_ctl::{epoch, stats};
        // Statistics are snapshots refreshed by advancing the epoch.
        epoch::advance().ok()?;
        Some(HeapStats {
            allocated: stats::allocated::read().ok()? as u64,
            resident: stats::resident::read().ok()? as u64,
            retained: stats::retained::read().ok()? as u64,
        })
    }
    #[cfg(target_env = "msvc")]
    None
}
