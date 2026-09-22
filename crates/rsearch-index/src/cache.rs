use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use tantivy::directory::OwnedBytes;

/// Granularity of split reads and of the cache (issue #89). Reads are
/// served in aligned blocks of this size, so opening a split costs the
/// footer blocks of its files rather than the files themselves, and a
/// query pays only for the blocks it touches.
pub const DEFAULT_BLOCK_SIZE: u64 = 256 << 10;

/// Memory-tier shards: every split read looks a block up here, so one
/// lock would serialize concurrent split searches.
const MEMORY_SHARDS: usize = 16;

/// One bundled file of one split (`"<split_id>/<file_name>"`). Built once
/// per file handle and shared by every block key of that file, so a
/// lookup clones an `Arc` instead of formatting a string.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FileKey(Arc<str>);

impl FileKey {
    /// The key of `file_name` inside split `split_id`.
    pub fn new(split_id: &str, file_name: &str) -> Self {
        Self(Arc::from(format!("{split_id}/{file_name}")))
    }

    /// `"<split_id>/<file_name>"`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

type BlockKey = (FileKey, u64);

/// Where a block lives on disk, relative to the cache root.
fn block_relative_path(file: &FileKey, index: u64) -> String {
    format!("{}.b{index}", file.as_str())
}

/// Budgets and block size of a [`SplitCache`].
#[derive(Debug, Clone)]
pub struct CacheOptions {
    /// Disk-tier root; None keeps blocks in memory only (a private scan
    /// cache that must never touch the node's shared cache).
    pub root: Option<PathBuf>,
    /// Disk-tier budget in bytes.
    pub disk_bytes: u64,
    /// Memory-tier budget in bytes; 0 disables the memory tier.
    pub memory_bytes: u64,
    /// Block size in bytes.
    pub block_size: u64,
}

/// Cumulative cache activity, exported on `/metrics`.
#[derive(Default)]
pub struct CacheStats {
    /// Block reads served from memory.
    pub memory_hits: AtomicU64,
    /// Block reads served from the disk tier.
    pub disk_hits: AtomicU64,
    /// Blocks fetched from storage.
    pub fetched_blocks: AtomicU64,
    /// Bytes fetched from storage.
    pub fetched_bytes: AtomicU64,
    /// Ranged storage requests issued.
    pub fetch_requests: AtomicU64,
    /// Reads that waited for another reader's in-flight fetch instead of
    /// fetching the same block again.
    pub fetch_waits: AtomicU64,
    /// Blocks evicted from the disk tier.
    pub disk_evictions: AtomicU64,
    /// Blocks evicted from the memory tier.
    pub memory_evictions: AtomicU64,
}

/// Recency is a tick-keyed BTreeMap mirror of `entries` so the eviction
/// victim is the first key — O(log n) per eviction instead of a full-map
/// scan under the lock every read goes through. Ticks are unique (bumped
/// on every touch), so the map never collides.
#[derive(Default)]
struct DiskState {
    /// block -> (size, last access tick)
    entries: HashMap<BlockKey, (u64, u64)>,
    /// last access tick -> block (mirror of `entries`).
    by_tick: BTreeMap<u64, BlockKey>,
    total_bytes: u64,
    tick: u64,
}

struct DiskTier {
    root: PathBuf,
    max_bytes: u64,
    state: Mutex<DiskState>,
    /// Monotonic counter for unique temp-file names.
    tmp_counter: AtomicU64,
}

struct MemoryShard {
    blocks: lru::LruCache<BlockKey, OwnedBytes>,
    bytes: u64,
}

/// Node-local cache of split blocks: a memory tier in front of a disk
/// tier, each LRU-evicted by bytes. Storage fetches go through
/// [`SplitCache::claim`] so concurrent readers of one block fetch it once.
pub struct SplitCache {
    block_size: u64,
    disk: Option<DiskTier>,
    memory: Vec<Mutex<MemoryShard>>,
    memory_shard_bytes: u64,
    /// Blocks some reader is fetching right now.
    inflight: Mutex<HashSet<BlockKey>>,
    /// Signalled whenever in-flight blocks are released.
    released: Condvar,
    stats: CacheStats,
}

impl SplitCache {
    /// A cache rooted at `root` with a `max_bytes` disk budget, the default
    /// block size and no memory tier.
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> std::io::Result<Self> {
        Self::with_options(CacheOptions {
            root: Some(root.into()),
            disk_bytes: max_bytes,
            memory_bytes: 0,
            block_size: DEFAULT_BLOCK_SIZE,
        })
    }

    /// A memory-only cache — never writes to disk.
    pub fn memory_only(memory_bytes: u64, block_size: u64) -> Self {
        Self::build(None, memory_bytes, block_size)
    }

    /// A cache with explicit budgets, adopting any blocks already on disk
    /// from a previous run.
    pub fn with_options(options: CacheOptions) -> std::io::Result<Self> {
        let disk = match options.root {
            Some(root) => {
                std::fs::create_dir_all(&root)?;
                let tier = DiskTier {
                    root,
                    max_bytes: options.disk_bytes,
                    state: Mutex::new(DiskState::default()),
                    tmp_counter: AtomicU64::new(0),
                };
                tier.rebuild_from_disk()?;
                Some(tier)
            }
            None => None,
        };
        Ok(Self::build(disk, options.memory_bytes, options.block_size))
    }

    fn build(disk: Option<DiskTier>, memory_bytes: u64, block_size: u64) -> Self {
        assert!(block_size > 0, "block size must be positive");
        Self {
            block_size,
            disk,
            memory: (0..MEMORY_SHARDS)
                .map(|_| {
                    Mutex::new(MemoryShard {
                        blocks: lru::LruCache::unbounded(),
                        bytes: 0,
                    })
                })
                .collect(),
            memory_shard_bytes: memory_bytes / MEMORY_SHARDS as u64,
            inflight: Mutex::new(HashSet::new()),
            released: Condvar::new(),
            stats: CacheStats::default(),
        }
    }

    /// Size of every block but a file's last.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Cumulative activity counters.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Bytes held by the disk tier.
    pub fn total_bytes(&self) -> u64 {
        self.disk
            .as_ref()
            .map(|d| d.state.lock().unwrap().total_bytes)
            .unwrap_or(0)
    }

    /// Bytes held by the memory tier.
    pub fn memory_bytes(&self) -> u64 {
        self.memory.iter().map(|s| s.lock().unwrap().bytes).sum()
    }

    /// The disk tier's root directory, if it has one.
    pub fn root(&self) -> Option<&Path> {
        self.disk.as_ref().map(|d| d.root.as_path())
    }

    fn memory_shard(&self, key: &BlockKey) -> &Mutex<MemoryShard> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        &self.memory[hasher.finish() as usize % MEMORY_SHARDS]
    }

    /// A cached block, or None. `expected_len` is the block's length (the
    /// last block of a file is short); a disk entry of any other length is
    /// stale and dropped. Blocks read from disk are promoted to memory.
    pub fn get_block(&self, file: &FileKey, index: u64, expected_len: u64) -> Option<OwnedBytes> {
        let key = (file.clone(), index);
        if let Some(bytes) = self.memory_shard(&key).lock().unwrap().blocks.get(&key) {
            self.stats.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Some(bytes.clone());
        }
        let disk = self.disk.as_ref()?;
        if !disk.touch(&key) {
            return None;
        }
        // The read runs outside the lock; a concurrent eviction unlinking
        // the file surfaces as NotFound and is simply a miss.
        match std::fs::read(disk.root.join(block_relative_path(file, index))) {
            Ok(data) if data.len() as u64 == expected_len => {
                self.stats.disk_hits.fetch_add(1, Ordering::Relaxed);
                let bytes = OwnedBytes::new(data);
                self.insert_memory(key, bytes.clone());
                Some(bytes)
            }
            _ => {
                disk.forget(&key);
                None
            }
        }
    }

    /// Store a block fetched from storage in both tiers. A disk-tier write
    /// failure (full disk) is logged, not returned: the block is still
    /// served from memory and the read that fetched it must not fail.
    pub fn put_block(&self, file: &FileKey, index: u64, data: OwnedBytes) {
        let key = (file.clone(), index);
        if let Some(disk) = &self.disk
            && let Err(e) = disk.insert(&key, data.as_slice(), &self.stats)
        {
            tracing::warn!(block = %block_relative_path(file, index), error = %e, "split cache: disk write failed");
        }
        self.insert_memory(key, data);
    }

    fn insert_memory(&self, key: BlockKey, data: OwnedBytes) {
        let size = data.len() as u64;
        if size > self.memory_shard_bytes {
            return;
        }
        let mut shard = self.memory_shard(&key).lock().unwrap();
        if let Some(old) = shard.blocks.put(key, data) {
            shard.bytes -= old.len() as u64;
        }
        shard.bytes += size;
        while shard.bytes > self.memory_shard_bytes {
            let Some((_, evicted)) = shard.blocks.pop_lru() else { break };
            shard.bytes -= evicted.len() as u64;
            self.stats.memory_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Split `indices` (blocks of `file` a reader is missing) into those
    /// the caller must now fetch — claimed until the returned guard drops
    /// — and those another reader is already fetching (wait for them with
    /// [`SplitCache::wait_for`], then look them up again).
    pub fn claim(&self, file: &FileKey, indices: &[u64]) -> Claim<'_> {
        let mut inflight = self.inflight.lock().unwrap();
        let mut mine = Vec::new();
        let mut theirs = Vec::new();
        for &index in indices {
            if inflight.insert((file.clone(), index)) {
                mine.push(index);
            } else {
                theirs.push(index);
            }
        }
        Claim {
            cache: self,
            file: file.clone(),
            mine,
            theirs,
        }
    }

    /// Block until none of `indices` is being fetched.
    pub fn wait_for(&self, file: &FileKey, indices: &[u64]) {
        self.stats.fetch_waits.fetch_add(1, Ordering::Relaxed);
        let mut inflight = self.inflight.lock().unwrap();
        while indices
            .iter()
            .any(|&index| inflight.contains(&(file.clone(), index)))
        {
            inflight = self.released.wait(inflight).unwrap();
        }
    }

    /// Record one ranged storage fetch of `bytes` covering `blocks` blocks.
    pub fn record_fetch(&self, blocks: u64, bytes: u64) {
        self.stats.fetch_requests.fetch_add(1, Ordering::Relaxed);
        self.stats.fetched_blocks.fetch_add(blocks, Ordering::Relaxed);
        self.stats.fetched_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Blocks a reader claimed for fetching; dropping it releases them (also
/// on error or panic) and wakes readers waiting on them.
pub struct Claim<'a> {
    cache: &'a SplitCache,
    file: FileKey,
    mine: Vec<u64>,
    theirs: Vec<u64>,
}

impl Claim<'_> {
    /// Blocks this reader must fetch.
    pub fn mine(&self) -> &[u64] {
        &self.mine
    }

    /// Blocks another reader is fetching.
    pub fn theirs(&self) -> &[u64] {
        &self.theirs
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        if self.mine.is_empty() {
            return;
        }
        let mut inflight = self.cache.inflight.lock().unwrap();
        for &index in &self.mine {
            inflight.remove(&(self.file.clone(), index));
        }
        drop(inflight);
        self.cache.released.notify_all();
    }
}

impl DiskTier {
    /// Rediscover surviving blocks after a restart. Anything that is not a
    /// block — temp files from a crash, whole files cached by releases
    /// before block reads — is deleted rather than counted.
    fn rebuild_from_disk(&self) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        for split_dir in std::fs::read_dir(&self.root)? {
            let split_dir = split_dir?;
            if !split_dir.file_type()?.is_dir() {
                continue;
            }
            let split_id = split_dir.file_name().to_string_lossy().to_string();
            for entry in std::fs::read_dir(split_dir.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                let parsed = (!name.contains(".tmp-cache"))
                    .then(|| name.rsplit_once(".b"))
                    .flatten()
                    .and_then(|(file, index)| Some((file, index.parse::<u64>().ok()?)));
                let Some((file_name, index)) = parsed else {
                    let _ = std::fs::remove_file(entry.path());
                    continue;
                };
                let size = entry.metadata()?.len();
                let key = (FileKey::new(&split_id, file_name), index);
                state.tick += 1;
                let tick = state.tick;
                state.entries.insert(key.clone(), (size, tick));
                state.by_tick.insert(tick, key);
                state.total_bytes += size;
            }
        }
        Ok(())
    }

    /// Bump a block's recency; false when the tier does not hold it.
    fn touch(&self, key: &BlockKey) -> bool {
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        state.tick += 1;
        let tick = state.tick;
        let Some(entry) = state.entries.get_mut(key) else {
            return false;
        };
        state.by_tick.remove(&entry.1);
        entry.1 = tick;
        state.by_tick.insert(tick, key.clone());
        true
    }

    /// Drop a block whose file is gone or unreadable.
    fn forget(&self, key: &BlockKey) {
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        if let Some((size, tick)) = state.entries.remove(key) {
            state.total_bytes -= size;
            state.by_tick.remove(&tick);
        }
        drop(guard);
        let _ = std::fs::remove_file(self.root.join(block_relative_path(&key.0, key.1)));
    }

    /// Write a block (temp file + rename, so a reader never sees a partial
    /// block) and evict least-recently-used blocks past the budget.
    fn insert(&self, key: &BlockKey, data: &[u8], stats: &CacheStats) -> std::io::Result<()> {
        let path = self.root.join(block_relative_path(&key.0, key.1));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let uniq = self.tmp_counter.fetch_add(1, Ordering::Relaxed);
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let tmp = path.with_file_name(format!(
            "{file_name}.tmp-cache-{}-{uniq}",
            std::process::id()
        ));
        if let Err(e) = std::fs::write(&tmp, data) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        std::fs::rename(&tmp, &path)?;

        let size = data.len() as u64;
        // Evicted files are unlinked after the lock is released; victims
        // are already out of the maps, so racing inserts never
        // double-delete.
        let victims: Vec<BlockKey> = {
            let mut guard = self.state.lock().unwrap();
            let state = &mut *guard;
            state.tick += 1;
            let tick = state.tick;
            if let Some((old_size, old_tick)) = state.entries.insert(key.clone(), (size, tick)) {
                state.total_bytes -= old_size;
                state.by_tick.remove(&old_tick);
            }
            state.by_tick.insert(tick, key.clone());
            state.total_bytes += size;

            let mut victims = Vec::new();
            while state.total_bytes > self.max_bytes {
                let Some((&victim_tick, _)) = state.by_tick.iter().next() else {
                    break;
                };
                let victim = state.by_tick.remove(&victim_tick).unwrap();
                // Never evict the block just written: it has the newest
                // tick, so reaching it means nothing else is left.
                if &victim == key {
                    state.by_tick.insert(victim_tick, victim);
                    break;
                }
                let (victim_size, _) = state.entries.remove(&victim).unwrap();
                state.total_bytes -= victim_size;
                victims.push(victim);
            }
            victims
        };
        stats
            .disk_evictions
            .fetch_add(victims.len() as u64, Ordering::Relaxed);
        for (file, index) in victims {
            let _ = std::fs::remove_file(self.root.join(block_relative_path(&file, index)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(byte: u8, len: usize) -> OwnedBytes {
        OwnedBytes::new(vec![byte; len])
    }

    fn disk_cache(root: &Path, disk_bytes: u64, memory_bytes: u64) -> SplitCache {
        SplitCache::with_options(CacheOptions {
            root: Some(root.to_path_buf()),
            disk_bytes,
            memory_bytes,
            block_size: 40,
        })
        .unwrap()
    }

    #[test]
    fn put_get_and_evict_by_disk_budget() {
        let dir = tempfile::tempdir().unwrap();
        let cache = disk_cache(dir.path(), 100, 0);
        let a = FileKey::new("s1", "a");
        let c = FileKey::new("s2", "c");
        cache.put_block(&a, 0, block(1, 40));
        cache.put_block(&a, 1, block(2, 40));
        assert!(cache.get_block(&a, 0, 40).is_some());
        // Third block exceeds the budget; the LRU victim is block 1 (block
        // 0 was just read).
        cache.put_block(&c, 0, block(3, 40));
        assert!(cache.get_block(&a, 1, 40).is_none());
        assert_eq!(cache.get_block(&a, 0, 40).unwrap().as_slice(), &[1u8; 40][..]);
        assert!(cache.get_block(&c, 0, 40).is_some());
        assert!(cache.total_bytes() <= 100);
        assert_eq!(cache.stats().disk_evictions.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn memory_tier_serves_hits_and_stays_in_budget() {
        let cache = SplitCache::memory_only(MEMORY_SHARDS as u64 * 100, 40);
        let a = FileKey::new("s1", "a");
        for index in 0..64 {
            cache.put_block(&a, index, block(index as u8, 40));
        }
        assert!(cache.memory_bytes() <= MEMORY_SHARDS as u64 * 100);
        assert!(cache.stats().memory_evictions.load(Ordering::Relaxed) > 0);
        let hit = (0..64).find_map(|i| cache.get_block(&a, i, 40).map(|b| (i, b)));
        let (index, bytes) = hit.expect("some block survives");
        assert_eq!(bytes.as_slice(), &[index as u8; 40][..]);
        assert!(cache.stats().memory_hits.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn disk_hit_is_promoted_and_length_checked() {
        let dir = tempfile::tempdir().unwrap();
        let cache = disk_cache(dir.path(), 1000, MEMORY_SHARDS as u64 * 1000);
        let a = FileKey::new("s1", "a");
        cache.put_block(&a, 0, block(7, 40));
        // A fresh cache over the same root starts with an empty memory
        // tier: the first read is a disk hit, the second a memory hit.
        let cache = disk_cache(dir.path(), 1000, MEMORY_SHARDS as u64 * 1000);
        assert!(cache.get_block(&a, 0, 40).is_some());
        assert!(cache.get_block(&a, 0, 40).is_some());
        assert_eq!(cache.stats().disk_hits.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().memory_hits.load(Ordering::Relaxed), 1);
        // A block of the wrong length is stale: dropped, not served.
        let cache = disk_cache(dir.path(), 1000, 0);
        assert!(cache.get_block(&a, 0, 39).is_none());
        assert_eq!(cache.total_bytes(), 0);
    }

    #[test]
    fn rebuild_adopts_blocks_and_drops_legacy_files() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cache = disk_cache(dir.path(), 1000, 0);
            cache.put_block(&FileKey::new("s1", "seg.store"), 3, block(1, 5));
        }
        // A whole file cached by a release before block reads, and a
        // crashed temp file.
        std::fs::write(dir.path().join("s1").join("seg.fast"), b"legacy").unwrap();
        std::fs::write(dir.path().join("s1").join("seg.idx.b0.tmp-cache-1-2"), b"x").unwrap();
        let cache = disk_cache(dir.path(), 1000, 0);
        assert_eq!(cache.total_bytes(), 5);
        assert!(cache.get_block(&FileKey::new("s1", "seg.store"), 3, 5).is_some());
        assert!(!dir.path().join("s1").join("seg.fast").exists());
        assert!(!dir.path().join("s1").join("seg.idx.b0.tmp-cache-1-2").exists());
    }

    #[test]
    fn claims_hand_each_block_to_one_fetcher() {
        let cache = Arc::new(SplitCache::memory_only(MEMORY_SHARDS as u64 * 1000, 40));
        let a = FileKey::new("s1", "a");
        let first = cache.claim(&a, &[0, 1]);
        assert_eq!(first.mine(), &[0, 1]);
        let second = cache.claim(&a, &[1, 2]);
        assert_eq!(second.mine(), &[2]);
        assert_eq!(second.theirs(), &[1]);
        drop(second);

        let waiter = {
            let cache = cache.clone();
            let a = a.clone();
            std::thread::spawn(move || {
                cache.wait_for(&a, &[1]);
                cache.get_block(&a, 1, 40).is_some()
            })
        };
        cache.put_block(&a, 1, block(9, 40));
        drop(first);
        assert!(waiter.join().unwrap(), "waiter sees the block its fetcher stored");
        // Released: a new claim owns the blocks again.
        assert_eq!(cache.claim(&a, &[0, 1]).mine(), &[0, 1]);
    }
}
