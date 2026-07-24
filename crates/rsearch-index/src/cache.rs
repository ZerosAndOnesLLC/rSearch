use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Node-local disk cache for split fragments (one entry per bundled
/// internal file). LRU-evicted by total bytes when over budget.
pub struct SplitCache {
    root: PathBuf,
    max_bytes: u64,
    state: Mutex<CacheState>,
    /// Monotonic counter for unique temp-file names.
    tmp_counter: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct CacheState {
    /// key -> (size, last access tick)
    entries: HashMap<String, (u64, u64)>,
    total_bytes: u64,
    tick: u64,
}

impl SplitCache {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let cache = Self {
            root,
            max_bytes,
            state: Mutex::new(CacheState::default()),
            tmp_counter: std::sync::atomic::AtomicU64::new(0),
        };
        cache.rebuild_from_disk()?;
        Ok(cache)
    }

    /// Rediscover surviving entries after a restart.
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
                let name = entry.file_name().to_string_lossy().to_string();
                // Leftover temp files from a crash are not real entries —
                // delete them rather than counting them (L5).
                if name.contains(".tmp-cache") {
                    let _ = std::fs::remove_file(entry.path());
                    continue;
                }
                if entry.file_type()?.is_file() {
                    let size = entry.metadata()?.len();
                    let key = format!("{split_id}/{name}");
                    state.entries.insert(key, (size, 0));
                    state.total_bytes += size;
                }
            }
        }
        Ok(())
    }

    fn path_for(&self, split_id: &str, file_name: &str) -> PathBuf {
        self.root.join(split_id).join(file_name)
    }

    /// Return the cached path if present, bumping its recency.
    pub fn get(&self, split_id: &str, file_name: &str) -> Option<PathBuf> {
        let key = format!("{split_id}/{file_name}");
        let mut state = self.state.lock().unwrap();
        state.tick += 1;
        let tick = state.tick;
        if let Some(entry) = state.entries.get_mut(&key) {
            entry.1 = tick;
            let path = self.path_for(split_id, file_name);
            if path.exists() {
                return Some(path);
            }
            // Disk and index disagree (external cleanup); drop the entry.
            let (size, _) = state.entries.remove(&key).unwrap();
            state.total_bytes -= size;
        }
        None
    }

    /// Insert file contents, evicting least-recently-used entries as needed.
    pub fn insert(
        &self,
        split_id: &str,
        file_name: &str,
        data: &[u8],
    ) -> std::io::Result<PathBuf> {
        let path = self.path_for(split_id, file_name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Unique temp name per file: bundled files share a segment-UUID
        // stem with different extensions, so a `with_extension` temp would
        // collide across concurrent fetches and persist one file's bytes
        // under another's name (H6). Include the full file name + a
        // per-call counter.
        let uniq = self.tmp_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = path.with_file_name(format!(
            "{file_name}.tmp-cache-{}-{uniq}",
            std::process::id()
        ));
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &path)?;

        let key = format!("{split_id}/{file_name}");
        let mut state = self.state.lock().unwrap();
        state.tick += 1;
        let tick = state.tick;
        if let Some((old_size, _)) = state.entries.insert(key.clone(), (data.len() as u64, tick)) {
            state.total_bytes -= old_size;
        }
        state.total_bytes += data.len() as u64;

        while state.total_bytes > self.max_bytes {
            let Some((victim, size)) = state
                .entries
                .iter()
                // Never evict the entry we just inserted, even if it alone
                // exceeds the budget — returning a path to a deleted file
                // would make the split unsearchable (M4).
                .filter(|(k, _)| *k != &key)
                .min_by_key(|(_, v)| v.1)
                .map(|(k, v)| (k.clone(), v.0))
            else {
                break;
            };
            state.entries.remove(&victim);
            state.total_bytes -= size;
            let _ = std::fs::remove_file(self.root.join(&victim));
        }
        Ok(path)
    }

    pub fn total_bytes(&self) -> u64 {
        self.state.lock().unwrap().total_bytes
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_and_evict() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SplitCache::new(dir.path(), 100).unwrap();
        cache.insert("s1", "a", &[0u8; 40]).unwrap();
        cache.insert("s1", "b", &[0u8; 40]).unwrap();
        assert!(cache.get("s1", "a").is_some());
        // Third insert exceeds budget; LRU victim is "b" (a was just touched).
        cache.insert("s2", "c", &[0u8; 40]).unwrap();
        assert!(cache.get("s1", "b").is_none());
        assert!(cache.get("s1", "a").is_some());
        assert!(cache.get("s2", "c").is_some());
        assert!(cache.total_bytes() <= 100);
    }

    #[test]
    fn rebuilds_index_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cache = SplitCache::new(dir.path(), 1000).unwrap();
            cache.insert("s1", "a", b"hello").unwrap();
        }
        let cache = SplitCache::new(dir.path(), 1000).unwrap();
        assert_eq!(cache.total_bytes(), 5);
        assert!(cache.get("s1", "a").is_some());
    }
}
