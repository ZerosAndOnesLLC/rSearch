use std::io::SeekFrom;
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{StorageError, StorageResult};
use crate::storage::Storage;

/// Local filesystem backend. Objects are plain files under a root
/// directory; used for dev, tests, and air-gapped single-node installs.
pub struct FsStorage {
    root: PathBuf,
}

/// Process-unique counter for temp-file names: two concurrent writers of
/// the same key must never share a temp path, or their writes interleave
/// and the rename can publish a corrupt object.
static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl FsStorage {
    /// Create a backend rooted at `root`; spawns a background sweep of
    /// temp files stranded by earlier crashes.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root: PathBuf = root.into();
        // Failed writes delete their temp on the error path, but a crash
        // can still strand one, and `list` deliberately hides temp names —
        // sweep leftovers in the background so they don't hold disk
        // forever. A file is a leftover only if no writer is running,
        // which holds at construction time (one FsStorage per process,
        // built before serving).
        let sweep_root = root.clone();
        std::thread::spawn(move || sweep_temp_files(&sweep_root));
        Self { root }
    }

    /// Resolve a key to a path under root, rejecting traversal. Keys are
    /// lexically confined to root (no `..`, absolute, or `.` segments); if
    /// the target already exists, its canonical path is additionally
    /// verified to stay under root, defeating a symlink placed inside the
    /// data dir that points outside it.
    fn resolve(&self, key: &str) -> StorageResult<PathBuf> {
        if key.is_empty()
            || key.starts_with('/')
            || key.split('/').any(|part| {
                part.is_empty() || part == "." || part == ".." || part.contains('\\')
            })
        {
            return Err(StorageError::InvalidKey(key.to_string()));
        }
        let path = self.root.join(key);
        // Best-effort symlink-escape guard on existing targets.
        if let (Ok(canon), Ok(root_canon)) = (path.canonicalize(), self.root.canonicalize())
            && !canon.starts_with(&root_canon)
        {
            return Err(StorageError::InvalidKey(format!(
                "{key} resolves outside the storage root"
            )));
        }
        Ok(path)
    }

    fn io_err(key: &str, source: std::io::Error) -> StorageError {
        if source.kind() == std::io::ErrorKind::NotFound {
            StorageError::NotFound(key.to_string())
        } else {
            StorageError::Io {
                key: key.to_string(),
                source,
            }
        }
    }

    async fn prepare_parent(&self, path: &Path, key: &str) -> StorageResult<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Self::io_err(key, e))?;
        }
        Ok(())
    }

    /// Atomic write: write to a temp sibling then rename over the key.
    /// The temp name is process-unique (concurrent writers of one key can
    /// never interleave into a shared temp) and removed on any failure.
    async fn write_atomic(&self, key: &str, data: &[u8]) -> StorageResult<()> {
        let path = self.resolve(key)?;
        self.prepare_parent(&path, key).await?;
        let tmp = self.unique_tmp(&path);
        let result = async {
            let mut file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| Self::io_err(key, e))?;
            file.write_all(data).await.map_err(|e| Self::io_err(key, e))?;
            file.sync_all().await.map_err(|e| Self::io_err(key, e))?;
            tokio::fs::rename(&tmp, &path)
                .await
                .map_err(|e| Self::io_err(key, e))
        }
        .await;
        if let Err(e) = result {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
        self.sync_parent(&path, key).await?;
        Ok(())
    }

    fn unique_tmp(&self, path: &Path) -> PathBuf {
        let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        path.with_extension(format!("tmp-{}-{seq}", std::process::id()))
    }
}

/// Recursively delete leftover temp files (`*.tmp-*`) under `dir`. Failed
/// writes remove their temp on the error path, but a crash strands them,
/// and `list` deliberately hides temp names — without this sweep they
/// hold disk forever. Only temps past a generous age are removed: the
/// sweep runs on a background thread concurrently with live writers, and
/// an in-flight write's temp is always fresh. Best-effort: errors ignored.
fn sweep_temp_files(dir: &Path) {
    const MIN_LEFTOVER_AGE: std::time::Duration = std::time::Duration::from_secs(3600);
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            sweep_temp_files(&path);
        } else if entry.file_name().to_string_lossy().contains(".tmp-")
            && entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .map(|age| age > MIN_LEFTOVER_AGE)
                .unwrap_or(false)
        {
            let _ = std::fs::remove_file(&path);
        }
    }
}

impl FsStorage {

    /// Atomic streamed write for peer transfers: chunks land in a temp
    /// sibling, fsync, rename, parent fsync — same durability discipline
    /// as `put`/`put_file`. Returns the byte count written.
    ///
    /// The temp name carries a process-unique counter so concurrent
    /// transfers of the same key (origin push racing a repair pull) can
    /// never interleave writes into one file, and the temp is removed on
    /// any failure so aborted transfers don't accumulate on disk.
    pub async fn put_stream<S, E>(&self, key: &str, stream: S) -> StorageResult<u64>
    where
        S: futures::Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        static RECV_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = self.resolve(key)?;
        self.prepare_parent(&path, key).await?;
        let seq = RECV_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = path.with_extension(format!("tmp-recv-{}-{seq}", std::process::id()));
        let result = self.write_stream_to(&tmp, key, stream).await;
        match result {
            Ok(written) => {
                tokio::fs::rename(&tmp, &path)
                    .await
                    .map_err(|e| Self::io_err(key, e))?;
                self.sync_parent(&path, key).await?;
                Ok(written)
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                Err(e)
            }
        }
    }

    async fn write_stream_to<S, E>(
        &self,
        tmp: &Path,
        key: &str,
        mut stream: S,
    ) -> StorageResult<u64>
    where
        S: futures::Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        use futures::StreamExt;
        let mut file = tokio::fs::File::create(tmp)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        let mut written: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| StorageError::Backend {
                key: key.to_string(),
                message: format!("transfer stream failed: {e}"),
            })?;
            file.write_all(&chunk).await.map_err(|e| Self::io_err(key, e))?;
            written += chunk.len() as u64;
        }
        file.sync_all().await.map_err(|e| Self::io_err(key, e))?;
        Ok(written)
    }

    /// Absolute path of an object in the root — for in-crate callers that
    /// need a durable source path (peer pushes outlive the caller's
    /// staging file).
    pub(crate) fn object_path(&self, key: &str) -> StorageResult<PathBuf> {
        self.resolve(key)
    }

    /// Open an object for streamed reading (peer GET). Returns the file
    /// handle and its length.
    pub async fn open_read(&self, key: &str) -> StorageResult<(tokio::fs::File, u64)> {
        let path = self.resolve(key)?;
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        let len = file
            .metadata()
            .await
            .map_err(|e| Self::io_err(key, e))?
            .len();
        Ok((file, len))
    }

    /// fsync the directory holding `path` so a rename survives power loss.
    async fn sync_parent(&self, path: &Path, key: &str) -> StorageResult<()> {
        if let Some(parent) = path.parent() {
            // Directory fsync is best-effort across platforms; ignore
            // ENOTSUP/EINVAL but surface real IO errors.
            if let Ok(dir) = tokio::fs::File::open(parent).await {
                match dir.sync_all().await {
                    Ok(()) => {}
                    // Some filesystems reject directory fsync (EINVAL 22).
                    Err(e) if e.raw_os_error() == Some(22) => {}
                    Err(e) => return Err(Self::io_err(key, e)),
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for FsStorage {
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()> {
        self.write_atomic(key, &data).await
    }

    async fn put_file(&self, key: &str, local: &Path) -> StorageResult<()> {
        let path = self.resolve(key)?;
        self.prepare_parent(&path, key).await?;
        // Process-unique temp (see write_atomic) — a fresh name also means
        // the hard link below never collides with a leftover.
        let tmp = self.unique_tmp(&path);
        let result = async {
            // Publish via hard link when the staging dir and the storage
            // root share a filesystem (both usually live under data_dir) —
            // a copy would rewrite every split byte a second time (2x
            // publish write amplification). Cross-device or unsupported
            // links fall back to the copy.
            if tokio::fs::hard_link(local, &tmp).await.is_err() {
                tokio::fs::copy(local, &tmp)
                    .await
                    .map_err(|e| Self::io_err(key, e))?;
            }
            // Durability: fsync the data before it becomes visible, so the
            // ingest WAL is only truncated after the split is truly on disk.
            {
                let file = tokio::fs::File::open(&tmp)
                    .await
                    .map_err(|e| Self::io_err(key, e))?;
                file.sync_all().await.map_err(|e| Self::io_err(key, e))?;
            }
            tokio::fs::rename(&tmp, &path)
                .await
                .map_err(|e| Self::io_err(key, e))
        }
        .await;
        if let Err(e) = result {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
        self.sync_parent(&path, key).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        let path = self.resolve(key)?;
        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        Ok(Bytes::from(data))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> StorageResult<Bytes> {
        let path = self.resolve(key)?;
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        // Clamp to the file before allocating, so an oversized range (the
        // internal API forwards attacker-controlled Range headers) cannot
        // demand an arbitrary-size buffer.
        let size = file
            .metadata()
            .await
            .map_err(|e| Self::io_err(key, e))?
            .len();
        let start = range.start.min(size);
        let len = range.end.min(size).saturating_sub(start) as usize;
        let mut buf = vec![0u8; len];
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|e| Self::io_err(key, e))?;
        file.read_exact(&mut buf)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        Ok(Bytes::from(buf))
    }

    async fn size(&self, key: &str) -> StorageResult<u64> {
        let path = self.resolve(key)?;
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        Ok(meta.len())
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        let path = self.resolve(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Self::io_err(key, e)),
        }
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        // Walk from the deepest existing directory implied by the prefix.
        let (dir, _) = match prefix.rsplit_once('/') {
            Some((dir, rest)) => (self.root.join(dir), rest),
            None => (self.root.clone(), prefix),
        };
        let mut keys = Vec::new();
        let mut stack = vec![dir];
        while let Some(current) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&current).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(Self::io_err(prefix, e)),
            };
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| Self::io_err(prefix, e))?
            {
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .await
                    .map_err(|e| Self::io_err(prefix, e))?;
                if file_type.is_dir() {
                    stack.push(path);
                } else if let Ok(rel) = path.strip_prefix(&self.root) {
                    let key = rel.to_string_lossy().replace('\\', "/");
                    if key.starts_with(prefix) && !key.contains(".tmp-") {
                        keys.push(key);
                    }
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage() -> (tempfile::TempDir, FsStorage) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        (dir, storage)
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let (_dir, s) = storage();
        s.put("a/b/obj.bin", Bytes::from_static(b"hello world"))
            .await
            .unwrap();
        assert_eq!(s.get("a/b/obj.bin").await.unwrap().as_ref(), b"hello world");
        assert_eq!(s.size("a/b/obj.bin").await.unwrap(), 11);
        assert!(s.exists("a/b/obj.bin").await.unwrap());
    }

    #[tokio::test]
    async fn get_range_reads_slice() {
        let (_dir, s) = storage();
        s.put("obj", Bytes::from_static(b"0123456789")).await.unwrap();
        assert_eq!(s.get_range("obj", 2..5).await.unwrap().as_ref(), b"234");
    }

    #[tokio::test]
    async fn missing_object_is_not_found() {
        let (_dir, s) = storage();
        assert!(matches!(
            s.get("nope").await,
            Err(StorageError::NotFound(_))
        ));
        assert!(!s.exists("nope").await.unwrap());
        // Deleting a missing object is fine.
        s.delete("nope").await.unwrap();
    }

    #[tokio::test]
    async fn list_filters_by_prefix() {
        let (_dir, s) = storage();
        for key in ["x/1", "x/2", "x/sub/3", "y/1"] {
            s.put(key, Bytes::from_static(b"d")).await.unwrap();
        }
        assert_eq!(s.list("x/").await.unwrap(), vec!["x/1", "x/2", "x/sub/3"]);
        assert_eq!(s.list("").await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn rejects_traversal_keys() {
        let (_dir, s) = storage();
        for bad in ["../evil", "/abs", "a//b", "a/./b", "a/../b", ""] {
            assert!(
                matches!(s.get(bad).await, Err(StorageError::InvalidKey(_))),
                "key {bad:?} should be rejected"
            );
        }
    }

    #[tokio::test]
    async fn put_file_uploads_local_file() {
        let (_dir, s) = storage();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"split-bytes").unwrap();
        s.put_file("splits/s1.split", src.path()).await.unwrap();
        assert_eq!(
            s.get("splits/s1.split").await.unwrap().as_ref(),
            b"split-bytes"
        );
    }

    #[tokio::test]
    async fn put_overwrites_existing() {
        let (_dir, s) = storage();
        s.put("k", Bytes::from_static(b"one")).await.unwrap();
        s.put("k", Bytes::from_static(b"two")).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().as_ref(), b"two");
    }
}
