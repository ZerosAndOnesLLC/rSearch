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

impl FsStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolve a key to a path under root, rejecting traversal.
    fn resolve(&self, key: &str) -> StorageResult<PathBuf> {
        if key.is_empty()
            || key.starts_with('/')
            || key.split('/').any(|part| {
                part.is_empty() || part == "." || part == ".." || part.contains('\\')
            })
        {
            return Err(StorageError::InvalidKey(key.to_string()));
        }
        Ok(self.root.join(key))
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
    async fn write_atomic(&self, key: &str, data: &[u8]) -> StorageResult<()> {
        let path = self.resolve(key)?;
        self.prepare_parent(&path, key).await?;
        let tmp = path.with_extension(format!(
            "tmp-{}",
            std::process::id() as u64 ^ path.as_os_str().len() as u64
        ));
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        file.write_all(data).await.map_err(|e| Self::io_err(key, e))?;
        file.sync_all().await.map_err(|e| Self::io_err(key, e))?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| Self::io_err(key, e))?;
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
        let tmp = path.with_extension("tmp-upload");
        tokio::fs::copy(local, &tmp)
            .await
            .map_err(|e| Self::io_err(key, e))?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| Self::io_err(key, e))?;
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
        let len = range.end.saturating_sub(range.start) as usize;
        let mut buf = vec![0u8; len];
        file.seek(SeekFrom::Start(range.start))
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
