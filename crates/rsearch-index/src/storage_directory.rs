//! Tantivy `Directory` over a split object in storage: every read is
//! served in aligned blocks through a [`SplitCache`], so a query fetches
//! only the byte ranges it touches (issue #89).

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{Directory, FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr};
use tokio::runtime::Handle;

use rsearch_storage::Storage;

use crate::cache::{FileKey, SplitCache};
use crate::split_file::{BundleMeta, FileSpan};

/// Largest single ranged storage request. A long run of missing blocks is
/// cut into requests of this size that run concurrently, so a cold read
/// is bounded by storage throughput rather than one request's latency
/// after another.
const FETCH_CHUNK_BYTES: u64 = 8 << 20;
/// Concurrent ranged requests per read.
const FETCH_CONCURRENCY: usize = 8;

pub(crate) struct DirectoryInner {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) key: String,
    pub(crate) meta: BundleMeta,
    pub(crate) cache: Arc<SplitCache>,
    pub(crate) runtime: Handle,
    /// Extra blocks fetched past a miss that continues a sequential read
    /// (0 = none). Scan readers (merge, compaction) walk whole files in
    /// order; search reads are random and use none.
    pub(crate) readahead_blocks: u64,
}

impl DirectoryInner {
    /// Read `range` of a bundled file. Must run on a thread where blocking
    /// is permitted.
    fn read(&self, handle: &LazyFileHandle, range: Range<u64>) -> std::io::Result<OwnedBytes> {
        let len = handle.span.len;
        if range.start > range.end || range.end > len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("read {range:?} past the end of {} ({len} bytes)", handle.name),
            ));
        }
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let block_size = self.cache.block_size();
        let first = range.start / block_size;
        let last = (range.end - 1) / block_size;
        let readahead = if self.readahead_blocks > 0 && handle.continues_sequential_read(first) {
            self.readahead_blocks
        } else {
            0
        };

        let mut blocks: Vec<Option<OwnedBytes>> = vec![None; (last - first + 1) as usize];
        loop {
            let mut missing = Vec::new();
            for (offset, slot) in blocks.iter_mut().enumerate() {
                if slot.is_none() {
                    let index = first + offset as u64;
                    *slot = self.cache.get_block(&handle.file, index, block_len(len, block_size, index));
                    if slot.is_none() {
                        missing.push(index);
                    }
                }
            }
            if missing.is_empty() {
                break;
            }
            let claim = self.cache.claim(&handle.file, &missing);
            if !claim.mine().is_empty() {
                for (index, data) in self.fetch(handle, claim.mine(), readahead)? {
                    if (first..=last).contains(&index) {
                        blocks[(index - first) as usize] = Some(data);
                    }
                }
            }
            let theirs = claim.theirs().to_vec();
            drop(claim);
            if !theirs.is_empty() {
                // Another reader is fetching these; once it releases them
                // they are cached (or its fetch failed and the next pass
                // claims them here).
                self.cache.wait_for(&handle.file, &theirs);
            }
        }
        handle.record_read(last);

        let blocks: Vec<OwnedBytes> = blocks.into_iter().map(Option::unwrap).collect();
        let start_in_first = (range.start - first * block_size) as usize;
        if let [only] = blocks.as_slice() {
            return Ok(only.slice(start_in_first..start_in_first + (range.end - range.start) as usize));
        }
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        for (offset, block) in blocks.iter().enumerate() {
            let block_start = (first + offset as u64) * block_size;
            let from = range.start.saturating_sub(block_start) as usize;
            let to = ((range.end - block_start) as usize).min(block.len());
            out.extend_from_slice(&block.as_slice()[from..to]);
        }
        Ok(OwnedBytes::new(out))
    }

    /// Fetch the claimed blocks `indices` (sorted) from storage — the last
    /// run extended by `readahead` blocks — store them in the cache and
    /// return them. Contiguous blocks share requests.
    fn fetch(
        &self,
        handle: &LazyFileHandle,
        indices: &[u64],
        readahead: u64,
    ) -> std::io::Result<Vec<(u64, OwnedBytes)>> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let len = handle.span.len;
        let block_size = self.cache.block_size();
        let total_blocks = len.div_ceil(block_size);
        let chunk_blocks = (FETCH_CHUNK_BYTES / block_size).max(1);

        // Contiguous runs of block indices, each cut into request-sized
        // chunks.
        let mut runs: Vec<Range<u64>> = Vec::new();
        for &index in indices {
            match runs.last_mut() {
                Some(run) if run.end == index => run.end += 1,
                _ => runs.push(index..index + 1),
            }
        }
        if let Some(run) = runs.last_mut() {
            run.end = (run.end + readahead).min(total_blocks);
        }
        let chunks: Vec<Range<u64>> = runs
            .into_iter()
            .flat_map(|run| {
                (run.start..run.end)
                    .step_by(chunk_blocks as usize)
                    .map(move |start| start..(start + chunk_blocks).min(run.end))
            })
            .collect();

        let started = Instant::now();
        let fetched: Vec<(u64, bytes::Bytes)> = self.runtime.block_on(
            stream::iter(chunks)
                .map(|chunk| async move {
                    let from = chunk.start * block_size;
                    let to = (chunk.end * block_size).min(len);
                    let object_range = handle.span.offset + from..handle.span.offset + to;
                    let data = self
                        .storage
                        .get_range(&self.key, object_range)
                        .await
                        .map_err(std::io::Error::other)?;
                    if data.len() as u64 != to - from {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!(
                                "{}: short read of {} ({} of {} bytes)",
                                self.key,
                                handle.name,
                                data.len(),
                                to - from
                            ),
                        ));
                    }
                    Ok((chunk.start, data))
                })
                .buffered(FETCH_CONCURRENCY)
                .try_collect(),
        )?;

        let mut out = Vec::new();
        let mut bytes_total = 0u64;
        for (first_index, data) in fetched {
            let mut index = first_index;
            for piece in data.chunks(block_size as usize) {
                let block = OwnedBytes::new(piece.to_vec());
                self.cache.put_block(&handle.file, index, block.clone());
                out.push((index, block));
                index += 1;
            }
            self.cache.record_fetch(index - first_index, data.len() as u64);
            bytes_total += data.len() as u64;
        }
        tracing::debug!(
            split = %self.meta.split.split_id,
            file = %handle.name,
            blocks = out.len(),
            bytes = bytes_total,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "split cache: fetched blocks from storage"
        );
        Ok(out)
    }
}

/// Length of block `index` of a `len`-byte file.
fn block_len(len: u64, block_size: u64, index: u64) -> u64 {
    (len - index * block_size).min(block_size)
}

impl std::fmt::Debug for DirectoryInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageDirectory")
            .field("key", &self.key)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StorageDirectory {
    pub(crate) inner: Arc<DirectoryInner>,
}

impl StorageDirectory {
    fn handle(&self, path: &Path) -> Result<LazyFileHandle, OpenReadError> {
        let name = path.to_string_lossy().to_string();
        let Some(&span) = self.inner.meta.files.get(&name) else {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        };
        Ok(LazyFileHandle {
            dir: self.inner.clone(),
            file: FileKey::new(&self.inner.meta.split.split_id, &name),
            name,
            span,
            last_block: AtomicU64::new(NO_READ),
        })
    }
}

impl Directory for StorageDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(self.handle(path)?))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self
            .inner
            .meta
            .files
            .contains_key(path.to_string_lossy().as_ref()))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let handle = self.handle(path)?;
        let len = handle.span.len;
        handle
            .dir
            .read(&handle, 0..len)
            .map(|bytes| bytes.as_slice().to_vec())
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(std::io::Error::other("split directories are read-only")),
            filepath: path.to_path_buf(),
        })
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            std::io::Error::other("split directories are read-only"),
            path.to_path_buf(),
        ))
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("split directories are read-only"))
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// Splits are immutable; locking is a no-op.
    fn acquire_lock(
        &self,
        _lock: &tantivy::directory::Lock,
    ) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
        Ok(tantivy::directory::DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, _callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

/// `last_block` before the first read.
const NO_READ: u64 = u64::MAX;

/// One bundled file. Holds no bytes itself: every read goes through the
/// block cache.
struct LazyFileHandle {
    dir: Arc<DirectoryInner>,
    file: FileKey,
    name: String,
    span: FileSpan,
    /// Last block of the previous read, for read-ahead detection.
    last_block: AtomicU64,
}

impl LazyFileHandle {
    /// Whether a read starting at block `first` continues the previous
    /// read (same or next block).
    fn continues_sequential_read(&self, first: u64) -> bool {
        let last = self.last_block.load(Ordering::Relaxed);
        last != NO_READ && (first == last || first == last + 1)
    }

    fn record_read(&self, last: u64) {
        self.last_block.store(last, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for LazyFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LazyFileHandle({})", self.name)
    }
}

impl tantivy::HasLen for LazyFileHandle {
    fn len(&self) -> usize {
        self.span.len as usize
    }
}

impl FileHandle for LazyFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        self.dir.read(self, range.start as u64..range.end as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use rsearch_storage::{FsStorage, StorageResult};
    use tantivy::aggregation::AggregationCollector;
    use tantivy::aggregation::agg_req::Aggregations;
    use tantivy::collector::Count;
    use tantivy::query::AllQuery;

    use super::*;
    use crate::builder::SplitBuilder;
    use crate::cache::CacheOptions;
    use crate::mapping::{IndexMapping, MappedSchema};
    use crate::reader::SplitReader;

    /// Storage that records every ranged read.
    struct CountingStorage {
        inner: FsStorage,
        ranges: Mutex<Vec<Range<u64>>>,
    }

    impl CountingStorage {
        /// Requests and bytes that fell inside the bundled files matching
        /// `suffix`.
        fn fetched_within(&self, meta: &BundleMeta, suffix: &str) -> (usize, u64) {
            let spans: Vec<Range<u64>> = meta
                .files
                .iter()
                .filter(|(name, _)| name.ends_with(suffix))
                .map(|(_, span)| span.offset..span.offset + span.len)
                .collect();
            let mut requests = 0;
            let mut bytes = 0;
            for range in self.ranges.lock().unwrap().iter() {
                for span in &spans {
                    let from = range.start.max(span.start);
                    let to = range.end.min(span.end);
                    if from < to {
                        requests += 1;
                        bytes += to - from;
                    }
                }
            }
            (requests, bytes)
        }

        fn reset(&self) {
            self.ranges.lock().unwrap().clear();
        }
    }

    #[async_trait::async_trait]
    impl Storage for CountingStorage {
        async fn put(&self, key: &str, data: bytes::Bytes) -> StorageResult<()> {
            self.inner.put(key, data).await
        }
        async fn put_file(&self, key: &str, local: &Path) -> StorageResult<()> {
            self.inner.put_file(key, local).await
        }
        async fn get(&self, key: &str) -> StorageResult<bytes::Bytes> {
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> StorageResult<bytes::Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            self.inner.get_range(key, range).await
        }
        async fn size(&self, key: &str) -> StorageResult<u64> {
            self.inner.size(key).await
        }
        async fn delete(&self, key: &str) -> StorageResult<()> {
            self.inner.delete(key).await
        }
        async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
            self.inner.list(prefix).await
        }
    }

    /// Log lines that do not compress away, so the doc store spans many
    /// blocks.
    fn noise(seed: u64, len: usize) -> String {
        let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (0..len)
            .map(|_| {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                char::from(b'a' + ((x >> 33) % 26) as u8)
            })
            .collect()
    }

    struct Fixture {
        _store_dir: tempfile::TempDir,
        storage: Arc<CountingStorage>,
        key: String,
        docs: usize,
    }

    async fn fixture(docs: usize, message_len: usize) -> Fixture {
        let store_dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let storage = Arc::new(CountingStorage {
            inner: FsStorage::new(store_dir.path()),
            ranges: Mutex::new(Vec::new()),
        });
        let schema = MappedSchema::build(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {"namespace": {"type": "keyword"}}
            }))
            .unwrap(),
        );
        let mut builder = SplitBuilder::new("logs", schema, scratch.path(), 50 << 20).unwrap();
        for i in 0..docs {
            builder
                .add_json(
                    serde_json::json!({
                        "@timestamp": 1_753_300_000_000_i64 + i as i64,
                        "namespace": format!("ns-{}", i % 3),
                        "message": noise(i as u64, message_len),
                    }),
                    tantivy::DateTime::from_timestamp_millis(0),
                )
                .unwrap();
        }
        let packaged = builder.finish().unwrap();
        let key = format!("splits/{}.split", packaged.meta.split_id);
        storage.put_file(&key, &packaged.file_path).await.unwrap();
        Fixture {
            _store_dir: store_dir,
            storage,
            key,
            docs,
        }
    }

    fn cache(dir: &Path, block_size: u64, memory_bytes: u64) -> Arc<SplitCache> {
        Arc::new(
            SplitCache::with_options(CacheOptions {
                root: Some(dir.to_path_buf()),
                disk_bytes: 1 << 30,
                memory_bytes,
                block_size,
            })
            .unwrap(),
        )
    }

    /// Issue #89: a `size: 0` terms aggregation reads the fast fields it
    /// aggregates and the footers of the rest — never the doc store.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aggregation_does_not_download_the_doc_store() {
        let f = fixture(4_000, 1_000).await;
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = cache(cache_dir.path(), crate::cache::DEFAULT_BLOCK_SIZE, 64 << 20);
        let reader = SplitReader::open(f.storage.clone(), &f.key, cache.clone())
            .await
            .unwrap();
        let meta = reader.meta.clone();
        let store_len: u64 = meta
            .files
            .iter()
            .filter(|(name, _)| name.ends_with(".store"))
            .map(|(_, span)| span.len)
            .sum();
        assert!(store_len > 8 * crate::cache::DEFAULT_BLOCK_SIZE, "doc store too small: {store_len}");

        let (count, buckets) = tokio::task::spawn_blocking(move || {
            let aggs: Aggregations = serde_json::from_value(serde_json::json!({
                "ns": {"terms": {"field": "namespace", "size": 10}}
            }))
            .unwrap();
            let collector = AggregationCollector::from_aggs(aggs, Default::default());
            let searcher = reader.searcher().unwrap();
            let (count, result) = searcher.search(&AllQuery, &(Count, collector)).unwrap();
            (count, serde_json::to_value(result).unwrap())
        })
        .await
        .unwrap();
        assert_eq!(count, f.docs);
        let buckets = buckets["ns"]["buckets"].as_array().unwrap().clone();
        assert_eq!(buckets.len(), 3);

        let (_, store_fetched) = f.storage.fetched_within(&meta, ".store");
        assert!(
            store_fetched < store_len / 4,
            "fetched {store_fetched} of a {store_len}-byte doc store"
        );
        assert!(cache.stats().fetched_bytes.load(Ordering::Relaxed) > 0);
    }

    /// Every byte of every bundled file reads back identically through
    /// small blocks — whole files and ranges straddling block edges.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_reads_match_the_object() {
        let f = fixture(300, 200).await;
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = cache(cache_dir.path(), 64, 1 << 20);
        let reader = SplitReader::open(f.storage.clone(), &f.key, cache.clone())
            .await
            .unwrap();
        let object = f.storage.get(&f.key).await.unwrap();
        let docs = f.docs;
        // The raw directory: Tantivy's managed wrapper strips footers.
        let directory = StorageDirectory {
            inner: Arc::new(DirectoryInner {
                storage: f.storage.clone(),
                key: f.key.clone(),
                meta: reader.meta.clone(),
                cache,
                runtime: Handle::current(),
                readahead_blocks: 0,
            }),
        };
        tokio::task::spawn_blocking(move || {
            for (name, span) in &reader.meta.files {
                let expected =
                    &object[span.offset as usize..(span.offset + span.len) as usize];
                assert_eq!(directory.atomic_read(Path::new(name)).unwrap(), expected, "{name}");
                let slice = directory.open_read(Path::new(name)).unwrap();
                assert_eq!(slice.read_bytes().unwrap().as_slice(), expected, "{name}");
                let len = span.len as usize;
                for (from, to) in [(0, len.min(1)), (63, len.min(65)), (len / 3, len - len / 5)] {
                    if from <= to {
                        let got = slice.read_bytes_slice(from..to).unwrap();
                        assert_eq!(got.as_slice(), &expected[from..to], "{name} {from}..{to}");
                    }
                }
            }
            let mut seen = 0;
            reader
                .for_each_doc(
                    |_, _| false,
                    |doc| {
                        assert!(doc.json["message"].as_str().unwrap().len() == 200);
                        seen += 1;
                        Ok(())
                    },
                )
                .unwrap();
            assert_eq!(seen, docs);
        })
        .await
        .unwrap();
    }

    /// Concurrent readers of the same cold blocks share one fetch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_readers_fetch_a_block_once() {
        let f = fixture(2_000, 500).await;
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = cache(cache_dir.path(), crate::cache::DEFAULT_BLOCK_SIZE, 64 << 20);
        let reader = Arc::new(
            SplitReader::open(f.storage.clone(), &f.key, cache.clone())
                .await
                .unwrap(),
        );
        let meta = reader.meta.clone();
        let (store_name, store_span) = meta
            .files
            .iter()
            .find(|(name, _)| name.ends_with(".store"))
            .map(|(n, s)| (n.clone(), *s))
            .unwrap();
        // The head of the doc store: not touched by opening the split.
        let range = 0..(store_span.len as usize).min(100_000);
        f.storage.reset();

        let barrier = Arc::new(std::sync::Barrier::new(4));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let reader = reader.clone();
                let barrier = barrier.clone();
                let name = store_name.clone();
                let range = range.clone();
                tokio::task::spawn_blocking(move || {
                    let slice = reader.index().directory().open_read(Path::new(&name)).unwrap();
                    barrier.wait();
                    slice.read_bytes_slice(range).unwrap().len()
                })
            })
            .collect();
        for thread in threads {
            assert_eq!(thread.await.unwrap(), range.len());
        }
        let (requests, _) = f.storage.fetched_within(&meta, ".store");
        assert_eq!(requests, 1, "one fetch for four concurrent readers");
    }

    /// A scan reader walks the doc store in read-ahead windows, not one
    /// request per block, and never writes to disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_reader_reads_ahead() {
        let f = fixture(6_000, 1_000).await;
        let reader = Arc::new(SplitReader::open_for_scan(f.storage.clone(), &f.key).await.unwrap());
        let meta = reader.meta.clone();
        let store_blocks: u64 = meta
            .files
            .iter()
            .filter(|(name, _)| name.ends_with(".store"))
            .map(|(_, span)| span.len.div_ceil(crate::cache::DEFAULT_BLOCK_SIZE))
            .sum();
        assert!(store_blocks >= 16, "doc store spans {store_blocks} blocks");
        f.storage.reset();

        let r = reader.clone();
        let seen = tokio::task::spawn_blocking(move || {
            let mut seen = 0;
            r.for_each_doc(|_, _| false, |_| {
                seen += 1;
                Ok(())
            })
            .unwrap();
            seen
        })
        .await
        .unwrap();
        assert_eq!(seen, f.docs);
        let (requests, _) = f.storage.fetched_within(&meta, ".store");
        assert!(
            (requests as u64) < store_blocks / 4,
            "{requests} requests for {store_blocks} doc-store blocks"
        );
    }
}
