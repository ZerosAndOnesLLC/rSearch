use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use tantivy::Index;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tokio::runtime::Handle;

use rsearch_storage::Storage;

use crate::cache::SplitCache;
use crate::error::{IndexError, IndexResult};
use crate::mapping::{ID_FIELD, IndexMapping, MappedSchema, SEQ_FIELD, SOURCE_FIELD, TIMESTAMP_FIELD};
use crate::split_file::{BundleMeta, FOOTER_TAIL_LEN, parse_footer_tail, parse_meta};

/// One document read back out of a split (merge / compaction re-index).
#[derive(Debug)]
pub struct ReadDoc {
    /// Parsed `_source`.
    pub json: serde_json::Value,
    /// The document's `_timestamp`, epoch millis.
    pub timestamp_millis: i64,
    /// Stored `_id`; None in legacy splits (no id field).
    pub id: Option<String>,
    /// `_seq` write stamp; 0 in legacy splits.
    pub seq: i64,
}

/// An opened split: footer metadata plus a lazily-fetching Tantivy index.
/// Opening reads only the footer; internal files are range-read from
/// storage on first use and cached on local disk.
pub struct SplitReader {
    /// Footer metadata: bundled file map plus split metadata.
    pub meta: BundleMeta,
    index: Index,
    /// Built once and reused: splits are immutable, so re-opening every
    /// segment's readers per query is pure waste.
    reader: tantivy::IndexReader,
    /// The mapped schema this split was written with (its own mapping and
    /// layout version), so queries resolve fields by the split's ordinals.
    schema: Arc<MappedSchema>,
    /// Tombstone exclusions applied so far (see `apply_tombstones`).
    pub(crate) exclusions: std::sync::Mutex<Arc<crate::exclusions::ExclusionSet>>,
    /// String-valued `_dynamic` paths, computed on first bare
    /// `query_string` and reused: splits are immutable, so the term
    /// dictionary never changes under us.
    dynamic_paths: OnceLock<Vec<String>>,
}

impl SplitReader {
    /// Open a split by storage key. Async (footer reads); the returned
    /// index performs storage reads lazily on blocking threads — run
    /// searches inside `spawn_blocking`.
    pub async fn open(
        storage: Arc<dyn Storage>,
        key: &str,
        cache: Arc<SplitCache>,
    ) -> IndexResult<Self> {
        let size = storage
            .size(key)
            .await
            .map_err(|e| IndexError::InvalidDocument(format!("stat split {key}: {e}")))?;
        if size < FOOTER_TAIL_LEN {
            return Err(IndexError::InvalidDocument(format!(
                "split {key} too small ({size} bytes)"
            )));
        }
        let tail = storage
            .get_range(key, size - FOOTER_TAIL_LEN..size)
            .await
            .map_err(|e| IndexError::InvalidDocument(format!("read split tail {key}: {e}")))?;
        let meta_len = parse_footer_tail(&tail)?;
        // Guard against a corrupt/hostile footer whose declared length
        // would underflow the offset math (L1).
        if meta_len > size - FOOTER_TAIL_LEN {
            return Err(IndexError::InvalidDocument(format!(
                "split {key} footer length {meta_len} exceeds object size {size}"
            )));
        }
        let meta_start = size - FOOTER_TAIL_LEN - meta_len;
        let meta_bytes = storage
            .get_range(key, meta_start..meta_start + meta_len)
            .await
            .map_err(|e| IndexError::InvalidDocument(format!("read split meta {key}: {e}")))?;
        let meta = parse_meta(&meta_bytes)?;

        let directory = StorageDirectory {
            inner: Arc::new(DirectoryInner {
                storage,
                key: key.to_string(),
                meta: meta.clone(),
                cache,
                runtime: Handle::current(),
            }),
        };
        // Index::open reads bundled files, which bridges back into the
        // async runtime — so it must run on a blocking thread. Build the
        // reader once here (also blocking: it opens every segment).
        let (index, reader) = tokio::task::spawn_blocking(move || {
            let index = Index::open(directory)?;
            let reader = index
                .reader_builder()
                .reload_policy(tantivy::ReloadPolicy::Manual)
                .try_into()?;
            Ok::<_, tantivy::TantivyError>((index, reader))
        })
        .await
        .map_err(|e| IndexError::InvalidDocument(format!("open task failed: {e}")))??;
        let mapping = IndexMapping::from_json(&meta.split.mapping).unwrap_or_default();
        let schema = Arc::new(MappedSchema::build_versioned(mapping, meta.split.schema_version));
        let segment_ids: Vec<tantivy::index::SegmentId> = reader
            .searcher()
            .segment_readers()
            .iter()
            .map(|s| s.segment_id())
            .collect();
        Ok(Self {
            meta,
            index,
            reader,
            schema,
            exclusions: std::sync::Mutex::new(Arc::new(
                crate::exclusions::ExclusionSet::empty(segment_ids),
            )),
            dynamic_paths: OnceLock::new(),
        })
    }

    /// The underlying lazily-fetching Tantivy index.
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// The schema this split was built with (own mapping + layout version).
    pub fn mapped_schema(&self) -> &Arc<MappedSchema> {
        &self.schema
    }


    /// A searcher over this immutable split, reusing the reader built at
    /// open time (segment readers are pooled, not re-opened). Call from a
    /// blocking context.
    pub fn searcher(&self) -> IndexResult<tantivy::Searcher> {
        Ok(self.reader.searcher())
    }

    /// The string-valued JSON paths present in this split's `_dynamic`
    /// field, for fanning a bare `query_string` across unmapped fields.
    /// Computed once per split (skip-scan of the term dictionary) and
    /// cached. Call from a blocking context.
    pub fn dynamic_string_paths(&self) -> IndexResult<&[String]> {
        if let Some(paths) = self.dynamic_paths.get() {
            return Ok(paths);
        }
        let paths = crate::dynamic_paths::dynamic_string_paths(
            &self.reader.searcher(),
            self.schema.dynamic,
        )?;
        Ok(self.dynamic_paths.get_or_init(|| paths))
    }

    /// Visit every document — used by the merge/compaction jobs to
    /// re-index splits. `skip(segment_ord, doc_id)` lets the caller drop
    /// documents (tombstoned ones) without reading their source. Call from
    /// a blocking context. Streams the doc store one document at a time so
    /// re-indexing a whole merge group holds O(one doc) in memory, not the
    /// entire group's parsed corpus.
    pub fn for_each_doc(
        &self,
        mut skip: impl FnMut(u32, tantivy::DocId) -> bool,
        mut visit: impl FnMut(ReadDoc) -> IndexResult<()>,
    ) -> IndexResult<()> {
        let searcher = self.searcher()?;
        let schema = self.index.schema();
        let source_field = schema
            .get_field(SOURCE_FIELD)
            .map_err(|_| IndexError::InvalidDocument("split lacks _source".into()))?;
        let id_field = schema.get_field(ID_FIELD).ok();
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            let store = segment_reader.get_store_reader(10)?;
            let ts_column = segment_reader.fast_fields().date(TIMESTAMP_FIELD)?;
            let seq_column = match id_field {
                Some(_) => Some(segment_reader.fast_fields().i64(SEQ_FIELD)?),
                None => None,
            };
            for doc_id in segment_reader.doc_ids_alive() {
                if skip(segment_ord as u32, doc_id) {
                    continue;
                }
                let doc: tantivy::TantivyDocument = store.get(doc_id)?;
                let source = doc
                    .get_first(source_field)
                    .and_then(|v| tantivy::schema::Value::as_str(&v))
                    .ok_or_else(|| {
                        IndexError::InvalidDocument("document missing _source".into())
                    })?;
                let json: serde_json::Value = serde_json::from_str(source).map_err(|e| {
                    IndexError::InvalidDocument(format!("corrupt _source: {e}"))
                })?;
                let timestamp_millis = ts_column
                    .first(doc_id)
                    .map(|dt| dt.into_timestamp_millis())
                    .unwrap_or_default();
                let id = id_field.and_then(|f| {
                    doc.get_first(f)
                        .and_then(|v| tantivy::schema::Value::as_str(&v))
                        .map(str::to_string)
                });
                let seq = seq_column
                    .as_ref()
                    .and_then(|c| c.first(doc_id))
                    .unwrap_or(0);
                visit(ReadDoc {
                    json,
                    timestamp_millis,
                    id,
                    seq,
                })?;
            }
        }
        Ok(())
    }
}

struct DirectoryInner {
    storage: Arc<dyn Storage>,
    key: String,
    meta: BundleMeta,
    cache: Arc<SplitCache>,
    runtime: Handle,
}

/// Ranged-read chunk size for cold split fetches. Large bundled files
/// (a merged split's doc store can be 100MB+) stream into the cache file
/// chunk by chunk, so transient memory stays O(chunk) per concurrent
/// fetch instead of O(file).
const FETCH_CHUNK_BYTES: u64 = 8 << 20;

impl DirectoryInner {
    /// Fetch an internal file (whole-file granularity) through the cache.
    /// Must run on a thread where blocking is permitted.
    fn fetch(&self, file_name: &str) -> std::io::Result<PathBuf> {
        if let Some(path) = self.cache.get(&self.meta.split.split_id, file_name) {
            return Ok(path);
        }
        let span = self.meta.files.get(file_name).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("{file_name} not in split bundle"),
            )
        })?;
        self.cache
            .insert_via(&self.meta.split.split_id, file_name, |file| {
                use std::io::Write;
                let mut offset = span.offset;
                let end = span.offset + span.len;
                while offset < end {
                    let chunk_end = (offset + FETCH_CHUNK_BYTES).min(end);
                    let data = self
                        .runtime
                        .block_on(self.storage.get_range(&self.key, offset..chunk_end))
                        .map_err(std::io::Error::other)?;
                    file.write_all(&data)?;
                    offset = chunk_end;
                }
                Ok(())
            })
    }
}

impl std::fmt::Debug for DirectoryInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageDirectory")
            .field("key", &self.key)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct StorageDirectory {
    inner: Arc<DirectoryInner>,
}

impl Directory for StorageDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let name = path.to_string_lossy().to_string();
        if !self.inner.meta.files.contains_key(&name) {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }
        Ok(Arc::new(LazyFileHandle {
            dir: self.inner.clone(),
            name,
            bytes: OnceLock::new(),
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self
            .inner
            .meta
            .files
            .contains_key(path.to_string_lossy().as_ref()))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let name = path.to_string_lossy().to_string();
        if !self.inner.meta.files.contains_key(&name) {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }
        let cached = self
            .inner
            .fetch(&name)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?;
        std::fs::read(cached).map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))
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

struct LazyFileHandle {
    dir: Arc<DirectoryInner>,
    name: String,
    bytes: OnceLock<OwnedBytes>,
}

impl LazyFileHandle {
    fn bytes(&self) -> std::io::Result<&OwnedBytes> {
        if let Some(bytes) = self.bytes.get() {
            return Ok(bytes);
        }
        let path = self.dir.fetch(&self.name)?;
        let file = std::fs::File::open(&path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let _ = self.bytes.set(OwnedBytes::new(mmap));
        Ok(self.bytes.get().unwrap())
    }
}

impl std::fmt::Debug for LazyFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LazyFileHandle({})", self.name)
    }
}

impl tantivy::HasLen for LazyFileHandle {
    fn len(&self) -> usize {
        self.dir
            .meta
            .files
            .get(&self.name)
            .map(|span| span.len as usize)
            .unwrap_or(0)
    }
}

impl FileHandle for LazyFileHandle {
    fn read_bytes(&self, range: std::ops::Range<usize>) -> std::io::Result<OwnedBytes> {
        Ok(self.bytes()?.slice(range))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::SplitBuilder;
    use crate::mapping::{IndexMapping, MappedSchema};
    use rsearch_storage::FsStorage;
    use tantivy::collector::Count;
    use tantivy::query::QueryParser;

    async fn build_and_upload(storage: &Arc<dyn Storage>) -> (String, u64) {
        let scratch = tempfile::tempdir().unwrap();
        let schema = MappedSchema::build(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {
                    "service": {"type": "keyword"},
                    "message": {"type": "text"},
                }
            }))
            .unwrap(),
        );
        let mut builder = SplitBuilder::new("logs", schema, scratch.path(), 20 << 20).unwrap();
        for i in 0..500 {
            builder
                .add_json(
                    serde_json::json!({
                        "@timestamp": 1_753_300_000_000_i64 + i,
                        "service": if i % 2 == 0 { "api" } else { "worker" },
                        "message": format!("event number {i}"),
                    }),
                    tantivy::DateTime::from_timestamp_millis(0),
                )
                .unwrap();
        }
        let packaged = builder.finish().unwrap();
        let key = format!("splits/{}.split", packaged.meta.split_id);
        storage.put_file(&key, &packaged.file_path).await.unwrap();
        (key, packaged.size_bytes)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn opens_and_searches_split_lazily() {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(store_dir.path()));
        let (key, split_size) = build_and_upload(&storage).await;

        let cache = Arc::new(SplitCache::new(cache_dir.path(), 1 << 30).unwrap());
        let reader = SplitReader::open(storage, &key, cache.clone()).await.unwrap();
        assert_eq!(reader.meta.split.doc_count, 500);

        let count = tokio::task::spawn_blocking(move || {
            let searcher = reader.searcher().unwrap();
            let parser =
                QueryParser::for_index(reader.index(), vec![]);
            let query = parser.parse_query("service:api").unwrap();
            searcher.search(&query, &Count).unwrap()
        })
        .await
        .unwrap();
        assert_eq!(count, 250);

        // Lazy fetching: the doc store was never touched, so the cache holds
        // strictly less than the full split.
        assert!(cache.total_bytes() > 0);
        assert!(
            cache.total_bytes() < split_size,
            "cache {} should be smaller than split {}",
            cache.total_bytes(),
            split_size
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn identity_round_trips_through_split() {
        use crate::document::DocIdentity;
        use tantivy::query::TermQuery;
        use tantivy::schema::{IndexRecordOption, Term};

        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(store_dir.path()));

        let schema = MappedSchema::build(IndexMapping::default());
        let mut builder = SplitBuilder::new("docs", schema, scratch.path(), 20 << 20).unwrap();
        for (id, seq) in [("alpha", 10), ("beta", 20), ("alpha", 30)] {
            builder
                .add_document(
                    serde_json::json!({"id": id, "seq": seq}),
                    None,
                    &DocIdentity::new(id, seq),
                    tantivy::DateTime::from_timestamp_millis(1_753_300_000_000),
                )
                .unwrap();
        }
        let packaged = builder.finish().unwrap();
        assert_eq!(packaged.meta.schema_version, crate::mapping::CURRENT_SCHEMA_VERSION);
        let key = format!("splits/{}.split", packaged.meta.split_id);
        storage.put_file(&key, &packaged.file_path).await.unwrap();

        let cache = Arc::new(SplitCache::new(cache_dir.path(), 1 << 30).unwrap());
        let reader = Arc::new(SplitReader::open(storage, &key, cache).await.unwrap());
        assert!(reader.mapped_schema().id.is_some());

        let r = reader.clone();
        let (alpha_count, seen) = tokio::task::spawn_blocking(move || {
            let searcher = r.searcher().unwrap();
            let id_field = r.mapped_schema().id.unwrap();
            let query = TermQuery::new(
                Term::from_field_text(id_field, "alpha"),
                IndexRecordOption::Basic,
            );
            let alpha_count = searcher.search(&query, &Count).unwrap();
            let mut seen = Vec::new();
            r.for_each_doc(
                |_, _| false,
                |doc| {
                    seen.push((doc.id.unwrap(), doc.seq));
                    Ok(())
                },
            )
            .unwrap();
            seen.sort();
            (alpha_count, seen)
        })
        .await
        .unwrap();
        assert_eq!(alpha_count, 2);
        assert_eq!(
            seen,
            vec![
                ("alpha".to_string(), 10),
                ("alpha".to_string(), 30),
                ("beta".to_string(), 20)
            ]
        );
    }
}
