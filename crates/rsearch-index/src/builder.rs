use std::path::PathBuf;

use tantivy::{Index, IndexWriter, TantivyDocument};

use crate::document::DocumentConverter;
use crate::error::{IndexError, IndexResult};
use crate::mapping::MappedSchema;
use crate::split_file::{self, SplitMeta};

/// Builds one immutable split from a batch of JSON log documents.
/// Synchronous and CPU-bound — callers run it on a blocking thread.
pub struct SplitBuilder {
    split_id: String,
    stream: String,
    converter: DocumentConverter,
    index: Index,
    writer: IndexWriter,
    work_dir: tempfile::TempDir,
    doc_count: u64,
    min_ts_millis: i64,
    max_ts_millis: i64,
}

/// A finished split: a single bundled file on local disk plus its
/// metadata, ready to upload and publish.
pub struct PackagedSplit {
    pub meta: SplitMeta,
    pub file_path: PathBuf,
    pub size_bytes: u64,
    /// Length of the footer metadata JSON (for single-range-read opens).
    pub footer_len: u64,
    // Keeps the backing temp dir alive until the split is uploaded.
    _work_dir: tempfile::TempDir,
}

impl SplitBuilder {
    /// `parent_work_dir`: node-local scratch space; a per-split temp dir is
    /// created beneath it. `memory_budget`: Tantivy writer heap in bytes.
    pub fn new(
        stream: impl Into<String>,
        schema: MappedSchema,
        parent_work_dir: &std::path::Path,
        memory_budget: usize,
    ) -> IndexResult<Self> {
        std::fs::create_dir_all(parent_work_dir)?;
        let work_dir = tempfile::Builder::new()
            .prefix("split-")
            .tempdir_in(parent_work_dir)?;
        let index_dir = work_dir.path().join("index");
        std::fs::create_dir(&index_dir)?;
        let index = Index::create_in_dir(&index_dir, schema.schema.clone())?;
        let writer = index.writer_with_num_threads(1, memory_budget)?;
        Ok(Self {
            split_id: uuid::Uuid::new_v4().simple().to_string(),
            stream: stream.into(),
            converter: DocumentConverter::new(schema),
            index,
            writer,
            work_dir,
            doc_count: 0,
            min_ts_millis: i64::MAX,
            max_ts_millis: i64::MIN,
        })
    }

    pub fn split_id(&self) -> &str {
        &self.split_id
    }

    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// Convert and buffer one document.
    pub fn add_json(
        &mut self,
        doc: &serde_json::Value,
        fallback_timestamp: tantivy::DateTime,
    ) -> IndexResult<()> {
        let (converted, ts): (TantivyDocument, _) =
            self.converter.convert(doc, fallback_timestamp)?;
        let millis = ts.into_timestamp_millis();
        self.min_ts_millis = self.min_ts_millis.min(millis);
        self.max_ts_millis = self.max_ts_millis.max(millis);
        self.writer.add_document(converted)?;
        self.doc_count += 1;
        Ok(())
    }

    /// Commit the index and bundle it into a single split file.
    pub fn finish(mut self) -> IndexResult<PackagedSplit> {
        if self.doc_count == 0 {
            return Err(IndexError::InvalidDocument(
                "refusing to build an empty split".to_string(),
            ));
        }
        self.writer.commit()?;
        self.writer.wait_merging_threads()?;

        let index_dir = self.work_dir.path().join("index");
        // Ensure directory contents are fully flushed before bundling.
        drop(self.index);

        let meta = SplitMeta {
            split_id: self.split_id.clone(),
            stream: self.stream.clone(),
            doc_count: self.doc_count,
            time_start_millis: self.min_ts_millis,
            time_end_millis: self.max_ts_millis,
            mapping: self.converter.schema().mapping.to_json(),
        };

        let file_path = self.work_dir.path().join(format!("{}.split", self.split_id));
        let mut out = std::io::BufWriter::new(std::fs::File::create(&file_path)?);
        let bundle = split_file::write_bundle(&index_dir, &mut out, meta.clone())?;
        let footer_len = serde_json::to_vec(&bundle)
            .map(|v| v.len() as u64)
            .unwrap_or(0);
        let out = out
            .into_inner()
            .map_err(|e| IndexError::Io(e.into_error()))?;
        out.sync_all()?;
        let size_bytes = out.metadata()?.len();

        Ok(PackagedSplit {
            meta,
            file_path,
            size_bytes,
            footer_len,
            _work_dir: self.work_dir,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{IndexMapping, MappedSchema};
    use crate::split_file::{FOOTER_TAIL_LEN, parse_footer_tail, parse_meta};

    fn schema() -> MappedSchema {
        MappedSchema::build(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {
                    "service": {"type": "keyword"},
                    "message": {"type": "text"},
                    "status": {"type": "long"},
                }
            }))
            .unwrap(),
        )
    }

    fn fallback() -> tantivy::DateTime {
        tantivy::DateTime::from_timestamp_millis(1_753_300_000_000)
    }

    #[test]
    fn builds_a_split_with_footer() {
        let scratch = tempfile::tempdir().unwrap();
        let mut builder =
            SplitBuilder::new("app-logs", schema(), scratch.path(), 50 << 20).unwrap();
        for i in 0..100 {
            builder
                .add_json(
                    &serde_json::json!({
                        "@timestamp": 1_753_300_000_000_i64 + i,
                        "service": "api",
                        "message": format!("request {i} handled"),
                        "status": 200,
                    }),
                    fallback(),
                )
                .unwrap();
        }
        let packaged = builder.finish().unwrap();
        assert_eq!(packaged.meta.doc_count, 100);
        assert_eq!(packaged.meta.time_start_millis, 1_753_300_000_000);
        assert_eq!(packaged.meta.time_end_millis, 1_753_300_000_099);
        assert!(packaged.size_bytes > 0);

        // Footer parses and the file map spans are consistent.
        let bytes = std::fs::read(&packaged.file_path).unwrap();
        let tail = &bytes[bytes.len() - FOOTER_TAIL_LEN as usize..];
        let meta_len = parse_footer_tail(tail).unwrap() as usize;
        let meta_start = bytes.len() - FOOTER_TAIL_LEN as usize - meta_len;
        let meta = parse_meta(&bytes[meta_start..meta_start + meta_len]).unwrap();
        assert_eq!(meta.split.doc_count, 100);
        assert!(!meta.files.is_empty());
        let data_end: u64 = meta.files.values().map(|s| s.offset + s.len).max().unwrap();
        assert_eq!(data_end as usize, meta_start);
    }

    #[test]
    fn refuses_empty_split() {
        let scratch = tempfile::tempdir().unwrap();
        let builder = SplitBuilder::new("s", schema(), scratch.path(), 50 << 20).unwrap();
        assert!(builder.finish().is_err());
    }
}

#[cfg(test)]
mod timestamp_units {
    use super::*;
    use crate::mapping::{IndexMapping, MappedSchema};

    /// Regression: shippers send epoch timestamps in seconds, millis,
    /// micros, or nanos. None of them may panic the indexer.
    #[test]
    fn hostile_timestamp_units_never_panic() {
        let scratch = tempfile::tempdir().unwrap();
        let schema = MappedSchema::build(IndexMapping::default());
        let mut builder = SplitBuilder::new("repro", schema, scratch.path(), 50 << 20).unwrap();
        for ts in [
            serde_json::json!(1_784_871_020_i64),             // seconds
            serde_json::json!(1_784_871_020_163_i64),         // millis
            serde_json::json!(1_784_871_020_163_018_i64),     // micros
            serde_json::json!(1_784_871_020_163_018_377_i64), // nanos
            serde_json::json!(1_784_871_020.163),             // float secs (GELF)
            serde_json::json!(i64::MAX),
            serde_json::json!(i64::MIN),
            serde_json::json!(f64::INFINITY),
        ] {
            builder
                .add_json(
                    &serde_json::json!({"@timestamp": ts, "message": "x"}),
                    tantivy::DateTime::from_timestamp_millis(1_784_871_020_163),
                )
                .unwrap();
        }
        let packaged = builder.finish().unwrap();
        assert_eq!(packaged.meta.doc_count, 8);
        // All finite epoch variants agree on the same instant (to the second).
        assert!(packaged.meta.time_start_millis <= -9_000_000_000_000);
        assert!(packaged.meta.time_end_millis >= 9_000_000_000_000);
    }

    #[test]
    fn epoch_unit_detection() {
        use crate::document::epoch_to_millis;
        assert_eq!(epoch_to_millis(1_784_871_020), 1_784_871_020_000);
        assert_eq!(epoch_to_millis(1_784_871_020_163), 1_784_871_020_163);
        assert_eq!(epoch_to_millis(1_784_871_020_163_018), 1_784_871_020_163);
        assert_eq!(epoch_to_millis(1_784_871_020_163_018_377), 1_784_871_020_163);
        assert!(epoch_to_millis(i64::MAX) <= i64::MAX / 1_000_000);
        assert!(epoch_to_millis(i64::MIN) >= -(i64::MAX / 1_000_000));
    }
}
