//! Tombstone application. A tombstone says "hide every version of `_id`
//! whose `_seq` is below `before_seq`". Resolving that against an
//! immutable split yields a fixed set of (segment, doc) addresses, so the
//! set is computed once per split and extended incrementally as newer
//! tombstones arrive — Lucene's live-docs bitset, kept in cache form
//! because splits are shared read-only objects.

use std::collections::HashMap;
use std::sync::Arc;

use tantivy::index::SegmentId;
use tantivy::query::{ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED};

use crate::error::IndexResult;
use crate::mapping::SEQ_FIELD;
use crate::reader::SplitReader;

/// One tombstone, as stored in the metastore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    /// Monotonic tombstone ordinal; application state is tracked by it.
    pub seq: i64,
    /// The `_id` whose older versions are hidden.
    pub doc_id: String,
    /// Versions with `_seq` strictly below this are hidden.
    pub before_seq: i64,
}

/// The documents of one split hidden by every tombstone applied so far.
#[derive(Debug)]
pub struct ExclusionSet {
    /// Highest tombstone `seq` folded into this set.
    pub applied_through: i64,
    /// Per segment ordinal: sorted, de-duplicated excluded doc ids.
    per_segment: Vec<Arc<[DocId]>>,
    /// Segment id → ordinal (a `SegmentReader` knows its id, not its
    /// ordinal, when a `Weight` builds a scorer for it).
    ordinals: HashMap<SegmentId, usize>,
    total: usize,
}

impl ExclusionSet {
    /// An empty set for a split whose segments are `segment_ids`, in
    /// ordinal order.
    pub fn empty(segment_ids: Vec<SegmentId>) -> Self {
        Self {
            applied_through: 0,
            per_segment: vec![Arc::from(Vec::new()); segment_ids.len()],
            ordinals: segment_ids
                .into_iter()
                .enumerate()
                .map(|(ord, id)| (id, ord))
                .collect(),
            total: 0,
        }
    }

    /// Excluded docs of the segment with the given id (empty if none).
    fn for_segment(&self, id: SegmentId) -> Arc<[DocId]> {
        self.ordinals
            .get(&id)
            .and_then(|ord| self.per_segment.get(*ord))
            .cloned()
            .unwrap_or_else(|| Arc::from(Vec::new()))
    }

    /// Number of excluded documents across all segments.
    pub fn len(&self) -> usize {
        self.total
    }

    /// True when nothing is excluded.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Whether (segment, doc) is excluded.
    pub fn contains(&self, segment_ord: u32, doc: DocId) -> bool {
        self.per_segment
            .get(segment_ord as usize)
            .map(|docs| docs.binary_search(&doc).is_ok())
            .unwrap_or(false)
    }

    /// Fold newly excluded docs (unsorted, may repeat) into a new set.
    fn extended(&self, mut additions: Vec<Vec<DocId>>, applied_through: i64) -> Self {
        let mut per_segment = Vec::with_capacity(self.per_segment.len());
        let mut total = 0;
        for (ord, existing) in self.per_segment.iter().enumerate() {
            let added = additions
                .get_mut(ord)
                .map(std::mem::take)
                .unwrap_or_default();
            let docs: Arc<[DocId]> = if added.is_empty() {
                existing.clone()
            } else {
                let mut merged: Vec<DocId> = existing.iter().copied().chain(added).collect();
                merged.sort_unstable();
                merged.dedup();
                Arc::from(merged)
            };
            total += docs.len();
            per_segment.push(docs);
        }
        Self {
            applied_through,
            per_segment,
            ordinals: self.ordinals.clone(),
            total,
        }
    }
}

impl SplitReader {
    /// Fold `tombstones` (ascending by `seq`) into this split's cached
    /// exclusion set and return the result. Tombstones at or below the
    /// already-applied seq are skipped, so callers pass their whole cached
    /// list each time and only the tail costs anything. Call from a
    /// blocking context (term lookups may read from storage).
    pub fn apply_tombstones(&self, tombstones: &[Tombstone]) -> IndexResult<Arc<ExclusionSet>> {
        // Serialize appliers: concurrent queries on one split wait for the
        // first rather than each repeating the term lookups. A poisoned
        // lock (a panic mid-apply) just yields the last good set.
        let mut slot = self
            .exclusions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = slot.clone();
        let start = tombstones.partition_point(|t| t.seq <= current.applied_through);
        let pending = &tombstones[start..];
        let Some(last) = pending.last() else {
            return Ok(current);
        };
        let applied_through = last.seq;
        let id_field = match self.mapped_schema().id {
            Some(field) => field,
            None => {
                // Legacy split: no ids, so tombstones can never apply.
                let next = Arc::new(current.extended(Vec::new(), applied_through));
                *slot = next.clone();
                return Ok(next);
            }
        };
        // Nothing in the split is older than a tombstone's bound when the
        // split's lowest _seq is already at or past it.
        let seq_min = self.meta.split.seq_min;
        let searcher = self.searcher()?;
        let segments = searcher.segment_readers();
        let mut additions: Vec<Vec<DocId>> = vec![Vec::new(); segments.len()];
        for (ord, segment) in segments.iter().enumerate() {
            let inverted = segment.inverted_index(id_field)?;
            let seq_column = segment.fast_fields().i64(SEQ_FIELD)?;
            for tombstone in pending {
                if seq_min.is_some_and(|min| tombstone.before_seq <= min) {
                    continue;
                }
                let term = Term::from_field_text(id_field, &tombstone.doc_id);
                let Some(mut postings) = inverted.read_postings(&term, IndexRecordOption::Basic)?
                else {
                    continue;
                };
                let mut doc = postings.doc();
                while doc != TERMINATED {
                    if seq_column
                        .first(doc)
                        .is_some_and(|seq| seq < tombstone.before_seq)
                    {
                        additions[ord].push(doc);
                    }
                    doc = postings.advance();
                }
            }
        }
        let next = Arc::new(current.extended(additions, applied_through));
        *slot = next.clone();
        Ok(next)
    }

    /// Start the exclusion bookkeeping at `applied_through`: tombstones at
    /// or below it are known to hide nothing in this split (it was built —
    /// by compaction or a merge — with them applied), so they are never
    /// looked up. Only moves forward, and only while nothing has been
    /// applied yet (the cached set would otherwise be incomplete).
    pub fn seed_applied_through(&self, applied_through: i64) {
        let mut slot = self
            .exclusions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.applied_through == 0 && slot.is_empty() && applied_through > 0 {
            *slot = Arc::new(ExclusionSet {
                applied_through,
                per_segment: slot.per_segment.clone(),
                ordinals: slot.ordinals.clone(),
                total: 0,
            });
        }
    }
}

/// Matches exactly the documents in an [`ExclusionSet`] — used as a
/// `must_not` clause so counts, hits, and aggregations all skip them.
#[derive(Debug, Clone)]
pub struct ExcludeDocsQuery {
    set: Arc<ExclusionSet>,
}

impl ExcludeDocsQuery {
    /// A query over the given set.
    pub fn new(set: Arc<ExclusionSet>) -> Self {
        Self { set }
    }
}

impl Query for ExcludeDocsQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(ExcludeDocsWeight {
            set: self.set.clone(),
        }))
    }
}

struct ExcludeDocsWeight {
    set: Arc<ExclusionSet>,
}

impl Weight for ExcludeDocsWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let docs = self.set.for_segment(reader.segment_id());
        Ok(Box::new(ConstScorer::new(SortedDocSet::new(docs), boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let docs = self.set.for_segment(reader.segment_id());
        if docs.binary_search(&doc).is_ok() {
            Ok(Explanation::new("tombstoned document", 1.0))
        } else {
            Err(tantivy::TantivyError::InvalidArgument(
                "document is not excluded".to_string(),
            ))
        }
    }
}

/// A `DocSet` over a sorted, de-duplicated slice of doc ids.
struct SortedDocSet {
    docs: Arc<[DocId]>,
    cursor: usize,
}

impl SortedDocSet {
    fn new(docs: Arc<[DocId]>) -> Self {
        Self { docs, cursor: 0 }
    }
}

impl DocSet for SortedDocSet {
    fn advance(&mut self) -> DocId {
        self.cursor = self.cursor.saturating_add(1).min(self.docs.len());
        self.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        // Docs are sorted: jump to the first id >= target.
        let rest = &self.docs[self.cursor.min(self.docs.len())..];
        self.cursor += rest.partition_point(|&d| d < target);
        self.doc()
    }

    fn doc(&self) -> DocId {
        self.docs.get(self.cursor).copied().unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        self.docs.len().saturating_sub(self.cursor) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::SplitBuilder;
    use crate::cache::SplitCache;
    use crate::document::DocIdentity;
    use crate::mapping::{IndexMapping, MappedSchema};
    use rsearch_storage::{FsStorage, Storage};
    use tantivy::collector::Count;
    use tantivy::query::{AllQuery, BooleanQuery, Occur, TermQuery};

    /// alpha@10, beta@20, alpha@30, gamma@40 (two segments worth of adds
    /// are irrelevant — one commit, one segment).
    async fn open_split() -> (Arc<SplitReader>, tempfile::TempDir, tempfile::TempDir) {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(FsStorage::new(store_dir.path()));
        let schema = MappedSchema::build(IndexMapping::default());
        let mut builder = SplitBuilder::new("docs", schema, scratch.path(), 20 << 20).unwrap();
        for (id, seq) in [("alpha", 10), ("beta", 20), ("alpha", 30), ("gamma", 40)] {
            builder
                .add_document(
                    serde_json::json!({"id": id}),
                    None,
                    &DocIdentity::new(id, seq),
                    tantivy::DateTime::from_timestamp_millis(1_753_300_000_000),
                )
                .unwrap();
        }
        let packaged = builder.finish().unwrap();
        assert_eq!(packaged.meta.seq_min, Some(10));
        assert_eq!(packaged.meta.seq_max, Some(40));
        let key = format!("splits/{}.split", packaged.meta.split_id);
        storage.put_file(&key, &packaged.file_path).await.unwrap();
        let cache = Arc::new(SplitCache::new(cache_dir.path(), 1 << 30).unwrap());
        let reader = Arc::new(SplitReader::open(storage, &key, cache).await.unwrap());
        (reader, store_dir, cache_dir)
    }

    fn count(reader: &SplitReader, set: &Arc<ExclusionSet>, query: Box<dyn Query>) -> usize {
        let searcher = reader.searcher().unwrap();
        let wrapped = BooleanQuery::new(vec![
            (Occur::Must, query),
            (Occur::MustNot, Box::new(ExcludeDocsQuery::new(set.clone()))),
        ]);
        searcher.search(&wrapped, &Count).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tombstones_hide_older_versions_incrementally() {
        let (reader, _s, _c) = open_split().await;
        let r = reader.clone();
        tokio::task::spawn_blocking(move || {
            let id_field = r.mapped_schema().id.unwrap();
            let alpha = || -> Box<dyn Query> {
                Box::new(TermQuery::new(
                    Term::from_field_text(id_field, "alpha"),
                    IndexRecordOption::Basic,
                ))
            };

            // Nothing applied: everything visible.
            let empty = r.apply_tombstones(&[]).unwrap();
            assert!(empty.is_empty());
            assert_eq!(count(&r, &empty, Box::new(AllQuery)), 4);

            // "alpha replaced at seq 30": hides alpha@10 only.
            let set = r
                .apply_tombstones(&[Tombstone {
                    seq: 1,
                    doc_id: "alpha".into(),
                    before_seq: 30,
                }])
                .unwrap();
            assert_eq!(set.len(), 1);
            assert_eq!(set.applied_through, 1);
            assert_eq!(count(&r, &set, Box::new(AllQuery)), 3);
            assert_eq!(count(&r, &set, alpha()), 1);

            // Re-applying the same list is a no-op (already past seq 1).
            let again = r
                .apply_tombstones(&[Tombstone {
                    seq: 1,
                    doc_id: "alpha".into(),
                    before_seq: 30,
                }])
                .unwrap();
            assert!(Arc::ptr_eq(&set, &again));

            // Delete beta (before everything) and alpha entirely: the
            // cached set extends by the tail only.
            let set = r
                .apply_tombstones(&[
                    Tombstone {
                        seq: 1,
                        doc_id: "alpha".into(),
                        before_seq: 30,
                    },
                    Tombstone {
                        seq: 2,
                        doc_id: "beta".into(),
                        before_seq: 1_000,
                    },
                    Tombstone {
                        seq: 3,
                        doc_id: "alpha".into(),
                        before_seq: 1_000,
                    },
                ])
                .unwrap();
            assert_eq!(set.len(), 3);
            assert_eq!(set.applied_through, 3);
            assert_eq!(count(&r, &set, Box::new(AllQuery)), 1);
            assert_eq!(count(&r, &set, alpha()), 0);
            // A tombstone at or below the split's lowest _seq is skipped
            // without any lookup; unknown ids are harmless.
            let set = r
                .apply_tombstones(&[
                    Tombstone {
                        seq: 4,
                        doc_id: "gamma".into(),
                        before_seq: 10,
                    },
                    Tombstone {
                        seq: 5,
                        doc_id: "nobody".into(),
                        before_seq: 1_000,
                    },
                ])
                .unwrap();
            assert_eq!(set.len(), 3);
            assert_eq!(set.applied_through, 5);
            assert!(set.contains(0, 0));
            assert!(!set.contains(0, 3));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seeded_reader_skips_already_applied_tombstones() {
        let (reader, _s, _c) = open_split().await;
        tokio::task::spawn_blocking(move || {
            // The split was (notionally) rebuilt with tombstones through
            // seq 5 applied: those are never looked up, later ones are.
            reader.seed_applied_through(5);
            let set = reader
                .apply_tombstones(&[
                    Tombstone {
                        seq: 3,
                        doc_id: "alpha".into(),
                        before_seq: 1_000,
                    },
                    Tombstone {
                        seq: 7,
                        doc_id: "beta".into(),
                        before_seq: 1_000,
                    },
                ])
                .unwrap();
            assert_eq!(set.applied_through, 7);
            assert_eq!(set.len(), 1, "only beta (seq 7) was applied");
            // Seeding after application is a no-op.
            reader.seed_applied_through(100);
            assert_eq!(reader.apply_tombstones(&[]).unwrap().applied_through, 7);
        })
        .await
        .unwrap();
    }
}
