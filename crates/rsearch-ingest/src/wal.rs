//! Append-before-ack write-ahead log. Documents are fsynced to a local
//! segment file before the bulk request is acknowledged; segments are
//! deleted once every document they contain has been published in a split.
//!
//! Record layout: [len: u32 LE][crc32(payload): u32 LE][payload]
//! where payload = [stream_len: u16 LE][stream][doc JSON bytes].

use std::collections::BTreeMap;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Position of a record: the segment that holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalPos {
    pub segment: u64,
}

/// A replayed record.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub stream: String,
    pub doc: Vec<u8>,
    pub pos: WalPos,
}

struct SegmentState {
    outstanding: u64,
    sealed: bool,
}

struct WalInner {
    current_seq: u64,
    current_len: u64,
    writer: BufWriter<std::fs::File>,
    segments: BTreeMap<u64, SegmentState>,
    /// Total records write(2)-flushed to the OS (not yet fsynced).
    written: u64,
}

/// Thread-safe WAL. Appends are called from blocking contexts (they do
/// file I/O); confirms are cheap.
pub struct Wal {
    dir: PathBuf,
    max_segment_bytes: u64,
    inner: Mutex<WalInner>,
    /// Records durably fsynced. Group commit: whoever holds `sync_gate`
    /// runs one fsync that makes every write flushed before it durable, so
    /// concurrent appends don't each pay an independent fsync.
    synced: std::sync::atomic::AtomicU64,
    sync_gate: Mutex<()>,
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal-{seq:016}.log"))
}

impl Wal {
    /// Open the WAL. Returns the WAL and a lazy [`WalReplay`] iterator over
    /// the outstanding records (unpublished documents from before a
    /// restart). Records are streamed from disk one at a time so replaying
    /// a large backlog never materializes it in memory — the caller's
    /// bounded queues provide end-to-end backpressure.
    pub fn open(dir: impl Into<PathBuf>, max_segment_bytes: u64) -> std::io::Result<(Self, WalReplay)> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;

        let mut seqs: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let name = entry?.file_name().to_string_lossy().to_string();
            if let Some(num) = name
                .strip_prefix("wal-")
                .and_then(|s| s.strip_suffix(".log"))
                && let Ok(seq) = num.parse::<u64>()
            {
                seqs.push(seq);
            }
        }
        seqs.sort_unstable();

        // Streaming counting pass: validates each sealed segment and sets
        // its outstanding count without keeping any records. The replay
        // iterator re-reads with the same cursor logic, so the records it
        // yields always balance the counts recorded here.
        let mut segments = BTreeMap::new();
        for &seq in &seqs {
            let path = segment_path(&dir, seq);
            let mut cursor = SegmentCursor::open(&path, seq)?;
            let mut count = 0u64;
            while cursor.advance() {
                count += 1;
            }
            segments.insert(
                seq,
                SegmentState {
                    outstanding: count,
                    sealed: true,
                },
            );
        }
        let replay = WalReplay {
            segments: seqs
                .iter()
                .map(|&seq| (seq, segment_path(&dir, seq)))
                .collect::<Vec<_>>()
                .into_iter(),
            current: None,
        };

        // New writes go to a fresh segment after the highest existing one.
        let current_seq = seqs.last().map(|s| s + 1).unwrap_or(0);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&dir, current_seq))?;
        segments.insert(
            current_seq,
            SegmentState {
                outstanding: 0,
                sealed: false,
            },
        );

        Ok((
            Self {
                dir,
                max_segment_bytes,
                inner: Mutex::new(WalInner {
                    current_seq,
                    current_len: 0,
                    writer: BufWriter::new(file),
                    segments,
                    written: 0,
                }),
                synced: std::sync::atomic::AtomicU64::new(0),
                sync_gate: Mutex::new(()),
            },
            replay,
        ))
    }

    /// Append a batch of (stream, doc) pairs durably. Returns the position
    /// of each record. Blocking — call via spawn_blocking.
    ///
    /// Two phases: records are written and OS-flushed under the writer lock
    /// (records serialized directly, CRC streamed — no per-record temp
    /// buffer), then the fsync runs under a separate gate so concurrent
    /// appends coalesce into one fsync (group commit) instead of each
    /// paying its own.
    pub fn append_batch<S, D>(&self, items: &[(S, D)]) -> std::io::Result<Vec<WalPos>>
    where
        S: AsRef<str>,
        D: AsRef<str>,
    {
        use std::sync::atomic::Ordering;
        let mut positions = Vec::with_capacity(items.len());
        // Segments sealed by rotation during this batch are fsynced below,
        // outside the writer lock; fully-confirmed segments GC found are
        // unlinked outside it too — neither disk op stalls other appenders.
        let mut sealed: Vec<std::fs::File> = Vec::new();
        let mut victims: Vec<PathBuf> = Vec::new();
        let (fd, my_gen) = {
            let mut inner = self.inner.lock().unwrap();
            for (stream, doc) in items {
                let doc = doc.as_ref().as_bytes();
                if inner.current_len >= self.max_segment_bytes {
                    let (old_file, mut gc) = self.rotate(&mut inner)?;
                    sealed.push(old_file);
                    victims.append(&mut gc);
                }
                let stream_bytes = stream.as_ref().as_bytes();
                let payload_len = 2 + stream_bytes.len() + doc.len();
                let len_field = stream_bytes.len() as u16;
                // Stream the CRC over the three payload slices; write them
                // directly to the buffered writer (no intermediate Vec).
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&len_field.to_le_bytes());
                hasher.update(stream_bytes);
                hasher.update(doc);
                let crc = hasher.finalize();
                inner.writer.write_all(&(payload_len as u32).to_le_bytes())?;
                inner.writer.write_all(&crc.to_le_bytes())?;
                inner.writer.write_all(&len_field.to_le_bytes())?;
                inner.writer.write_all(stream_bytes)?;
                inner.writer.write_all(doc)?;
                inner.current_len += 8 + payload_len as u64;
                let seq = inner.current_seq;
                inner
                    .segments
                    .get_mut(&seq)
                    .expect("current segment tracked")
                    .outstanding += 1;
                positions.push(WalPos { segment: seq });
            }
            // Push buffered bytes to the OS, then clone the fd so the fsync
            // can run outside this lock. `written` is bumped only after the
            // flush, so any observer of a given gen knows it was flushed.
            inner.writer.flush()?;
            let fd = inner.writer.get_ref().try_clone()?;
            inner.written += items.len() as u64;
            (fd, inner.written)
        };

        // Rotated-out segments may hold records from this batch, so they
        // must be durable before the positions are acked. The group-commit
        // fsync below only covers the current segment's fd.
        for old in &sealed {
            old.sync_data()?;
        }
        for path in victims {
            let _ = std::fs::remove_file(path);
        }

        // Group commit: if someone already fsynced past our records, skip;
        // otherwise fsync once (covers everyone flushed so far).
        if self.synced.load(Ordering::Acquire) < my_gen {
            let _gate = self.sync_gate.lock().unwrap();
            if self.synced.load(Ordering::Acquire) < my_gen {
                // Snapshot the flushed high-water mark before the fsync so
                // we can advance `synced` to cover all coalesced writers.
                let covered = self.inner.lock().unwrap().written;
                fd.sync_data()?;
                self.synced.fetch_max(covered, Ordering::AcqRel);
            }
        }
        Ok(positions)
    }

    /// Seal the current segment and open a fresh one. Returns the sealed
    /// file — the caller fsyncs it outside the writer lock — plus any
    /// fully-confirmed segments for the caller to unlink, also outside.
    fn rotate(&self, inner: &mut WalInner) -> std::io::Result<(std::fs::File, Vec<PathBuf>)> {
        inner.writer.flush()?;
        let old_seq = inner.current_seq;
        if let Some(state) = inner.segments.get_mut(&old_seq) {
            state.sealed = true;
        }
        let new_seq = old_seq + 1;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&self.dir, new_seq))?;
        let old = std::mem::replace(&mut inner.writer, BufWriter::new(file));
        let old_file = old
            .into_inner()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        inner.current_seq = new_seq;
        inner.current_len = 0;
        inner.segments.insert(
            new_seq,
            SegmentState {
                outstanding: 0,
                sealed: false,
            },
        );
        // The old segment may already be fully confirmed.
        let victims = self.collect_confirmed(inner);
        Ok((old_file, victims))
    }

    /// Confirm records as durably published; deletes exhausted segments.
    pub fn confirm(&self, positions: &[WalPos]) {
        let victims = {
            let mut inner = self.inner.lock().unwrap();
            for pos in positions {
                if let Some(state) = inner.segments.get_mut(&pos.segment) {
                    state.outstanding = state.outstanding.saturating_sub(1);
                }
            }
            self.collect_confirmed(&mut inner)
        };
        for path in victims {
            let _ = std::fs::remove_file(path);
        }
    }

    /// Drop fully-confirmed sealed segments from tracking and return their
    /// paths. The caller unlinks them after releasing the writer lock so
    /// the syscall never stalls concurrent appends. Each victim is removed
    /// from the map here, so racing callers never unlink the same path.
    fn collect_confirmed(&self, inner: &mut WalInner) -> Vec<PathBuf> {
        let seqs: Vec<u64> = inner
            .segments
            .iter()
            .filter(|(_, s)| s.sealed && s.outstanding == 0)
            .map(|(&seq, _)| seq)
            .collect();
        seqs.into_iter()
            .map(|seq| {
                inner.segments.remove(&seq);
                segment_path(&self.dir, seq)
            })
            .collect()
    }

    /// Number of live (non-deleted) segments — used by tests and metrics.
    pub fn segment_count(&self) -> usize {
        self.inner.lock().unwrap().segments.len()
    }

    /// Total unconfirmed records across all segments.
    pub fn outstanding(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .segments
            .values()
            .map(|s| s.outstanding)
            .sum()
    }
}

/// Streaming cursor over one segment file. Holds a single reusable
/// payload buffer, so memory stays O(largest record) regardless of
/// segment size. A corrupt or truncated tail (torn write from a crash)
/// ends the segment silently, as does a mid-segment read error.
struct SegmentCursor {
    seq: u64,
    reader: std::io::BufReader<std::fs::File>,
    buf: Vec<u8>,
    stream_len: usize,
}

impl SegmentCursor {
    fn open(path: &Path, seq: u64) -> std::io::Result<Self> {
        Ok(Self {
            seq,
            reader: std::io::BufReader::new(std::fs::File::open(path)?),
            buf: Vec::new(),
            stream_len: 0,
        })
    }

    /// Read and validate the next record into the internal buffer.
    /// Returns false at the end of the segment (EOF, torn tail, or
    /// corruption).
    fn advance(&mut self) -> bool {
        let mut header = [0u8; 8];
        if self.reader.read_exact(&mut header).is_err() {
            return false;
        }
        let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if len < 2 {
            return false;
        }
        self.buf.clear();
        // take + read_to_end grows the buffer incrementally, so a corrupt
        // length field yields a short read instead of a huge allocation.
        match (&mut self.reader).take(len as u64).read_to_end(&mut self.buf) {
            Ok(n) if n == len => {}
            _ => return false, // torn tail
        }
        if crc32fast::hash(&self.buf) != crc {
            return false; // corruption — stop replaying this segment
        }
        let stream_len = u16::from_le_bytes(self.buf[0..2].try_into().unwrap()) as usize;
        if 2 + stream_len > self.buf.len() {
            return false;
        }
        self.stream_len = stream_len;
        true
    }

    fn record(&self) -> WalRecord {
        WalRecord {
            stream: String::from_utf8_lossy(&self.buf[2..2 + self.stream_len]).to_string(),
            doc: self.buf[2 + self.stream_len..].to_vec(),
            pos: WalPos { segment: self.seq },
        }
    }
}

/// Lazy iterator over the outstanding records found at [`Wal::open`].
/// Segments are opened and read one record at a time; an error opening a
/// segment is yielded so the caller can abort replay rather than silently
/// lose records.
pub struct WalReplay {
    segments: std::vec::IntoIter<(u64, PathBuf)>,
    current: Option<SegmentCursor>,
}

impl Iterator for WalReplay {
    type Item = std::io::Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                let (seq, path) = self.segments.next()?;
                match SegmentCursor::open(&path, seq) {
                    Ok(cursor) => self.current = Some(cursor),
                    Err(e) => return Some(Err(e)),
                }
            }
            let cursor = self.current.as_mut().expect("cursor set above");
            if cursor.advance() {
                return Some(Ok(cursor.record()));
            }
            self.current = None; // segment exhausted
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(n: usize, stream: &str) -> Vec<(String, String)> {
        (0..n)
            .map(|i| (stream.to_string(), format!("{{\"n\":{i}}}")))
            .collect()
    }

    #[test]
    fn append_confirm_deletes_sealed_segments() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segments force rotation quickly.
        let (wal, replayed) = Wal::open(dir.path(), 128).unwrap();
        assert_eq!(replayed.count(), 0);

        let positions = wal.append_batch(&items(20, "s")).unwrap();
        assert_eq!(wal.outstanding(), 20);
        assert!(wal.segment_count() > 1, "should have rotated");

        wal.confirm(&positions);
        assert_eq!(wal.outstanding(), 0);
        // Only the unsealed current segment remains.
        assert_eq!(wal.segment_count(), 1);
    }

    #[test]
    fn replay_after_restart_returns_unconfirmed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = Wal::open(dir.path(), 1 << 20).unwrap();
            wal.append_batch(&items(5, "app")).unwrap();
            // No confirm — simulating a crash before publish.
        }
        let (wal, replayed) = Wal::open(dir.path(), 1 << 20).unwrap();
        let replayed: Vec<WalRecord> = replayed.collect::<std::io::Result<_>>().unwrap();
        assert_eq!(replayed.len(), 5);
        assert_eq!(replayed[0].stream, "app");
        assert_eq!(wal.outstanding(), 5);
        // Confirming replayed positions releases the old segments.
        let positions: Vec<WalPos> = replayed.iter().map(|r| r.pos).collect();
        wal.confirm(&positions);
        assert_eq!(wal.outstanding(), 0);
    }

    #[test]
    fn concurrent_group_commit_loses_nothing() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::open(dir.path(), 1 << 20).unwrap();
        let wal = Arc::new(wal);
        // 8 threads each append 100 records concurrently; group commit must
        // not drop or mis-sync any of them.
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let wal = wal.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        wal.append_batch(&items(1, &format!("s{t}"))).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(wal.outstanding(), 800);
        drop(wal);
        // Everything must replay after a "restart".
        let (_, replayed) = Wal::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(replayed.map(|r| r.unwrap()).count(), 800);
    }

    #[test]
    fn torn_tail_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = Wal::open(dir.path(), 1 << 20).unwrap();
            wal.append_batch(&items(3, "s")).unwrap();
        }
        // Corrupt the tail of the only data segment.
        let seg = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.to_string_lossy().contains("wal-0000000000000000"))
            .unwrap();
        let mut data = std::fs::read(&seg).unwrap();
        let cut = data.len() - 3;
        data.truncate(cut);
        data.extend_from_slice(&[0xFF; 2]);
        std::fs::write(&seg, data).unwrap();

        let (wal, replayed) = Wal::open(dir.path(), 1 << 20).unwrap();
        // Last record is torn; first two survive.
        assert_eq!(replayed.map(|r| r.unwrap()).count(), 2);
        // The counting pass must agree with what the iterator yielded.
        assert_eq!(wal.outstanding(), 2);
    }

    #[test]
    fn replay_streams_in_order_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        {
            // Tiny segments so 50 records span many files.
            let (wal, _) = Wal::open(dir.path(), 64).unwrap();
            wal.append_batch(&items(50, "s")).unwrap();
            assert!(wal.segment_count() > 1, "should have rotated");
            // No confirm — simulating a crash before publish.
        }
        let (wal, replayed) = Wal::open(dir.path(), 64).unwrap();
        let records: Vec<WalRecord> = replayed.collect::<std::io::Result<_>>().unwrap();
        assert_eq!(records.len(), 50);
        assert_eq!(wal.outstanding(), 50);
        // Records come back in append order with segment positions that
        // balance the outstanding counts when confirmed.
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.doc, format!("{{\"n\":{i}}}").into_bytes());
        }
        let positions: Vec<WalPos> = records.iter().map(|r| r.pos).collect();
        wal.confirm(&positions);
        assert_eq!(wal.outstanding(), 0);
        assert_eq!(wal.segment_count(), 1);
    }
}
