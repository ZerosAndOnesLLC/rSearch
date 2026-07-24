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
    /// Open the WAL, replaying any existing segments. Returns the WAL and
    /// the replayed records (unpublished documents from before a restart).
    pub fn open(dir: impl Into<PathBuf>, max_segment_bytes: u64) -> std::io::Result<(Self, Vec<WalRecord>)> {
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

        let mut records = Vec::new();
        let mut segments = BTreeMap::new();
        for &seq in &seqs {
            let count = replay_segment(&segment_path(&dir, seq), seq, &mut records)?;
            segments.insert(
                seq,
                SegmentState {
                    outstanding: count,
                    sealed: true,
                },
            );
        }

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
            records,
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
    pub fn append_batch(&self, items: &[(String, Vec<u8>)]) -> std::io::Result<Vec<WalPos>> {
        use std::sync::atomic::Ordering;
        let mut positions = Vec::with_capacity(items.len());
        let (fd, my_gen) = {
            let mut inner = self.inner.lock().unwrap();
            for (stream, doc) in items {
                if inner.current_len >= self.max_segment_bytes {
                    self.rotate(&mut inner)?;
                }
                let stream_bytes = stream.as_bytes();
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

    fn rotate(&self, inner: &mut WalInner) -> std::io::Result<()> {
        inner.writer.flush()?;
        inner.writer.get_ref().sync_data()?;
        let old_seq = inner.current_seq;
        if let Some(state) = inner.segments.get_mut(&old_seq) {
            state.sealed = true;
        }
        let new_seq = old_seq + 1;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&self.dir, new_seq))?;
        inner.writer = BufWriter::new(file);
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
        Self::gc_locked(&self.dir, inner);
        Ok(())
    }

    /// Confirm records as durably published; deletes exhausted segments.
    pub fn confirm(&self, positions: &[WalPos]) {
        let mut inner = self.inner.lock().unwrap();
        for pos in positions {
            if let Some(state) = inner.segments.get_mut(&pos.segment) {
                state.outstanding = state.outstanding.saturating_sub(1);
            }
        }
        Self::gc_locked(&self.dir, &mut inner);
    }

    fn gc_locked(dir: &Path, inner: &mut WalInner) {
        let victims: Vec<u64> = inner
            .segments
            .iter()
            .filter(|(_, s)| s.sealed && s.outstanding == 0)
            .map(|(&seq, _)| seq)
            .collect();
        for seq in victims {
            inner.segments.remove(&seq);
            let _ = std::fs::remove_file(segment_path(dir, seq));
        }
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

/// Read a segment, appending valid records. A corrupt or truncated tail
/// (torn write from a crash) ends the segment silently. Returns the
/// record count.
fn replay_segment(
    path: &Path,
    seq: u64,
    out: &mut Vec<WalRecord>,
) -> std::io::Result<u64> {
    let mut data = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut data)?;
    let mut offset = 0usize;
    let mut count = 0u64;
    while offset + 8 <= data.len() {
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap());
        let start = offset + 8;
        let end = start + len;
        if end > data.len() || len < 2 {
            break; // torn tail
        }
        let payload = &data[start..end];
        if crc32fast::hash(payload) != crc {
            break; // corruption — stop replaying this segment
        }
        let stream_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
        if 2 + stream_len > payload.len() {
            break;
        }
        let stream = String::from_utf8_lossy(&payload[2..2 + stream_len]).to_string();
        let doc = payload[2 + stream_len..].to_vec();
        out.push(WalRecord {
            stream,
            doc,
            pos: WalPos { segment: seq },
        });
        count += 1;
        offset = end;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(n: usize, stream: &str) -> Vec<(String, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    stream.to_string(),
                    format!("{{\"n\":{i}}}").into_bytes(),
                )
            })
            .collect()
    }

    #[test]
    fn append_confirm_deletes_sealed_segments() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segments force rotation quickly.
        let (wal, replayed) = Wal::open(dir.path(), 128).unwrap();
        assert!(replayed.is_empty());

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
        assert_eq!(replayed.len(), 800);
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

        let (_, replayed) = Wal::open(dir.path(), 1 << 20).unwrap();
        // Last record is torn; first two survive.
        assert_eq!(replayed.len(), 2);
    }
}
