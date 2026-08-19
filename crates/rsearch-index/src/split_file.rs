//! The split bundle format: every file of a sealed Tantivy index
//! concatenated into one immutable object, followed by a JSON metadata
//! footer so readers can open a split from its tail without downloading
//! the whole object:
//!
//! ```text
//! [file bytes...][file bytes...][meta JSON][meta len: u64 LE][RSPLIT01]
//! ```

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{IndexError, IndexResult};

pub const SPLIT_MAGIC: &[u8; 8] = b"RSPLIT01";
/// Footer tail = meta length (8 bytes LE) + magic (8 bytes).
pub const FOOTER_TAIL_LEN: u64 = 16;

/// Location of one bundled file within the split object.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileSpan {
    /// Byte offset of the file within the split object.
    pub offset: u64,
    /// File length in bytes.
    pub len: u64,
}

/// Descriptive metadata for a split, stored in the footer and mirrored in
/// the metastore when the split is published.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitMeta {
    /// Unique split identifier (UUID, simple format).
    pub split_id: String,
    /// Stream the split's documents belong to.
    pub stream: String,
    /// Number of documents in the split.
    pub doc_count: u64,
    /// Inclusive document-timestamp range, epoch milliseconds.
    pub time_start_millis: i64,
    /// Upper end of the inclusive timestamp range, epoch millis.
    pub time_end_millis: i64,
    /// The index mapping the split was built with (ES mapping shape).
    pub mapping: serde_json::Value,
    /// Schema layout version (see `mapping::CURRENT_SCHEMA_VERSION`);
    /// absent in footers written before `_id`/`_seq` existed.
    #[serde(default)]
    pub schema_version: u32,
    /// Lowest `_seq` in the split; None when documents carry no ids.
    #[serde(default)]
    pub seq_min: Option<i64>,
    /// Highest `_seq` in the split; None when documents carry no ids.
    #[serde(default)]
    pub seq_max: Option<i64>,
}

/// Full footer contents: bundled file map + split metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleMeta {
    /// Bundled file name -> its byte span within the object.
    pub files: BTreeMap<String, FileSpan>,
    /// Descriptive split metadata.
    pub split: SplitMeta,
}

/// Bundle every file in `index_dir` plus the footer into `out`, streaming
/// file contents. Returns the finished bundle metadata.
pub fn write_bundle(
    index_dir: &Path,
    out: &mut (impl Write + ?Sized),
    split: SplitMeta,
) -> IndexResult<BundleMeta> {
    let mut names: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(index_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        // Writer lock files are transient state, not index data.
        if name.ends_with(".lock") {
            continue;
        }
        names.push(name);
    }
    names.sort();

    let mut files = BTreeMap::new();
    let mut offset = 0u64;
    let mut buf = vec![0u8; 1 << 20];
    for name in names {
        let path = index_dir.join(&name);
        let mut file = std::fs::File::open(&path)?;
        let mut written = 0u64;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n])?;
            written += n as u64;
        }
        files.insert(
            name,
            FileSpan {
                offset,
                len: written,
            },
        );
        offset += written;
    }

    let meta = BundleMeta { files, split };
    let meta_json = serde_json::to_vec(&meta)
        .map_err(|e| IndexError::InvalidDocument(format!("serializing split meta: {e}")))?;
    out.write_all(&meta_json)?;
    out.write_all(&(meta_json.len() as u64).to_le_bytes())?;
    out.write_all(SPLIT_MAGIC)?;
    Ok(meta)
}

/// Parse the 16-byte footer tail; returns the metadata JSON length.
pub fn parse_footer_tail(tail: &[u8]) -> IndexResult<u64> {
    if tail.len() != FOOTER_TAIL_LEN as usize {
        return Err(IndexError::InvalidDocument(format!(
            "footer tail must be {FOOTER_TAIL_LEN} bytes, got {}",
            tail.len()
        )));
    }
    if &tail[8..16] != SPLIT_MAGIC {
        return Err(IndexError::InvalidDocument(
            "not a split file (bad magic)".to_string(),
        ));
    }
    Ok(u64::from_le_bytes(tail[0..8].try_into().unwrap()))
}

/// Parse the metadata JSON section.
pub fn parse_meta(meta_json: &[u8]) -> IndexResult<BundleMeta> {
    serde_json::from_slice(meta_json)
        .map_err(|e| IndexError::InvalidDocument(format!("parsing split meta: {e}")))
}
