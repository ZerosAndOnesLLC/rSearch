//! Ingest path: `_bulk` parsing, append-before-ack WAL, and the per-stream
//! indexer pipeline that turns buffered documents into published splits.

mod bulk;
mod error;
mod pipeline;
mod wal;

pub use bulk::{BulkAction, BulkItem, BulkParseOutcome, parse_bulk_body};
pub use error::{IngestError, IngestResult};
pub use pipeline::{IngestPipeline, PipelineConfig};
pub use wal::{Wal, WalPos, WalRecord};
