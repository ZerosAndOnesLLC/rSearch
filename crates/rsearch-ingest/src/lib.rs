#![warn(missing_docs)]
//! Ingest path: `_bulk` parsing, append-before-ack WAL, and the per-stream
//! indexer pipeline that turns buffered documents into published splits.

mod bulk;
mod gelf;
mod inputs;
mod error;
mod pipeline;
mod syslog;
mod wal;

pub use gelf::parse_gelf;
pub use inputs::spawn_inputs;
pub use syslog::parse_syslog;
pub use bulk::{BulkAction, BulkItem, BulkParseOutcome, parse_bulk_body};
pub use error::{IngestError, IngestResult};
pub use pipeline::{IngestPipeline, PipelineConfig, SeqClock};
pub use wal::{Wal, WalItem, WalPos, WalRecord, WalReplay};
