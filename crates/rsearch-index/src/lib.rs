//! Index engine: ES-style mappings translated onto Tantivy schemas, and
//! immutable split files built from batches of log documents.

mod builder;
mod cache;
mod document;
mod error;
mod mapping;
mod reader;
mod split_file;

pub use builder::{PackagedSplit, SplitBuilder};
pub use tantivy::DateTime;
pub use cache::SplitCache;
pub use reader::SplitReader;
pub use document::{DocumentConverter, extract_timestamp};
pub use error::{IndexError, IndexResult};
pub use mapping::{FieldType, IndexMapping, MappedSchema};
pub use split_file::{BundleMeta, FOOTER_TAIL_LEN, FileSpan, SplitMeta, parse_footer_tail, parse_meta};
