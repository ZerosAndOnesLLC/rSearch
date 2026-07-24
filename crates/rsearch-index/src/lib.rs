//! Index engine: ES-style mappings translated onto Tantivy schemas, and
//! immutable split files built from batches of log documents.

mod document;
mod error;
mod mapping;

pub use document::{DocumentConverter, extract_timestamp};
pub use error::{IndexError, IndexResult};
pub use mapping::{FieldType, IndexMapping, MappedSchema};
