#![warn(missing_docs)]
//! Index engine: ES-style mappings translated onto Tantivy schemas, and
//! immutable split files built from batches of log documents.

mod builder;
mod cache;
mod document;
mod dynamic_paths;
mod error;
mod exclusions;
mod mapping;
mod reader;
mod split_file;
mod tokenizer;

pub use builder::{PackagedSplit, SplitBuilder};
pub use dynamic_paths::dynamic_string_paths;
pub use tantivy::DateTime;
pub use cache::SplitCache;
pub use reader::{ReadDoc, SplitReader};
pub use document::{
    DocIdentity, DocumentConverter, MAX_SAFE_MILLIS, epoch_to_millis, extract_timestamp,
};
pub use error::{IndexError, IndexResult};
pub use exclusions::{ExcludeDocsQuery, ExclusionSet, Tombstone};
pub use mapping::{
    CURRENT_SCHEMA_VERSION, DYNAMIC_FIELD, DYNAMIC_RAW_FIELD, FieldType, ID_FIELD, IndexMapping,
    KEYWORD_IGNORE_ABOVE, KEYWORD_SUBFIELD, MappedSchema, SEQ_FIELD, SOURCE_FIELD,
    TIMESTAMP_FIELD,
};
pub use split_file::{BundleMeta, FOOTER_TAIL_LEN, FileSpan, SplitMeta, parse_footer_tail, parse_meta};
pub use tokenizer::{STANDARD_TOKENIZER, register_tokenizers, standard_analyzer};
