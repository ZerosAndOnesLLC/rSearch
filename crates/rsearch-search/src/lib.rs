#![warn(missing_docs)]
//! Search path: OpenSearch query-DSL subset translated onto Tantivy,
//! executed across published splits, merged into ES-shaped responses.

mod error;
mod executor;
pub mod logql;
mod query_dsl;

pub use error::{SearchError, SearchResult};
pub use executor::{FoundDocument, SearchRequest, SearchService};
pub use query_dsl::{extract_time_bounds, rewrite_agg_fields, translate_query};
