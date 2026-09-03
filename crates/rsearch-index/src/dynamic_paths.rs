//! Discovery of the JSON paths present in the `_dynamic` field.
//!
//! A bare `query_string` must search every field, but Tantivy's parser
//! cannot fan a term across the paths of a JSON field — each path has to
//! be named explicitly (issue #42). This module lists the string-valued
//! paths a segment actually contains so the query layer can do that.

use std::collections::BTreeSet;

use tantivy::schema::{Field, Type};
use tantivy::{Searcher, TantivyError};

/// Byte layout of a JSON field's term-dictionary keys
/// (`tantivy_common::json_path_writer`):
/// `[path segments, separated by 0x01][0x00][value type code][value bytes]`.
const JSON_PATH_SEGMENT_SEP: u8 = 1;
const JSON_END_OF_PATH: u8 = 0;

/// List every JSON path in `dynamic` that holds at least one string term,
/// in query-parser form: segments joined with `.`, literal dots escaped
/// as `\.`. Sorted and deduplicated across segments.
///
/// Skip-scans the term dictionary: one seek per (path, probe) pair rather
/// than a walk of every term, so cost is proportional to the number of
/// distinct paths, not the number of terms. Call from a blocking context
/// (term dictionaries may be fetched from storage on first touch).
pub fn dynamic_string_paths(
    searcher: &Searcher,
    dynamic: Field,
) -> tantivy::Result<Vec<String>> {
    let mut paths = BTreeSet::new();
    for segment in searcher.segment_readers() {
        let inverted = segment.inverted_index(dynamic)?;
        let dict = inverted.terms();
        let mut bound: Option<Vec<u8>> = None;
        loop {
            let mut stream = match &bound {
                None => dict.stream()?,
                Some(b) => dict.range().ge(b).into_stream()?,
            };
            if !stream.advance() {
                break;
            }
            let key = stream.key();
            let Some(end) = key.iter().position(|&b| b == JSON_END_OF_PATH) else {
                return Err(TantivyError::InternalError(format!(
                    "malformed JSON term key in field {dynamic:?}: no end-of-path byte"
                )));
            };
            let path = key[..end].to_vec();
            drop(stream);
            // Terms of one path sort by value type code after the 0x00, so
            // the first term seen may be numeric while string terms for the
            // same path sit further on — probe for the string block directly.
            let mut probe = path.clone();
            probe.push(JSON_END_OF_PATH);
            probe.push(Type::Str.to_code());
            let mut probe_stream = dict.range().ge(&probe).into_stream()?;
            if probe_stream.advance() && probe_stream.key().starts_with(&probe) {
                paths.insert(decode_path(&path));
            }
            drop(probe_stream);
            // 0x00 (end-of-path) sorts below 0x01, so this bound skips the
            // rest of this path's terms while keeping nested child paths
            // (`path ++ 0x01 ++ …`) in range for the next iteration.
            let mut next = path;
            next.push(JSON_PATH_SEGMENT_SEP);
            bound = Some(next);
        }
    }
    Ok(paths.into_iter().collect())
}

/// Encoded path bytes → query-parser path: segments joined with `.`,
/// literal dots inside a segment escaped (`_dynamic` does not expand
/// dots, so `log.level` as one key must be queried as `log\.level`).
fn decode_path(bytes: &[u8]) -> String {
    bytes
        .split(|&b| b == JSON_PATH_SEGMENT_SEP)
        .map(|segment| String::from_utf8_lossy(segment).replace('.', "\\."))
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{IndexMapping, MappedSchema};

    #[test]
    fn lists_string_paths_only() {
        let schema = MappedSchema::build(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {"message": {"type": "text"}}
            }))
            .unwrap(),
        );
        let index = schema.create_in_ram();
        let mut writer = index.writer(15_000_000).unwrap();
        let converter = crate::document::DocumentConverter::new(schema.clone());
        for doc in [
            serde_json::json!({"message": "mapped stays out", "level": "info", "count": 7}),
            serde_json::json!({"ctx": {"job": "cleanup"}, "log.level": "warn"}),
        ] {
            let (doc, _) = converter
                .convert(doc, tantivy::DateTime::from_timestamp_millis(0))
                .unwrap();
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let paths = dynamic_string_paths(&reader.searcher(), schema.dynamic).unwrap();
        // `count` is numeric-only; `message` is mapped, not dynamic.
        assert_eq!(paths, vec!["ctx.job", "level", "log\\.level"]);
    }
}
