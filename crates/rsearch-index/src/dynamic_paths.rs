//! Discovery of the JSON paths present in the `_dynamic` field.
//!
//! A bare `query_string` must search every field, but Tantivy's parser
//! cannot fan a term across the paths of a JSON field — each path has to
//! be named explicitly (issue #42). This module lists the paths a segment
//! actually contains, with the value types seen under each, so the query
//! layer can do that and `GET /{index}/_mapping` can report unmapped
//! fields the way OpenSearch's dynamic mapping does (issue #76).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, Type};
use tantivy::{Searcher, TantivyError};

/// Byte layout of a JSON field's term-dictionary keys
/// (`tantivy_common::json_path_writer`):
/// `[path segments, separated by 0x01][0x00][value type code][value bytes]`.
const JSON_PATH_SEGMENT_SEP: u8 = 1;
const JSON_END_OF_PATH: u8 = 0;

/// A JSON value type seen under a dynamic path, named the way the
/// mapping reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DynamicType {
    /// A JSON string: `text` with a `.keyword` sub-field in the mapping.
    String,
    /// A JSON integer (Tantivy i64 or u64 term).
    Long,
    /// A JSON number with a fraction.
    Double,
    /// A JSON boolean.
    Boolean,
    /// A date term (never written by rSearch's converter today; reported
    /// faithfully if a split holds one).
    Date,
}

impl DynamicType {
    /// The Tantivy term-type codes that map onto this type.
    fn codes(self) -> &'static [Type] {
        match self {
            DynamicType::String => &[Type::Str],
            DynamicType::Long => &[Type::I64, Type::U64],
            DynamicType::Double => &[Type::F64],
            DynamicType::Boolean => &[Type::Bool],
            DynamicType::Date => &[Type::Date],
        }
    }

    const ALL: [DynamicType; 5] = [
        DynamicType::String,
        DynamicType::Long,
        DynamicType::Double,
        DynamicType::Boolean,
        DynamicType::Date,
    ];
}

/// Dynamic path (query-parser form) → the value types seen under it.
pub type DynamicFields = BTreeMap<String, BTreeSet<DynamicType>>;

/// List every JSON path in `dynamic` with the value types it holds, in
/// query-parser form: segments joined with `.`, literal dots escaped as
/// `\.`. Sorted and deduplicated across segments.
///
/// Skip-scans the term dictionary: one seek per (path, type probe) pair
/// rather than a walk of every term, so cost is proportional to the
/// number of distinct paths, not the number of terms. Call from a
/// blocking context (term dictionaries may be fetched from storage on
/// first touch).
pub fn dynamic_field_types(searcher: &Searcher, dynamic: Field) -> tantivy::Result<DynamicFields> {
    let mut paths: DynamicFields = BTreeMap::new();
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
            // the first term seen tells only one type — probe for each
            // type's block directly.
            let mut probe = path.clone();
            probe.push(JSON_END_OF_PATH);
            let probe_len = probe.len();
            for ty in DynamicType::ALL {
                for code in ty.codes() {
                    probe.truncate(probe_len);
                    probe.push(code.to_code());
                    let mut probe_stream = dict.range().ge(&probe).into_stream()?;
                    if probe_stream.advance() && probe_stream.key().starts_with(&probe) {
                        paths.entry(decode_path(&path)).or_default().insert(ty);
                        break;
                    }
                }
            }
            // 0x00 (end-of-path) sorts below 0x01, so this bound skips the
            // rest of this path's terms while keeping nested child paths
            // (`path ++ 0x01 ++ …`) in range for the next iteration.
            let mut next = path;
            next.push(JSON_PATH_SEGMENT_SEP);
            bound = Some(next);
        }
    }
    Ok(paths)
}

/// The paths holding at least one string term (see
/// [`dynamic_field_types`]).
pub fn dynamic_string_paths(searcher: &Searcher, dynamic: Field) -> tantivy::Result<Vec<String>> {
    Ok(string_paths(&dynamic_field_types(searcher, dynamic)?))
}

/// The string-valued paths of a [`DynamicFields`] inventory.
pub fn string_paths(fields: &DynamicFields) -> Vec<String> {
    fields
        .iter()
        .filter(|(_, types)| types.contains(&DynamicType::String))
        .map(|(path, _)| path.clone())
        .collect()
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

    fn index_with(docs: &[serde_json::Value]) -> (MappedSchema, tantivy::Index) {
        let schema = MappedSchema::build(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {"message": {"type": "text"}}
            }))
            .unwrap(),
        );
        let index = schema.create_in_ram();
        let mut writer = index.writer(15_000_000).unwrap();
        let converter = crate::document::DocumentConverter::new(schema.clone());
        for doc in docs {
            let (doc, _) = converter
                .convert(doc.clone(), tantivy::DateTime::from_timestamp_millis(0))
                .unwrap();
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
        (schema, index)
    }

    #[test]
    fn lists_string_paths_only() {
        let (schema, index) = index_with(&[
            serde_json::json!({"message": "mapped stays out", "level": "info", "count": 7}),
            serde_json::json!({"ctx": {"job": "cleanup"}, "log.level": "warn"}),
        ]);
        let reader = index.reader().unwrap();
        let paths = dynamic_string_paths(&reader.searcher(), schema.dynamic).unwrap();
        // `count` is numeric-only; `message` is mapped, not dynamic.
        assert_eq!(paths, vec!["ctx.job", "level", "log\\.level"]);
    }

    #[test]
    fn reports_every_type_under_a_path() {
        let (schema, index) = index_with(&[
            serde_json::json!({"n": 7, "f": 1.5, "ok": true, "s": "x", "mixed": "a"}),
            serde_json::json!({"n": -1, "mixed": 3, "neg": -5}),
        ]);
        let reader = index.reader().unwrap();
        let fields = dynamic_field_types(&reader.searcher(), schema.dynamic).unwrap();
        let types = |p: &str| fields[p].iter().copied().collect::<Vec<_>>();
        assert_eq!(types("n"), vec![DynamicType::Long]);
        assert_eq!(types("neg"), vec![DynamicType::Long]);
        assert_eq!(types("f"), vec![DynamicType::Double]);
        assert_eq!(types("ok"), vec![DynamicType::Boolean]);
        assert_eq!(types("s"), vec![DynamicType::String]);
        assert_eq!(types("mixed"), vec![DynamicType::String, DynamicType::Long]);
        assert_eq!(fields.len(), 6);
        assert_eq!(string_paths(&fields), vec!["mixed", "s"]);
    }
}
