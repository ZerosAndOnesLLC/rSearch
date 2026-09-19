use std::net::IpAddr;

use tantivy::TantivyDocument;

use crate::date::{DEFAULT_DATE_FORMAT, parse_date_string};
use crate::error::{IndexError, IndexResult};
use crate::mapping::{FieldDef, FieldType, KEYWORD_IGNORE_ABOVE, MappedField, MappedSchema};

/// A document's identity within its stream: the `_id` (client-supplied or
/// generated) and the write sequence stamp that orders versions of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocIdentity {
    /// The document id (`_id`).
    pub id: String,
    /// Node-local monotonic write stamp (micros since epoch); `0` for
    /// documents whose write predates sequence tracking.
    pub seq: i64,
}

impl DocIdentity {
    /// Identity with the given id and sequence.
    pub fn new(id: impl Into<String>, seq: i64) -> Self {
        Self { id: id.into(), seq }
    }

    /// A fresh UUID id at sequence 0.
    pub fn generated() -> Self {
        Self::new(uuid::Uuid::new_v4().simple().to_string(), 0)
    }
}

/// Extract a document timestamp from `@timestamp` or `timestamp` fields.
/// Accepts every `strict_date_optional_time` form, epoch seconds, or
/// epoch milliseconds (numbers >= 1e12 are treated as milliseconds).
pub fn extract_timestamp(doc: &serde_json::Value) -> Option<tantivy::DateTime> {
    let value = doc.get("@timestamp").or_else(|| doc.get("timestamp"))?;
    parse_timestamp(value)
}

/// Widest epoch-millis range tantivy's nanosecond representation holds
/// (~year 1677 to ~2262). Out-of-range inputs clamp instead of panicking.
pub const MAX_SAFE_MILLIS: i64 = i64::MAX / 1_000_000;

/// Normalize an epoch number of unknown unit (secs, millis, micros, or
/// nanos — shippers send all four) to clamped epoch milliseconds.
pub fn epoch_to_millis(value: i64) -> i64 {
    // Every branch flows through the clamp: tantivy multiplies millis by
    // 1e6 to reach nanos, so anything past MAX_SAFE_MILLIS overflows i64
    // there — a debug-build panic, silent garbage timestamps in release.
    // The micros branch used to skip it (#22).
    let millis = match value.unsigned_abs() {
        0..=99_999_999_999 => value.saturating_mul(1000),  // seconds (to ~5138 AD)
        100_000_000_000..=99_999_999_999_999 => value,     // millis
        100_000_000_000_000..=99_999_999_999_999_999 => value / 1_000, // micros
        _ => value / 1_000_000,                            // nanos
    };
    millis.clamp(-MAX_SAFE_MILLIS, MAX_SAFE_MILLIS)
}

fn parse_timestamp(value: &serde_json::Value) -> Option<tantivy::DateTime> {
    match value {
        // The whole `strict_date_optional_time||epoch_millis` grammar,
        // not just RFC 3339 (issue #86).
        serde_json::Value::String(s) => {
            parse_date_string(s).map(tantivy::DateTime::from_timestamp_millis)
        }
        serde_json::Value::Number(n) => {
            let millis = if let Some(i) = n.as_i64() {
                epoch_to_millis(i)
            } else {
                let f = n.as_f64()?;
                if !f.is_finite() {
                    return None;
                }
                // Floats follow the same unit heuristic (GELF sends
                // fractional seconds).
                if f.abs() < 100_000_000_000.0 {
                    ((f * 1000.0) as i64).clamp(-MAX_SAFE_MILLIS, MAX_SAFE_MILLIS)
                } else {
                    epoch_to_millis(f as i64)
                }
            };
            Some(tantivy::DateTime::from_timestamp_millis(millis))
        }
        _ => None,
    }
}

/// Converts raw JSON log documents into Tantivy documents according to a
/// [`MappedSchema`]: mapped fields are indexed with their declared type,
/// everything else lands in the `_dynamic` JSON field, and the original
/// document is stored verbatim in `_source`.
pub struct DocumentConverter {
    schema: MappedSchema,
    /// Values dropped because a mapped field could not parse them, since
    /// this converter was created (issue #86). Only a log-mode index ever
    /// gets here with one: a document-mode write is refused up front.
    malformed_dropped: std::sync::atomic::AtomicU64,
}

impl DocumentConverter {
    /// Create a converter for the given schema.
    pub fn new(schema: MappedSchema) -> Self {
        Self { schema, malformed_dropped: std::sync::atomic::AtomicU64::new(0) }
    }

    /// How many values this converter has dropped as unparseable.
    pub fn malformed_dropped(&self) -> u64 {
        self.malformed_dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The schema documents are converted against.
    pub fn schema(&self) -> &MappedSchema {
        &self.schema
    }

    /// Convert one document, deriving the stored `_source` from the doc.
    /// Identity is a fresh generated id at sequence 0 (tests, legacy
    /// re-index paths).
    pub fn convert(
        &self,
        doc: serde_json::Value,
        fallback_timestamp: tantivy::DateTime,
    ) -> IndexResult<(TantivyDocument, tantivy::DateTime)> {
        let id = DocIdentity::generated();
        self.convert_with_source(doc, None, &id, fallback_timestamp)
    }

    /// Convert one document. `source` is the exact bytes to store as
    /// `_source`; when `None` the doc is serialized. Passing the client's
    /// original NDJSON line avoids a redundant re-serialization on the
    /// ingest hot path. `fallback_timestamp` is used when the document
    /// carries no parseable `@timestamp`/`timestamp`. Returns the
    /// converted document and its effective timestamp.
    ///
    /// Takes the document by value: unmapped fields (most of a typical log
    /// line) are moved into `_dynamic` rather than deep-cloned.
    pub fn convert_with_source(
        &self,
        doc: serde_json::Value,
        source: Option<&str>,
        identity: &DocIdentity,
        fallback_timestamp: tantivy::DateTime,
    ) -> IndexResult<(TantivyDocument, tantivy::DateTime)> {
        let timestamp = extract_timestamp(&doc).unwrap_or(fallback_timestamp);
        // `_source` must be serialized before the fields are moved out.
        let serialized = match source {
            Some(_) => None,
            None => Some(doc.to_string()),
        };
        let obj = match doc {
            serde_json::Value::Object(obj) => obj,
            _ => return Err(IndexError::InvalidDocument("document must be an object".into())),
        };

        let mut out = TantivyDocument::new();
        out.add_date(self.schema.timestamp, timestamp);
        if let (Some(id_field), Some(seq_field)) = (self.schema.id, self.schema.seq) {
            out.add_text(id_field, &identity.id);
            out.add_i64(seq_field, identity.seq);
        }
        match (source, serialized) {
            (Some(source), _) => out.add_text(self.schema.source, source),
            (None, Some(serialized)) => out.add_text(self.schema.source, serialized),
            (None, None) => unreachable!("serialized computed for source: None"),
        }

        let mut dynamic = serde_json::Map::new();
        for (key, value) in obj {
            match self.schema.mapping.properties.get(&key) {
                // A mapped field indexes into its own column and into
                // every declared multi-field (issue #85). A value the
                // field cannot parse is dropped here — the write path
                // has already refused it on a document-mode index
                // (issue #86), and a log-mode index keeps ingesting.
                Some(def) => {
                    let dropped = index_mapped(&mut out, &self.schema, &key, def, &value);
                    if dropped > 0 {
                        self.malformed_dropped
                            .fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                None => {
                    dynamic.insert(key, value);
                }
            }
        }
        if !dynamic.is_empty() {
            // The `.keyword` view: exact string values, with OpenSearch's
            // dynamic `ignore_above` applied. Built before `dynamic` is
            // moved into the tokenized field.
            if let Some(raw_field) = self.schema.dynamic_raw
                && let Some(raw) = keyword_projection(&dynamic)
            {
                out.add_object(
                    raw_field,
                    raw.into_iter()
                        .map(|(k, v)| (k, tantivy::schema::OwnedValue::from(v)))
                        .collect(),
                );
            }
            out.add_object(
                self.schema.dynamic,
                dynamic
                    .into_iter()
                    .map(|(k, v)| (k, tantivy::schema::OwnedValue::from(v)))
                    .collect(),
            );
        }
        Ok((out, timestamp))
    }
}

/// The subset of `obj` that gets a `.keyword` value: string leaves of at
/// most [`KEYWORD_IGNORE_ABOVE`] characters, keeping their object/array
/// nesting so the JSON path is the same as in `_dynamic`. None when no
/// string qualifies.
fn keyword_projection(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let projected: serde_json::Map<String, serde_json::Value> = obj
        .iter()
        .filter_map(|(k, v)| keyword_value(v).map(|v| (k.clone(), v)))
        .collect();
    (!projected.is_empty()).then_some(projected)
}

fn keyword_value(value: &serde_json::Value) -> Option<serde_json::Value> {
    match value {
        serde_json::Value::String(s) => {
            (s.chars().count() <= KEYWORD_IGNORE_ABOVE).then(|| value.clone())
        }
        serde_json::Value::Object(map) => keyword_projection(map).map(serde_json::Value::Object),
        serde_json::Value::Array(items) => {
            let kept: Vec<serde_json::Value> = items.iter().filter_map(keyword_value).collect();
            (!kept.is_empty()).then_some(serde_json::Value::Array(kept))
        }
        _ => None,
    }
}

/// Arrays index each element; everything else is a single value.
fn flatten(value: &serde_json::Value) -> Vec<&serde_json::Value> {
    match value {
        serde_json::Value::Array(items) => items.iter().collect(),
        other => vec![other],
    }
}

/// A value parsed against a mapped field, ready to add to the document.
enum Indexed<'a> {
    Str(std::borrow::Cow<'a, str>),
    I64(i64),
    F64(f64),
    Bool(bool),
    Date(tantivy::DateTime),
    Ip(std::net::Ipv6Addr),
}

/// Why a value could not be indexed into a mapped field, in OpenSearch's
/// wording — it becomes the `caused_by` reason of the write's
/// `mapper_parsing_exception` (issue #86).
struct Malformed(String);

/// Index one mapped value into `path`'s column and, recursively, into
/// every multi-field declared beneath it. Returns how many values were
/// dropped because the field could not parse them — on a log-mode index
/// that is the documented behaviour, and the count is what the flush
/// reports (a document-mode write never reaches here with one, see
/// [`MappedSchema::validate_document`]).
fn index_mapped(
    out: &mut TantivyDocument,
    schema: &MappedSchema,
    path: &str,
    def: &FieldDef,
    value: &serde_json::Value,
) -> u64 {
    let mut dropped = 0;
    if let Some(mapped) = schema.fields.get(path) {
        for item in flatten(value) {
            match parse_for(mapped, item) {
                Ok(Some(parsed)) => add_parsed(out, mapped, parsed),
                Ok(None) => {}
                // `ignore_malformed` is OpenSearch's own way to ask for
                // the value to be dropped, so it is not counted.
                Err(_) if mapped.ignore_malformed => {}
                Err(_) => dropped += 1,
            }
        }
    }
    for (name, sub) in &def.fields {
        let sub_path = format!("{path}.{name}");
        dropped += index_mapped(out, schema, &sub_path, sub, value);
    }
    dropped
}

/// Check one mapped value the way [`index_mapped`] would index it,
/// without building anything: the first value the field cannot parse is
/// the error the write reports.
fn validate_mapped(
    schema: &MappedSchema,
    path: &str,
    def: &FieldDef,
    value: &serde_json::Value,
) -> IndexResult<()> {
    if let Some(mapped) = schema.fields.get(path)
        && !mapped.ignore_malformed
    {
        for item in flatten(value) {
            if let Err(Malformed(reason)) = parse_for(mapped, item) {
                return Err(IndexError::MalformedField {
                    field: path.to_string(),
                    ty: mapped.ty.as_str(),
                    reason,
                    preview: preview(item),
                });
            }
        }
    }
    for (name, sub) in &def.fields {
        validate_mapped(schema, &format!("{path}.{name}"), sub, value)?;
    }
    Ok(())
}

/// The offending value as OpenSearch previews it in the error message.
fn preview(value: &serde_json::Value) -> String {
    match value.as_str() {
        Some(s) => s.to_string(),
        None => value.to_string(),
    }
}

fn add_parsed(out: &mut TantivyDocument, mapped: &MappedField, parsed: Indexed<'_>) {
    match parsed {
        Indexed::Str(text) => {
            // `ignore_above` applies to the value as declared, before
            // normalization, exactly as in OpenSearch.
            if mapped
                .ignore_above
                .is_some_and(|limit| text.chars().count() > limit)
            {
                return;
            }
            out.add_text(mapped.field, mapped.normalize(&text).as_ref());
        }
        Indexed::I64(v) => out.add_i64(mapped.field, v),
        Indexed::F64(v) => out.add_f64(mapped.field, v),
        Indexed::Bool(v) => out.add_bool(mapped.field, v),
        Indexed::Date(v) => out.add_date(mapped.field, v),
        Indexed::Ip(v) => out.add_ip_addr(mapped.field, v),
    }
}

/// Parse `value` for a mapped field the way OpenSearch's mappers do:
/// `Ok(None)` for a value that indexes nothing (`null`, and the empty
/// string every non-string mapper reads as null), `Err` for one that
/// fails the document unless `ignore_malformed` is set.
fn parse_for<'a>(
    mapped: &MappedField,
    value: &'a serde_json::Value,
) -> Result<Option<Indexed<'a>>, Malformed> {
    use serde_json::Value;
    if value.is_null() {
        return Ok(None);
    }
    match mapped.ty {
        FieldType::Keyword | FieldType::Text => match value {
            Value::String(s) => Ok(Some(Indexed::Str(std::borrow::Cow::Borrowed(s)))),
            Value::Number(_) | Value::Bool(_) => {
                Ok(Some(Indexed::Str(std::borrow::Cow::Owned(value.to_string()))))
            }
            _ => Err(Malformed("Can't get text on a START_OBJECT".to_string())),
        },
        FieldType::Long => match parse_number(value)? {
            Some(n) if n.fract() != 0.0 || n.abs() < i64::MAX as f64 => {
                Ok(Some(Indexed::I64(n.trunc() as i64)))
            }
            Some(n) => Err(Malformed(format!(
                "Numeric value ({n}) out of range of long"
            ))),
            None => Ok(None),
        },
        FieldType::Double => Ok(parse_number(value)?.map(Indexed::F64)),
        FieldType::Boolean => match value {
            Value::Bool(b) => Ok(Some(Indexed::Bool(*b))),
            Value::String(s) if s.is_empty() => Ok(None),
            Value::String(s) if s == "true" || s == "false" => {
                Ok(Some(Indexed::Bool(s == "true")))
            }
            Value::String(s) => Err(Malformed(format!(
                "Failed to parse value [{s}] as only [true] or [false] are allowed."
            ))),
            _ => Err(Malformed(format!(
                "Current token ({}) not of boolean type",
                token_name(value)
            ))),
        },
        FieldType::Date => match parse_timestamp(value) {
            Some(date) => Ok(Some(Indexed::Date(date))),
            None if value.as_str() == Some("") => {
                Err(Malformed("cannot parse empty date".to_string()))
            }
            None => Err(Malformed(format!(
                "failed to parse date field [{}] with format [{DEFAULT_DATE_FORMAT}]",
                preview(value)
            ))),
        },
        FieldType::Ip => match value.as_str() {
            Some(s) => match s.parse::<IpAddr>() {
                Ok(IpAddr::V4(v4)) => Ok(Some(Indexed::Ip(v4.to_ipv6_mapped()))),
                Ok(IpAddr::V6(v6)) => Ok(Some(Indexed::Ip(v6))),
                Err(_) => Err(Malformed(format!("'{s}' is not an IP string literal."))),
            },
            None => Err(Malformed(format!(
                "'{}' is not an IP string literal.",
                preview(value)
            ))),
        },
    }
}

/// A numeric mapper's view of a value: numbers and numeric strings are
/// accepted (OpenSearch's `coerce`, on by default), the empty string
/// reads as null, everything else fails the document.
fn parse_number(value: &serde_json::Value) -> Result<Option<f64>, Malformed> {
    match value {
        serde_json::Value::Number(n) => match n.as_f64() {
            Some(f) if f.is_finite() => Ok(Some(f)),
            _ => Err(Malformed(format!("Numeric value ({n}) out of range"))),
        },
        serde_json::Value::String(s) if s.is_empty() => Ok(None),
        serde_json::Value::String(s) => match s.parse::<f64>() {
            // A leading or trailing space is a parse failure in
            // OpenSearch; Rust's parser accepts neither, so the check is
            // the same one.
            Ok(f) if f.is_finite() => Ok(Some(f)),
            _ => Err(Malformed(format!("For input string: \"{s}\""))),
        },
        other => Err(Malformed(format!(
            "Current token ({}) not numeric, cannot use numeric value accessors",
            token_name(other)
        ))),
    }
}

/// Jackson's token name for a JSON value, as OpenSearch's messages use it.
fn token_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "VALUE_NULL",
        serde_json::Value::Bool(true) => "VALUE_TRUE",
        serde_json::Value::Bool(false) => "VALUE_FALSE",
        serde_json::Value::Number(n) if n.is_f64() => "VALUE_NUMBER_FLOAT",
        serde_json::Value::Number(_) => "VALUE_NUMBER_INT",
        serde_json::Value::String(_) => "VALUE_STRING",
        serde_json::Value::Array(_) => "START_ARRAY",
        serde_json::Value::Object(_) => "START_OBJECT",
    }
}

impl MappedSchema {
    /// Check a document against this mapping the way OpenSearch does on
    /// write: the first mapped field that cannot parse its value (and is
    /// not `ignore_malformed`) fails the whole document (issue #86).
    ///
    /// Document-mode writes run this before the WAL, so the client gets a
    /// per-item `mapper_parsing_exception` instead of a success that
    /// silently dropped a field.
    pub fn validate_document(&self, doc: &serde_json::Value) -> IndexResult<()> {
        if self.mapping.properties.is_empty() {
            return Ok(());
        }
        let Some(obj) = doc.as_object() else {
            return Err(IndexError::InvalidDocument("document must be an object".into()));
        };
        for (key, value) in obj {
            if let Some(def) = self.mapping.properties.get(key) {
                validate_mapped(self, key, def, value)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::IndexMapping;

    fn converter() -> DocumentConverter {
        let mapping = IndexMapping::from_json(&serde_json::json!({
            "properties": {
                "service": {"type": "keyword"},
                "message": {"type": "text"},
                "status": {"type": "long"},
                "client": {"type": "ip"},
            }
        }))
        .unwrap();
        DocumentConverter::new(MappedSchema::build(mapping))
    }

    fn fallback() -> tantivy::DateTime {
        tantivy::DateTime::from_timestamp_secs(1_700_000_000)
    }

    #[test]
    fn converts_mapped_and_dynamic_fields() {
        let c = converter();
        let (doc, ts) = c
            .convert(
                serde_json::json!({
                    "@timestamp": "2026-07-24T01:02:03Z",
                    "service": "api",
                    "message": "user login ok",
                    "status": 200,
                    "client": "10.1.2.3",
                    "extra_field": {"nested": "value"},
                }),
                fallback(),
            )
            .unwrap();
        assert_ne!(ts, fallback());
        // _source + _timestamp + 4 mapped + _dynamic + _dynamic_raw
        assert!(doc.field_values().count() >= 7);
    }

    /// Values a mapped field parses, in the shape a query would see them.
    fn indexed(mapping: serde_json::Value, doc: serde_json::Value) -> Vec<(String, String)> {
        let schema = MappedSchema::build(IndexMapping::from_json(&mapping).unwrap());
        let converter = DocumentConverter::new(schema.clone());
        let (tantivy_doc, _) = converter.convert(doc, fallback()).unwrap();
        let mut out = Vec::new();
        for (field, value) in tantivy_doc.field_values() {
            let name = schema.schema.get_field_name(field).to_string();
            if name.starts_with('_') {
                continue;
            }
            use tantivy::schema::Value as _;
            let rendered = match value.as_str() {
                Some(text) => text.to_string(),
                None => format!("{value:?}"),
            };
            out.push((name, rendered));
        }
        out.sort();
        out
    }

    #[test]
    fn multi_fields_index_every_declared_view() {
        let values = indexed(
            serde_json::json!({"properties": {"name": {"type": "text", "fields": {
                "keyword": {"type": "keyword"},
                "short": {"type": "keyword", "ignore_above": 5},
            }}}}),
            serde_json::json!({"name": "Kirsten Andersen"}),
        );
        let names: Vec<&str> = values.iter().map(|(n, _)| n.as_str()).collect();
        // The parent and the declared sub-field both hold the value; the
        // `ignore_above` view holds nothing (issue #85).
        assert_eq!(names, vec!["name", "name.keyword"]);
    }

    #[test]
    fn normalizer_applies_at_index_time() {
        let mapping = IndexMapping::from_request(
            &serde_json::json!({"properties": {"code": {"type": "keyword", "normalizer": "lower"}}}),
            Some(&serde_json::json!({
                "analysis": {"normalizer": {"lower": {"type": "custom", "filter": ["lowercase"]}}}
            })),
        )
        .unwrap();
        let values = indexed(mapping.to_json(), serde_json::json!({"code": "Kirsten Andersen"}));
        assert!(values[0].1.contains("kirsten andersen"), "{values:?}");
    }

    #[test]
    fn mapped_values_coerce_the_way_opensearch_does() {
        let mapping = serde_json::json!({"properties": {
            "n": {"type": "long"}, "f": {"type": "double"}, "b": {"type": "boolean"},
            "kw": {"type": "keyword"}, "d": {"type": "date"}, "ip": {"type": "ip"},
        }});
        let schema = MappedSchema::build(IndexMapping::from_json(&mapping).unwrap());
        let ok = |doc: serde_json::Value| schema.validate_document(&doc).is_ok();
        // Accepted, with coercion.
        assert!(ok(serde_json::json!({"n": "5"})));
        assert!(ok(serde_json::json!({"n": 5.7})));
        assert!(ok(serde_json::json!({"n": "5.7"})));
        assert!(ok(serde_json::json!({"f": "5.7"})));
        assert!(ok(serde_json::json!({"b": "true"})));
        assert!(ok(serde_json::json!({"kw": 5})));
        assert!(ok(serde_json::json!({"kw": true})));
        assert!(ok(serde_json::json!({"d": "2026-09-16"})));
        assert!(ok(serde_json::json!({"d": 1789578275562i64})));
        assert!(ok(serde_json::json!({"ip": "10.1.2.3"})));
        // The empty string reads as null for every non-string mapper.
        assert!(ok(serde_json::json!({"n": "", "f": "", "b": ""})));
        // Null and arrays of values.
        assert!(ok(serde_json::json!({"n": null, "kw": ["a", "b"]})));

        // Refused, the way OpenSearch refuses them.
        for bad in [
            serde_json::json!({"n": "abc"}),
            serde_json::json!({"n": " 5"}),
            serde_json::json!({"n": true}),
            serde_json::json!({"f": true}),
            serde_json::json!({"b": 1}),
            serde_json::json!({"b": "yes"}),
            serde_json::json!({"kw": {"a": 1}}),
            serde_json::json!({"d": "nonsense"}),
            serde_json::json!({"d": ""}),
            serde_json::json!({"d": "2026-09-16 00:00:00"}),
            serde_json::json!({"ip": "1.2.3"}),
        ] {
            assert!(schema.validate_document(&bad).is_err(), "{bad} should fail");
        }
    }

    #[test]
    fn malformed_values_report_the_opensearch_reason() {
        let schema = MappedSchema::build(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {"d": {"type": "date"}, "n": {"type": "long", "ignore_malformed": true}}
            }))
            .unwrap(),
        );
        let err = schema
            .validate_document(&serde_json::json!({"d": "nonsense"}))
            .unwrap_err();
        match err {
            IndexError::MalformedField { field, ty, reason, preview } => {
                assert_eq!(field, "d");
                assert_eq!(ty, "date");
                assert_eq!(
                    reason,
                    "failed to parse date field [nonsense] with format \
                     [strict_date_optional_time||epoch_millis]"
                );
                assert_eq!(preview, "nonsense");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        // `ignore_malformed` keeps OpenSearch's drop-the-value behaviour.
        assert!(schema.validate_document(&serde_json::json!({"n": "abc"})).is_ok());
        assert!(indexed(
            serde_json::json!({"properties": {"n": {"type": "long", "ignore_malformed": true}}}),
            serde_json::json!({"n": "abc"}),
        )
        .is_empty());
    }

    #[test]
    fn keyword_projection_keeps_short_strings_only() {
        let long = "x".repeat(KEYWORD_IGNORE_ABOVE + 1);
        let exact = "y".repeat(KEYWORD_IGNORE_ABOVE);
        let obj = serde_json::json!({
            "role": "tech_admin",
            "n": 1,
            "flag": true,
            "long": long,
            "exact": exact,
            "nested": {"city": "Austin", "count": 2, "deeper": {"zip": "78701"}},
            "tags": ["a", 7, {"k": "v"}, long, null],
            "numbers": [1, 2],
            "empty": {},
        });
        let projected = keyword_projection(obj.as_object().unwrap()).unwrap();
        assert_eq!(
            serde_json::Value::Object(projected),
            serde_json::json!({
                "role": "tech_admin",
                "exact": exact,
                "nested": {"city": "Austin", "deeper": {"zip": "78701"}},
                "tags": ["a", {"k": "v"}],
            })
        );
        assert!(keyword_projection(serde_json::json!({"n": 1}).as_object().unwrap()).is_none());
    }

    /// Legacy layouts have no `_dynamic_raw`; conversion must not touch it.
    #[test]
    fn legacy_schema_gets_no_raw_field() {
        let schema = MappedSchema::build_versioned(IndexMapping::default(), 1);
        assert!(schema.dynamic_raw.is_none());
        let c = DocumentConverter::new(schema);
        let (doc, _) = c.convert(serde_json::json!({"role": "tech_admin"}), fallback()).unwrap();
        // _source + _timestamp + _id + _seq + _dynamic
        assert_eq!(doc.field_values().count(), 5);
    }

    #[test]
    fn uses_fallback_when_timestamp_missing() {
        let c = converter();
        let (_, ts) = c
            .convert(serde_json::json!({"message": "no ts"}), fallback())
            .unwrap();
        assert_eq!(ts, fallback());
    }

    #[test]
    fn epoch_millis_and_secs_both_parse() {
        let millis = extract_timestamp(&serde_json::json!({"timestamp": 1_753_300_000_000_i64}))
            .unwrap();
        let secs = extract_timestamp(&serde_json::json!({"timestamp": 1_753_300_000})).unwrap();
        assert_eq!(millis, secs);
    }

    /// Every unit branch clamps to the tantivy-safe range; the micros
    /// branch used to leak values whose nanos representation overflows
    /// i64 (#22) — in debug builds `from_timestamp_millis` then panicked.
    #[test]
    fn out_of_range_epochs_clamp_in_every_unit() {
        for value in [
            99_999_999_999_i64,      // max seconds branch
            99_999_999_999_999,      // max millis branch
            99_999_999_999_999_999,  // max micros branch — the #22 overflow
            17_865_684_004_574_505,  // truncated-nanos garbage seen in the wild
            i64::MAX,                // max nanos branch
        ] {
            let millis = epoch_to_millis(value);
            assert!(millis <= MAX_SAFE_MILLIS, "{value} -> {millis} exceeds safe range");
            // The real failure mode: this multiply overflowed.
            let _ = tantivy::DateTime::from_timestamp_millis(millis);
            let neg = epoch_to_millis(-value);
            assert!(neg >= -MAX_SAFE_MILLIS, "-{value} -> {neg} exceeds safe range");
            let _ = tantivy::DateTime::from_timestamp_millis(neg);
        }
    }

    #[test]
    fn rejects_non_object_documents() {
        let c = converter();
        assert!(c.convert(serde_json::json!([1, 2]), fallback()).is_err());
    }

    #[test]
    fn arrays_index_each_element() {
        let c = converter();
        let (doc, _) = c
            .convert(
                serde_json::json!({"service": ["a", "b"], "status": [1, 2, 3]}),
                fallback(),
            )
            .unwrap();
        // 2 service + 3 status + _source + _timestamp + _id + _seq
        assert_eq!(doc.field_values().count(), 9);
    }
}
