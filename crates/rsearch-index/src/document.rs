use std::net::IpAddr;

use tantivy::TantivyDocument;
use tantivy::time::OffsetDateTime;
use tantivy::time::format_description::well_known::Rfc3339;

use crate::error::{IndexError, IndexResult};
use crate::mapping::{FieldType, MappedSchema};

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
/// Accepts RFC 3339 strings, epoch seconds, or epoch milliseconds
/// (numbers >= 1e12 are treated as milliseconds).
pub fn extract_timestamp(doc: &serde_json::Value) -> Option<tantivy::DateTime> {
    let value = doc.get("@timestamp").or_else(|| doc.get("timestamp"))?;
    parse_timestamp(value)
}

/// Widest epoch-millis range tantivy's nanosecond representation holds
/// (~year 1677 to ~2262). Out-of-range inputs clamp instead of panicking.
const MAX_SAFE_MILLIS: i64 = i64::MAX / 1_000_000;

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
        serde_json::Value::String(s) => OffsetDateTime::parse(s, &Rfc3339)
            .ok()
            .map(tantivy::DateTime::from_utc),
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
}

impl DocumentConverter {
    /// Create a converter for the given schema.
    pub fn new(schema: MappedSchema) -> Self {
        Self { schema }
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
            match self.schema.fields.get(&key) {
                Some((field, ty)) => {
                    for item in flatten(&value) {
                        add_typed(&mut out, *field, *ty, item);
                    }
                }
                None => {
                    dynamic.insert(key, value);
                }
            }
        }
        if !dynamic.is_empty() {
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

/// Arrays index each element; everything else is a single value.
fn flatten(value: &serde_json::Value) -> Vec<&serde_json::Value> {
    match value {
        serde_json::Value::Array(items) => items.iter().collect(),
        other => vec![other],
    }
}

/// Best-effort coercion in the ES spirit: values that don't fit the mapped
/// type are dropped rather than failing the whole document.
fn add_typed(
    out: &mut TantivyDocument,
    field: tantivy::schema::Field,
    ty: FieldType,
    value: &serde_json::Value,
) {
    match ty {
        FieldType::Keyword | FieldType::Text => {
            if let Some(s) = value.as_str() {
                out.add_text(field, s);
            } else if !value.is_null() {
                out.add_text(field, value.to_string());
            }
        }
        FieldType::Long => {
            if let Some(i) = value.as_i64() {
                out.add_i64(field, i);
            } else if let Some(s) = value.as_str()
                && let Ok(i) = s.parse::<i64>()
            {
                out.add_i64(field, i);
            }
        }
        FieldType::Double => {
            if let Some(f) = value.as_f64() {
                out.add_f64(field, f);
            } else if let Some(s) = value.as_str()
                && let Ok(f) = s.parse::<f64>()
            {
                out.add_f64(field, f);
            }
        }
        FieldType::Boolean => {
            if let Some(b) = value.as_bool() {
                out.add_bool(field, b);
            } else if let Some(s) = value.as_str()
                && let Ok(b) = s.parse::<bool>()
            {
                out.add_bool(field, b);
            }
        }
        FieldType::Date => {
            if let Some(ts) = parse_timestamp(value) {
                out.add_date(field, ts);
            }
        }
        FieldType::Ip => {
            if let Some(s) = value.as_str()
                && let Ok(ip) = s.parse::<IpAddr>()
            {
                let ipv6 = match ip {
                    IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                    IpAddr::V6(v6) => v6,
                };
                out.add_ip_addr(field, ipv6);
            }
        }
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
        // _source + _timestamp + 4 mapped + _dynamic
        assert!(doc.field_values().count() >= 6);
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
