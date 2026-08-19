use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tantivy::schema::{
    DateOptions, DateTimePrecision, FAST, Field, INDEXED, STORED, STRING, Schema, TEXT,
};

use crate::error::{IndexError, IndexResult};

/// Reserved stored `_source` field (the client's original document).
pub const SOURCE_FIELD: &str = "_source";
/// Reserved indexed+fast `_timestamp` field every document is sorted by.
pub const TIMESTAMP_FIELD: &str = "_timestamp";
/// Reserved JSON field unmapped keys are indexed under.
pub const DYNAMIC_FIELD: &str = "_dynamic";
/// Reserved document-id field (the client's `_id` or a generated UUID).
/// Present in splits with `schema_version >= 1`.
pub const ID_FIELD: &str = "_id";
/// Reserved write-sequence field: a node-local monotonic stamp (micros
/// since epoch) taken when the write was accepted. Orders versions of the
/// same `_id` and scopes tombstones. Present with `schema_version >= 1`.
pub const SEQ_FIELD: &str = "_seq";
/// Schema version written into new splits. `0` (or absent in the footer)
/// is the legacy layout without `_id`/`_seq`; `1` adds them after the
/// mapped fields so legacy field ordinals are unchanged.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Supported field types — the ES mapping subset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// Exact-match string (indexed untokenized, fast field).
    Keyword,
    /// Full-text string (tokenized, scored).
    Text,
    /// 64-bit signed integer (also covers ES integer/short/byte).
    Long,
    /// 64-bit float (also covers ES float/half_float).
    Double,
    /// Boolean flag.
    Boolean,
    /// Timestamp, indexed at millisecond precision.
    Date,
    /// IP address (v4 mapped to v6), range-queryable.
    Ip,
}

impl FieldType {
    fn parse(s: &str) -> IndexResult<Self> {
        match s {
            "keyword" => Ok(Self::Keyword),
            // Common ES numeric aliases collapse onto our two numerics.
            "long" | "integer" | "short" | "byte" => Ok(Self::Long),
            "double" | "float" | "half_float" => Ok(Self::Double),
            "text" => Ok(Self::Text),
            "boolean" => Ok(Self::Boolean),
            "date" => Ok(Self::Date),
            "ip" => Ok(Self::Ip),
            other => Err(IndexError::InvalidMapping(format!(
                "unsupported field type '{other}'"
            ))),
        }
    }
}

/// Parsed index mapping: explicit field definitions. Unmapped fields are
/// indexed dynamically under the `_dynamic` JSON field.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexMapping {
    /// Field name -> declared type for explicitly mapped fields.
    pub properties: BTreeMap<String, FieldType>,
}

impl IndexMapping {
    /// Parse the ES mapping shape: {"properties": {"f": {"type": "..."}}}.
    /// Unknown per-field parameters are ignored; unknown types are errors.
    pub fn from_json(mapping: &serde_json::Value) -> IndexResult<Self> {
        let mut properties = BTreeMap::new();
        let Some(props) = mapping.get("properties") else {
            return Ok(Self { properties });
        };
        let props = props.as_object().ok_or_else(|| {
            IndexError::InvalidMapping("'properties' must be an object".to_string())
        })?;
        for (name, def) in props {
            if name.starts_with('_') {
                return Err(IndexError::InvalidMapping(format!(
                    "field name '{name}' is reserved"
                )));
            }
            let ty = def
                .get("type")
                .and_then(|t| t.as_str())
                .ok_or_else(|| {
                    IndexError::InvalidMapping(format!("field '{name}' is missing 'type'"))
                })?;
            properties.insert(name.clone(), FieldType::parse(ty)?);
        }
        Ok(Self { properties })
    }

    /// Render back into the ES mapping shape.
    pub fn to_json(&self) -> serde_json::Value {
        let props: serde_json::Map<String, serde_json::Value> = self
            .properties
            .iter()
            .map(|(name, ty)| {
                (
                    name.clone(),
                    serde_json::json!({ "type": serde_json::to_value(ty).unwrap() }),
                )
            })
            .collect();
        serde_json::json!({ "properties": props })
    }
}

/// A Tantivy schema built from an [`IndexMapping`], with handles to the
/// reserved fields and every mapped field.
#[derive(Clone)]
pub struct MappedSchema {
    /// The built Tantivy schema.
    pub schema: Schema,
    /// Handle to the stored `_source` field.
    pub source: Field,
    /// Handle to the indexed `_timestamp` fast field.
    pub timestamp: Field,
    /// Handle to the `_dynamic` JSON field for unmapped keys.
    pub dynamic: Field,
    /// Handles and types for every explicitly mapped field.
    pub fields: BTreeMap<String, (Field, FieldType)>,
    /// The mapping this schema was built from.
    pub mapping: IndexMapping,
    /// Handle to the stored `_id` field; None for legacy (version 0)
    /// schemas, which have no document ids.
    pub id: Option<Field>,
    /// Handle to the `_seq` fast field; None for legacy schemas.
    pub seq: Option<Field>,
    /// The layout version this schema follows.
    pub schema_version: u32,
}

impl MappedSchema {
    /// Build the current-version Tantivy schema: reserved fields plus one
    /// field per mapping entry, typed per [`FieldType`].
    pub fn build(mapping: IndexMapping) -> Self {
        Self::build_versioned(mapping, CURRENT_SCHEMA_VERSION)
    }

    /// Build the schema for a given layout version — used to interpret a
    /// split exactly as it was written (field ordinals must match).
    pub fn build_versioned(mapping: IndexMapping, schema_version: u32) -> Self {
        let mut builder = Schema::builder();
        let source = builder.add_text_field(SOURCE_FIELD, STORED);
        let timestamp = builder.add_date_field(
            TIMESTAMP_FIELD,
            DateOptions::default()
                .set_indexed()
                .set_fast()
                .set_precision(DateTimePrecision::Milliseconds),
        );
        let dynamic = builder.add_json_field(DYNAMIC_FIELD, TEXT | FAST);

        let mut fields = BTreeMap::new();
        for (name, ty) in &mapping.properties {
            let field = match ty {
                FieldType::Keyword => builder.add_text_field(name, STRING | FAST),
                FieldType::Text => builder.add_text_field(name, TEXT),
                FieldType::Long => builder.add_i64_field(name, INDEXED | FAST),
                FieldType::Double => builder.add_f64_field(name, INDEXED | FAST),
                FieldType::Boolean => builder.add_bool_field(name, INDEXED | FAST),
                FieldType::Date => builder.add_date_field(
                    name,
                    DateOptions::default()
                        .set_indexed()
                        .set_fast()
                        .set_precision(DateTimePrecision::Milliseconds),
                ),
                FieldType::Ip => builder.add_ip_addr_field(name, INDEXED | FAST),
            };
            fields.insert(name.clone(), (field, *ty));
        }
        // Appended last so a version-0 split's mapped-field ordinals are
        // identical to a version-1 split built from the same mapping.
        let (id, seq) = if schema_version >= 1 {
            (
                Some(builder.add_text_field(ID_FIELD, STRING | STORED)),
                Some(builder.add_i64_field(SEQ_FIELD, INDEXED | FAST)),
            )
        } else {
            (None, None)
        };

        Self {
            schema: builder.build(),
            source,
            timestamp,
            dynamic,
            fields,
            mapping,
            id,
            seq,
            schema_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_es_mapping_subset() {
        let mapping = IndexMapping::from_json(&serde_json::json!({
            "properties": {
                "service": {"type": "keyword"},
                "message": {"type": "text"},
                "status": {"type": "integer", "ignored_param": true},
                "latency": {"type": "double"},
                "ok": {"type": "boolean"},
                "ts": {"type": "date"},
                "client": {"type": "ip"},
            }
        }))
        .unwrap();
        assert_eq!(mapping.properties["service"], FieldType::Keyword);
        assert_eq!(mapping.properties["status"], FieldType::Long);
        assert_eq!(mapping.properties.len(), 7);
    }

    #[test]
    fn rejects_unknown_type_and_reserved_names() {
        assert!(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {"f": {"type": "geo_shape"}}
            }))
            .is_err()
        );
        assert!(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {"_source": {"type": "keyword"}}
            }))
            .is_err()
        );
    }

    #[test]
    fn empty_mapping_builds_reserved_fields_only() {
        let schema = MappedSchema::build(IndexMapping::default());
        assert!(schema.schema.get_field(SOURCE_FIELD).is_ok());
        assert!(schema.schema.get_field(TIMESTAMP_FIELD).is_ok());
        assert!(schema.schema.get_field(DYNAMIC_FIELD).is_ok());
        assert!(schema.schema.get_field(ID_FIELD).is_ok());
        assert!(schema.schema.get_field(SEQ_FIELD).is_ok());
        assert!(schema.fields.is_empty());
        assert_eq!(schema.schema_version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn legacy_schema_keeps_mapped_field_ordinals() {
        let mapping = IndexMapping::from_json(&serde_json::json!({
            "properties": {"a": {"type": "keyword"}, "b": {"type": "long"}}
        }))
        .unwrap();
        let legacy = MappedSchema::build_versioned(mapping.clone(), 0);
        let current = MappedSchema::build(mapping);
        assert!(legacy.id.is_none() && legacy.seq.is_none());
        assert!(legacy.schema.get_field(ID_FIELD).is_err());
        for name in ["a", "b"] {
            assert_eq!(legacy.fields[name].0, current.fields[name].0);
        }
    }

    #[test]
    fn mapping_roundtrips_to_json() {
        let json = serde_json::json!({
            "properties": {"service": {"type": "keyword"}, "n": {"type": "long"}}
        });
        let mapping = IndexMapping::from_json(&json).unwrap();
        assert_eq!(mapping.to_json(), json);
    }
}
