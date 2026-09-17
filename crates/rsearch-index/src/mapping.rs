use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tantivy::schema::{
    DateOptions, DateTimePrecision, FAST, Field, INDEXED, IndexRecordOption, JsonObjectOptions,
    STORED, STRING, Schema, TEXT, TextFieldIndexing, TextOptions,
};

use crate::normalizer::{Normalizer, NormalizerFilter};
use crate::tokenizer::STANDARD_TOKENIZER;

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
/// Reserved JSON field holding the exact (untokenized) string values of
/// unmapped keys: the `<path>.keyword` sub-field OpenSearch maps every
/// dynamic string with. Present with `schema_version >= 2`.
pub const DYNAMIC_RAW_FIELD: &str = "_dynamic_raw";
/// Sub-field name for the exact-value view of a dynamic string.
pub const KEYWORD_SUBFIELD: &str = "keyword";
/// OpenSearch's `ignore_above` on dynamic keyword sub-fields: strings
/// longer than this many characters have no `.keyword` value.
pub const KEYWORD_IGNORE_ABOVE: usize = 256;
/// Key the resolved normalizer definitions are persisted under, beside
/// `properties`, so a split carries the chains it was written with. It is
/// stripped from every `_mapping` response (issue #85).
pub const NORMALIZERS_KEY: &str = "_normalizers";
/// Schema version written into new splits. `0` (or absent in the footer)
/// is the legacy layout without `_id`/`_seq`; `1` adds them after the
/// mapped fields so legacy field ordinals are unchanged; `2` switches
/// text analysis to the OpenSearch `standard` analyzer and appends the
/// `_dynamic_raw` keyword sub-field store (issue #66). Declared
/// multi-fields (issue #85) need no new version: a split is always read
/// back with the mapping it was written with, so a sub-field added later
/// simply does not exist in older splits — as in OpenSearch.
pub const CURRENT_SCHEMA_VERSION: u32 = 2;

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

    /// The canonical type name, for OpenSearch-shaped error messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Keyword => "keyword",
            Self::Text => "text",
            Self::Long => "long",
            Self::Double => "double",
            Self::Boolean => "boolean",
            Self::Date => "date",
            Self::Ip => "ip",
        }
    }

    /// Whether this type is one OpenSearch accepts `ignore_malformed` on.
    fn takes_ignore_malformed(&self) -> bool {
        matches!(self, Self::Long | Self::Double | Self::Date | Self::Ip)
    }
}

/// One declared field: its type, the per-field parameters rSearch honors,
/// and its multi-fields (issue #85).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDef {
    /// The parsed type.
    pub ty: FieldType,
    /// The type name exactly as declared, so `_mapping` echoes `integer`
    /// back as `integer` even though it indexes as a `long`.
    pub declared: String,
    /// Name of the `normalizer` applied to this keyword's values, looked
    /// up in [`IndexMapping::normalizers`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalizer: Option<String>,
    /// `ignore_above`: keyword values longer than this are not indexed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_above: Option<usize>,
    /// `ignore_malformed`: a value this field cannot parse is dropped
    /// instead of failing the document (issue #86).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_malformed: bool,
    /// Client `meta`, stored and echoed verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    /// Declared multi-fields, by sub-field name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, FieldDef>,
}

impl FieldDef {
    /// A field of `ty` with every parameter at its default.
    pub fn new(ty: FieldType) -> Self {
        Self {
            ty,
            declared: ty.as_str().to_string(),
            normalizer: None,
            ignore_above: None,
            ignore_malformed: false,
            meta: None,
            fields: BTreeMap::new(),
        }
    }

    /// Parse one property definition. `path` is the full dotted path, used
    /// in error messages the way OpenSearch names the mapper.
    fn from_json(path: &str, def: &Value, top_level: bool) -> IndexResult<Self> {
        let obj = def.as_object().ok_or_else(|| {
            IndexError::MapperParsing(format!("expected map for property [{path}]"))
        })?;
        let ty = match obj.get("type").and_then(Value::as_str) {
            Some(ty) => ty,
            None if top_level => {
                return Err(IndexError::MapperParsing(format!(
                    "No type specified for field [{path}]"
                )));
            }
            None => {
                return Err(IndexError::MapperParsing(format!(
                    "no type specified for property [{path}]"
                )));
            }
        };
        let parsed = FieldType::parse(ty)?;
        let mut field = Self::new(parsed);
        field.declared = ty.to_string();
        for (key, value) in obj {
            match key.as_str() {
                "type" => {}
                "fields" => {
                    let subs = value.as_object().ok_or_else(|| {
                        IndexError::MapperParsing(format!(
                            "[fields] must be an object on mapper [{path}]"
                        ))
                    })?;
                    for (sub_name, sub_def) in subs {
                        if sub_name.is_empty() {
                            return Err(IndexError::MapperParsing(format!(
                                "field name cannot be an empty string in [fields] on mapper [{path}]"
                            )));
                        }
                        let sub_path = format!("{path}.{sub_name}");
                        field
                            .fields
                            .insert(sub_name.clone(), Self::from_json(&sub_path, sub_def, false)?);
                    }
                }
                "normalizer" => {
                    if parsed != FieldType::Keyword {
                        return Err(unknown_parameter(key, path, ty));
                    }
                    let name = value.as_str().ok_or_else(|| {
                        IndexError::MapperParsing(format!(
                            "[normalizer] on mapper [{path}] must be a string"
                        ))
                    })?;
                    field.normalizer = Some(name.to_string());
                }
                "ignore_above" => {
                    if parsed != FieldType::Keyword {
                        return Err(unknown_parameter(key, path, ty));
                    }
                    let limit = value.as_u64().ok_or_else(|| {
                        IndexError::MapperParsing(format!(
                            "[ignore_above] on mapper [{path}] must be a non-negative integer"
                        ))
                    })?;
                    field.ignore_above = Some(limit as usize);
                }
                "ignore_malformed" => {
                    if !parsed.takes_ignore_malformed() {
                        return Err(unknown_parameter(key, path, ty));
                    }
                    let flag = value.as_bool().ok_or_else(|| {
                        IndexError::MapperParsing(format!(
                            "[ignore_malformed] on mapper [{path}] must be a boolean"
                        ))
                    })?;
                    field.ignore_malformed = flag;
                }
                "meta" => {
                    if !value.is_object() {
                        return Err(IndexError::MapperParsing(format!(
                            "[meta] on mapper [{path}] must be an object"
                        )));
                    }
                    field.meta = Some(value.clone());
                }
                other => match inert_default(parsed, other) {
                    // A parameter rSearch does not implement is accepted
                    // only at the value that makes it a no-op, so a client
                    // sending OpenSearch defaults still works while a real
                    // request for unimplemented behaviour is refused
                    // instead of silently ignored (issues #85, #86).
                    Some(default) if is_default(value, &default) => {}
                    Some(default) => {
                        let default = render_default(&default);
                        return Err(IndexError::MapperParsing(format!(
                            "parameter [{other}] on mapper [{path}] of type [{ty}] is supported \
                             only at its default value [{default}]"
                        )));
                    }
                    None => return Err(unknown_parameter(other, path, ty)),
                },
            }
        }
        Ok(field)
    }

    /// Render back into the ES mapping shape.
    fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("type".to_string(), Value::String(self.declared.clone()));
        if let Some(normalizer) = &self.normalizer {
            out.insert("normalizer".to_string(), Value::String(normalizer.clone()));
        }
        if let Some(limit) = self.ignore_above {
            out.insert("ignore_above".to_string(), Value::from(limit));
        }
        if self.ignore_malformed {
            out.insert("ignore_malformed".to_string(), Value::Bool(true));
        }
        if let Some(meta) = &self.meta {
            out.insert("meta".to_string(), meta.clone());
        }
        if !self.fields.is_empty() {
            let subs: Map<String, Value> = self
                .fields
                .iter()
                .map(|(name, def)| (name.clone(), def.to_json()))
                .collect();
            out.insert("fields".to_string(), Value::Object(subs));
        }
        Value::Object(out)
    }
}

/// OpenSearch's own wording for a parameter no mapper of this type has.
fn unknown_parameter(key: &str, path: &str, ty: &str) -> IndexError {
    IndexError::MapperParsing(format!(
        "unknown parameter [{key}] on mapper [{path}] of type [{ty}]"
    ))
}

/// The value at which an unimplemented OpenSearch mapping parameter is a
/// no-op, or `None` when no mapper of this type accepts the parameter at
/// all. Keeps `PUT /{index}` accepting the parameter blocks stock clients
/// and index dumps send.
fn inert_default(ty: FieldType, key: &str) -> Option<Value> {
    let text = ty == FieldType::Text;
    match key {
        "index" | "enabled" => Some(Value::Bool(true)),
        "doc_values" => Some(Value::Bool(!text)),
        "store" => Some(Value::Bool(false)),
        "norms" => Some(Value::Bool(text)),
        "boost" => Some(Value::from(1.0)),
        "similarity" => Some(Value::String("BM25".to_string())),
        "copy_to" | "null_value" => Some(Value::Null),
        "coerce" if matches!(ty, FieldType::Long | FieldType::Double) => Some(Value::Bool(true)),
        "index_options" => Some(Value::String(
            if text { "positions" } else { "docs" }.to_string(),
        )),
        "eager_global_ordinals" if ty == FieldType::Keyword => Some(Value::Bool(false)),
        "split_queries_on_whitespace" if ty == FieldType::Keyword => Some(Value::Bool(false)),
        "fielddata" if text => Some(Value::Bool(false)),
        "term_vector" if text => Some(Value::String("no".to_string())),
        "position_increment_gap" if text => Some(Value::from(100)),
        "analyzer" | "search_analyzer" | "search_quote_analyzer" if text => {
            Some(Value::String("standard".to_string()))
        }
        // The one date format rSearch implements is OpenSearch's default.
        "format" if ty == FieldType::Date => Some(Value::String(
            "strict_date_optional_time||epoch_millis".to_string(),
        )),
        _ => None,
    }
}

/// A default value as it reads in an error message: a bare string, or
/// the JSON form for everything else.
fn render_default(value: &Value) -> String {
    match value.as_str() {
        Some(s) => s.to_string(),
        None => value.to_string(),
    }
}

/// Whether a declared parameter value is the inert one. An explicit null
/// or an empty list counts as "not set" for every parameter.
fn is_default(value: &Value, default: &Value) -> bool {
    if value.is_null() || value.as_array().is_some_and(|a| a.is_empty()) {
        return true;
    }
    match (value, default) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        _ => value == default,
    }
}

/// Parsed index mapping: explicit field definitions plus the normalizers
/// they reference. Unmapped fields are indexed dynamically under the
/// `_dynamic` JSON field.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexMapping {
    /// Field name -> declaration for explicitly mapped fields.
    pub properties: BTreeMap<String, FieldDef>,
    /// Normalizer name -> resolved filter chain, as declared in the
    /// index's `settings.analysis.normalizer` when it was created.
    #[serde(default)]
    pub normalizers: BTreeMap<String, Normalizer>,
}

impl IndexMapping {
    /// Parse a stored mapping: the ES shape `{"properties": {...}}` plus
    /// the resolved `_normalizers` rSearch persists beside it.
    pub fn from_json(mapping: &Value) -> IndexResult<Self> {
        let mut normalizers = BTreeMap::new();
        if let Some(stored) = mapping.get(NORMALIZERS_KEY) {
            let stored = stored.as_object().ok_or_else(|| {
                IndexError::InvalidMapping("stored normalizers must be an object".to_string())
            })?;
            for (name, def) in stored {
                normalizers.insert(name.clone(), parse_normalizer(name, def)?);
            }
        }
        Self::parse(mapping, normalizers)
    }

    /// Parse a client `PUT /{index}` body: `mappings` validated against
    /// the normalizers declared in `settings.analysis.normalizer`.
    pub fn from_request(mappings: &Value, settings: Option<&Value>) -> IndexResult<Self> {
        let mut normalizers = BTreeMap::new();
        let declared = settings
            .and_then(|s| s.pointer("/analysis/normalizer"))
            .or_else(|| settings.and_then(|s| s.pointer("/index/analysis/normalizer")));
        if let Some(declared) = declared {
            let declared = declared.as_object().ok_or_else(|| {
                IndexError::IllegalArgument("[analysis.normalizer] must be an object".to_string())
            })?;
            for (name, def) in declared {
                normalizers.insert(name.clone(), parse_normalizer(name, def)?);
            }
        }
        Self::parse(mappings, normalizers)
    }

    fn parse(mapping: &Value, normalizers: BTreeMap<String, Normalizer>) -> IndexResult<Self> {
        let mut properties = BTreeMap::new();
        let Some(props) = mapping.get("properties") else {
            return Ok(Self { properties, normalizers });
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
            properties.insert(name.clone(), FieldDef::from_json(name, def, true)?);
        }
        let parsed = Self { properties, normalizers };
        parsed.check_normalizer_references()?;
        Ok(parsed)
    }

    /// Every `normalizer` a field names must be defined, the way
    /// OpenSearch refuses the mapping otherwise.
    fn check_normalizer_references(&self) -> IndexResult<()> {
        fn walk(
            path: &str,
            def: &FieldDef,
            normalizers: &BTreeMap<String, Normalizer>,
        ) -> IndexResult<()> {
            if let Some(name) = &def.normalizer
                && !normalizers.contains_key(name)
            {
                return Err(IndexError::MapperParsing(format!(
                    "normalizer [{name}] not found for field [{path}]"
                )));
            }
            for (sub_name, sub) in &def.fields {
                walk(&format!("{path}.{sub_name}"), sub, normalizers)?;
            }
            Ok(())
        }
        for (name, def) in &self.properties {
            walk(name, def, &self.normalizers)?;
        }
        Ok(())
    }

    /// Render back into the stored shape: the ES `properties` block plus
    /// the resolved normalizer chains.
    pub fn to_json(&self) -> Value {
        let props: Map<String, Value> = self
            .properties
            .iter()
            .map(|(name, def)| (name.clone(), def.to_json()))
            .collect();
        let mut out = Map::new();
        out.insert("properties".to_string(), Value::Object(props));
        if !self.normalizers.is_empty() {
            let normalizers: Map<String, Value> = self
                .normalizers
                .iter()
                .map(|(name, n)| (name.clone(), n.to_json()))
                .collect();
            out.insert(NORMALIZERS_KEY.to_string(), Value::Object(normalizers));
        }
        Value::Object(out)
    }

    /// The `settings.analysis` block echoing the declared normalizers, or
    /// `None` when the index declares none.
    pub fn analysis_json(&self) -> Option<Value> {
        if self.normalizers.is_empty() {
            return None;
        }
        let normalizers: Map<String, Value> = self
            .normalizers
            .iter()
            .map(|(name, n)| (name.clone(), n.to_json()))
            .collect();
        Some(serde_json::json!({ "normalizer": Value::Object(normalizers) }))
    }

    /// Every indexable path this mapping declares — each property and,
    /// recursively, its multi-fields — in schema order.
    fn paths(&self) -> Vec<(String, &FieldDef)> {
        fn walk<'a>(path: String, def: &'a FieldDef, out: &mut Vec<(String, &'a FieldDef)>) {
            out.push((path.clone(), def));
            for (name, sub) in &def.fields {
                walk(format!("{path}.{name}"), sub, out);
            }
        }
        let mut out = Vec::new();
        for (name, def) in &self.properties {
            walk(name.clone(), def, &mut out);
        }
        out
    }
}

/// Parse one `settings.analysis.normalizer` definition, with OpenSearch's
/// restriction that a normalizer may not use a tokenizing filter.
fn parse_normalizer(name: &str, def: &Value) -> IndexResult<Normalizer> {
    let obj = def.as_object().ok_or_else(|| {
        IndexError::IllegalArgument(format!("normalizer [{name}] must be an object"))
    })?;
    if let Some(ty) = obj.get("type").and_then(Value::as_str)
        && ty != "custom"
    {
        return Err(IndexError::IllegalArgument(format!(
            "Unknown normalizer type [{ty}] for [{name}]"
        )));
    }
    if let Some(char_filter) = obj.get("char_filter")
        && !char_filter.as_array().is_some_and(|a| a.is_empty())
    {
        return Err(IndexError::IllegalArgument(format!(
            "Custom normalizer [{name}] may not use char filters"
        )));
    }
    let mut filters = Vec::new();
    if let Some(declared) = obj.get("filter") {
        let declared = declared.as_array().ok_or_else(|| {
            IndexError::IllegalArgument(format!("normalizer [{name}] filter must be an array"))
        })?;
        for filter in declared {
            let filter_name = filter.as_str().ok_or_else(|| {
                IndexError::IllegalArgument(format!(
                    "normalizer [{name}] filter names must be strings"
                ))
            })?;
            let parsed = NormalizerFilter::parse(filter_name).ok_or_else(|| {
                IndexError::IllegalArgument(format!(
                    "Custom normalizer [{name}] may not use filter [{filter_name}]"
                ))
            })?;
            filters.push(parsed);
        }
    }
    Ok(Normalizer { filters })
}

/// One mapped path in a built schema: its Tantivy field plus the
/// parameters the write and query paths need.
#[derive(Debug, Clone)]
pub struct MappedField {
    /// The Tantivy field values are indexed into.
    pub field: Field,
    /// The declared type.
    pub ty: FieldType,
    /// Resolved `normalizer` chain, applied at index time and to query
    /// input (keyword fields only).
    pub normalizer: Option<Normalizer>,
    /// `ignore_above`: longer keyword values are not indexed.
    pub ignore_above: Option<usize>,
    /// `ignore_malformed`: drop an unparseable value instead of failing
    /// the document.
    pub ignore_malformed: bool,
}

impl MappedField {
    /// Apply this field's normalizer, if it has one.
    pub fn normalize<'a>(&self, value: &'a str) -> std::borrow::Cow<'a, str> {
        match &self.normalizer {
            Some(normalizer) => normalizer.apply(value),
            None => std::borrow::Cow::Borrowed(value),
        }
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
    /// Every explicitly mapped path, including declared multi-fields
    /// under their full dotted name (`title.keyword`).
    pub fields: BTreeMap<String, MappedField>,
    /// The mapping this schema was built from.
    pub mapping: IndexMapping,
    /// Handle to the stored `_id` field; None for legacy (version 0)
    /// schemas, which have no document ids.
    pub id: Option<Field>,
    /// Handle to the `_seq` fast field; None for legacy schemas.
    pub seq: Option<Field>,
    /// Handle to the `_dynamic_raw` JSON field (exact string values of
    /// unmapped keys, the `.keyword` view); None before version 2.
    pub dynamic_raw: Option<Field>,
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
        // Version 2 analyzes text the way OpenSearch does; earlier splits
        // were written with Tantivy's `default` tokenizer and must be
        // read back with the layout they were built under.
        let (text_options, dynamic_options): (TextOptions, JsonObjectOptions) = if schema_version >= 2 {
            let indexing = TextFieldIndexing::default()
                .set_tokenizer(STANDARD_TOKENIZER)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions);
            (
                TextOptions::default().set_indexing_options(indexing.clone()),
                JsonObjectOptions::default()
                    .set_indexing_options(indexing)
                    .set_fast(None),
            )
        } else {
            (TEXT.into(), (TEXT | FAST).into())
        };
        let dynamic = builder.add_json_field(DYNAMIC_FIELD, dynamic_options);

        let mut fields = BTreeMap::new();
        // A property and its multi-fields are added together, so the
        // ordinals of a mapping without sub-fields are unchanged.
        for (path, def) in mapping.paths() {
            let field = match def.ty {
                FieldType::Keyword => builder.add_text_field(&path, STRING | FAST),
                FieldType::Text => builder.add_text_field(&path, text_options.clone()),
                FieldType::Long => builder.add_i64_field(&path, INDEXED | FAST),
                FieldType::Double => builder.add_f64_field(&path, INDEXED | FAST),
                FieldType::Boolean => builder.add_bool_field(&path, INDEXED | FAST),
                FieldType::Date => builder.add_date_field(
                    &path,
                    DateOptions::default()
                        .set_indexed()
                        .set_fast()
                        .set_precision(DateTimePrecision::Milliseconds),
                ),
                FieldType::Ip => builder.add_ip_addr_field(&path, INDEXED | FAST),
            };
            fields.insert(
                path,
                MappedField {
                    field,
                    ty: def.ty,
                    normalizer: def
                        .normalizer
                        .as_ref()
                        .and_then(|name| mapping.normalizers.get(name).cloned()),
                    ignore_above: def.ignore_above,
                    ignore_malformed: def.ignore_malformed,
                },
            );
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
        // Exact string values, one term per value (the `raw` tokenizer),
        // no positions needed. Also a fast field: range, exists and
        // aggregations on `<path>.keyword` read it, which keeps
        // `ignore_above` semantics (a value past the limit has no keyword
        // view at all) exactly as OpenSearch's keyword doc values do.
        let dynamic_raw = (schema_version >= 2).then(|| {
            builder.add_json_field(
                DYNAMIC_RAW_FIELD,
                JsonObjectOptions::default()
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("raw")
                            .set_index_option(IndexRecordOption::Basic),
                    )
                    .set_fast(None),
            )
        });

        Self {
            schema: builder.build(),
            source,
            timestamp,
            dynamic,
            fields,
            mapping,
            id,
            seq,
            dynamic_raw,
            schema_version,
        }
    }

    /// Name of the analyzer `text` fields and `_dynamic` strings were
    /// indexed with under this layout version — what `match`-style
    /// queries must analyze their input with to hit the same tokens.
    pub fn text_tokenizer(&self) -> &'static str {
        if self.schema_version >= 2 { STANDARD_TOKENIZER } else { "default" }
    }

    /// A fresh in-memory Tantivy index over this schema with rSearch's
    /// analyzers registered (tests and tooling).
    pub fn create_in_ram(&self) -> tantivy::Index {
        let index = tantivy::Index::create_in_ram(self.schema.clone());
        crate::tokenizer::register_tokenizers(&index);
        index
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
                "status": {"type": "integer", "index": true},
                "latency": {"type": "double"},
                "ok": {"type": "boolean"},
                "ts": {"type": "date"},
                "client": {"type": "ip"},
            }
        }))
        .unwrap();
        assert_eq!(mapping.properties["service"].ty, FieldType::Keyword);
        assert_eq!(mapping.properties["status"].ty, FieldType::Long);
        // The declared alias is echoed back, as OpenSearch does.
        assert_eq!(mapping.properties["status"].declared, "integer");
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
    fn rejects_parameters_it_would_otherwise_ignore() {
        let err = |mapping: serde_json::Value| {
            IndexMapping::from_json(&mapping).unwrap_err().to_string()
        };
        assert_eq!(
            err(serde_json::json!({"properties": {"f": {"type": "keyword", "bogus": true}}})),
            "unknown parameter [bogus] on mapper [f] of type [keyword]"
        );
        // Parameters OpenSearch only allows on other types are unknown
        // here too, with OpenSearch's own wording.
        assert_eq!(
            err(serde_json::json!({"properties": {"f": {"type": "text", "ignore_above": 10}}})),
            "unknown parameter [ignore_above] on mapper [f] of type [text]"
        );
        assert_eq!(
            err(serde_json::json!({"properties": {"f": {"type": "keyword", "ignore_malformed": true}}})),
            "unknown parameter [ignore_malformed] on mapper [f] of type [keyword]"
        );
        assert_eq!(
            err(serde_json::json!({"properties": {"f": {"type": "keyword", "index": false}}})),
            "parameter [index] on mapper [f] of type [keyword] is supported only at its \
             default value [true]"
        );
        assert_eq!(
            err(serde_json::json!({"properties": {"f": {"type": "date", "format": "yyyy/MM/dd"}}})),
            "parameter [format] on mapper [f] of type [date] is supported only at its \
             default value [strict_date_optional_time||epoch_millis]"
        );
    }

    #[test]
    fn accepts_opensearch_defaults_unchanged() {
        let mapping = IndexMapping::from_json(&serde_json::json!({
            "properties": {
                "f": {"type": "keyword", "index": true, "doc_values": true, "store": false,
                      "copy_to": [], "null_value": null, "eager_global_ordinals": false},
                "m": {"type": "text", "analyzer": "standard", "norms": true,
                      "position_increment_gap": 100},
                "n": {"type": "long", "coerce": true},
                "d": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            }
        }))
        .unwrap();
        assert_eq!(mapping.properties.len(), 4);
    }

    #[test]
    fn multi_fields_become_their_own_schema_fields() {
        let mapping = IndexMapping::from_json(&serde_json::json!({
            "properties": {
                "name": {"type": "text", "fields": {
                    "keyword": {"type": "keyword"},
                    "raw": {"type": "keyword", "ignore_above": 5},
                }},
            }
        }))
        .unwrap();
        let schema = MappedSchema::build(mapping);
        assert_eq!(schema.fields["name"].ty, FieldType::Text);
        assert_eq!(schema.fields["name.keyword"].ty, FieldType::Keyword);
        assert_eq!(schema.fields["name.raw"].ignore_above, Some(5));
        assert!(schema.schema.get_field("name.keyword").is_ok());
    }

    #[test]
    fn normalizers_resolve_from_settings() {
        let mapping = IndexMapping::from_request(
            &serde_json::json!({"properties": {"code": {"type": "keyword", "normalizer": "lower"}}}),
            Some(&serde_json::json!({
                "analysis": {"normalizer": {"lower": {"type": "custom", "filter": ["lowercase"]}}}
            })),
        )
        .unwrap();
        let schema = MappedSchema::build(mapping.clone());
        assert_eq!(schema.fields["code"].normalize("Kirsten"), "kirsten");
        // The resolved chain persists with the mapping, so a split can be
        // read back without the index settings.
        let stored = IndexMapping::from_json(&mapping.to_json()).unwrap();
        assert_eq!(stored.normalizers["lower"].filters.len(), 1);
    }

    #[test]
    fn rejects_undefined_and_tokenizing_normalizers() {
        let missing = IndexMapping::from_request(
            &serde_json::json!({"properties": {"c": {"type": "keyword", "normalizer": "nope"}}}),
            None,
        )
        .unwrap_err();
        assert_eq!(missing.to_string(), "normalizer [nope] not found for field [c]");

        let bad_filter = IndexMapping::from_request(
            &serde_json::json!({"properties": {"c": {"type": "keyword", "normalizer": "bad"}}}),
            Some(&serde_json::json!({
                "analysis": {"normalizer": {"bad": {"type": "custom", "filter": ["stop"]}}}
            })),
        )
        .unwrap_err();
        assert_eq!(
            bad_filter.to_string(),
            "Custom normalizer [bad] may not use filter [stop]"
        );
    }

    #[test]
    fn missing_type_uses_opensearch_wording() {
        let top = IndexMapping::from_json(&serde_json::json!({"properties": {"f": {}}}))
            .unwrap_err()
            .to_string();
        assert_eq!(top, "No type specified for field [f]");
        let sub = IndexMapping::from_json(&serde_json::json!({
            "properties": {"f": {"type": "text", "fields": {"k": {}}}}
        }))
        .unwrap_err()
        .to_string();
        assert_eq!(sub, "no type specified for property [f.k]");
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
            assert_eq!(legacy.fields[name].field, current.fields[name].field);
        }
    }

    #[test]
    fn mapping_roundtrips_to_json() {
        let json = serde_json::json!({
            "properties": {
                "service": {"type": "keyword", "ignore_above": 32},
                "n": {"type": "long", "ignore_malformed": true},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            }
        });
        let mapping = IndexMapping::from_json(&json).unwrap();
        assert_eq!(mapping.to_json(), json);
    }
}
