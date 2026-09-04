//! Field sorting (issue #77): `sort` on keyword, numeric, boolean, date
//! and ip fields — mapped or dynamic (`<path>.keyword`, numeric paths)
//! — with OpenSearch's missing-value placement, `search_after` cursors
//! over the full sort key, and its refusals (text fields, unknown
//! fields without `unmapped_type`).
//!
//! Collection is per split: a segment collector keeps the top-k by a
//! cheap per-segment key (term ordinals for strings, raw numbers
//! otherwise), and only the survivors are materialized into comparable
//! values for the cross-segment and cross-split merges. The implicit
//! final tiebreak is always (timestamp desc, `_seq` desc), appended to
//! every hit's `sort` values so a cursor built from them is unique.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::net::Ipv6Addr;
use std::sync::Arc;

use serde_json::{Value, json};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{Column, DynamicColumn, StrColumn};
use tantivy::columnar::TermOrdHit;
use tantivy::{DocAddress, DocId, Score, SegmentOrdinal, SegmentReader};

use rsearch_index::{FieldType, MappedSchema};

use crate::error::{SearchError, SearchResult};

const TIMESTAMP_ALIASES: [&str; 3] = ["@timestamp", "timestamp", "_timestamp"];

/// The value type a sort key is compared and reported as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortType {
    /// Exact string (mapped keyword, dynamic `.keyword`).
    Keyword,
    /// Integer (mapped long, dynamic integer path, `_seq`).
    Long,
    /// Floating point (mapped double, dynamic fractional path).
    Double,
    /// Boolean.
    Boolean,
    /// Date, as epoch millis.
    Date,
    /// IP address.
    Ip,
}

impl SortType {
    fn parse_unmapped(name: &str) -> Option<Self> {
        match name {
            "keyword" => Some(Self::Keyword),
            "long" | "integer" | "short" | "byte" => Some(Self::Long),
            "double" | "float" | "half_float" => Some(Self::Double),
            "boolean" => Some(Self::Boolean),
            "date" => Some(Self::Date),
            "ip" => Some(Self::Ip),
            _ => None,
        }
    }
}

/// One `sort` clause as written in the request body.
#[derive(Clone, Debug, PartialEq)]
pub struct SortField {
    /// Field name as requested.
    pub field: String,
    /// `order: desc` (`asc` is the default, as in OpenSearch).
    pub desc: bool,
    /// `missing`: `_first`, `_last` (default) or a substitute value.
    pub missing: Option<Value>,
    /// `unmapped_type`: sort on a field no document holds, as all
    /// missing, instead of a 400.
    pub unmapped_type: Option<String>,
}

impl SortField {
    /// Parse one entry of a `sort` array: a bare field name or
    /// `{field: order}` / `{field: {order, missing, unmapped_type, mode}}`.
    /// `_score` and `_doc` are no-ops (None), as they are alongside a
    /// field sort in OpenSearch.
    pub fn parse(entry: &Value) -> SearchResult<Option<Self>> {
        match entry {
            Value::String(name) => Ok(Self::named(name)),
            Value::Object(map) => {
                let mut parsed = None;
                for (name, spec) in map {
                    let Some(mut field) = Self::named(name) else { continue };
                    match spec {
                        Value::String(order) => field.desc = parse_order(name, order)?,
                        Value::Object(opts) => {
                            for (key, value) in opts {
                                match key.as_str() {
                                    "order" => {
                                        let order = value.as_str().ok_or_else(|| {
                                            SearchError::BadRequest(format!(
                                                "sort order for [{name}] must be a string"
                                            ))
                                        })?;
                                        field.desc = parse_order(name, order)?;
                                    }
                                    "missing" => field.missing = Some(value.clone()),
                                    "unmapped_type" => {
                                        field.unmapped_type =
                                            Some(value.as_str().unwrap_or("").to_string());
                                    }
                                    // Multi-valued fields sort by their
                                    // min (asc) / max (desc), the default
                                    // mode; other modes are not supported.
                                    "mode" => {
                                        let mode = value.as_str().unwrap_or("");
                                        if mode != "min" && mode != "max" {
                                            return Err(SearchError::BadRequest(format!(
                                                "sort mode [{mode}] on [{name}] is not supported"
                                            )));
                                        }
                                    }
                                    other => {
                                        return Err(SearchError::BadRequest(format!(
                                            "sort option [{other}] on [{name}] is not supported"
                                        )));
                                    }
                                }
                            }
                        }
                        other => {
                            return Err(SearchError::BadRequest(format!(
                                "sort spec for [{name}] must be an order or an object, got {other}"
                            )));
                        }
                    }
                    parsed = Some(field);
                }
                Ok(parsed)
            }
            other => Err(SearchError::BadRequest(format!(
                "sort entries must be a field name or an object, got {other}"
            ))),
        }
    }

    fn named(name: &str) -> Option<Self> {
        if name == "_score" || name == "_doc" {
            return None;
        }
        // A bare field name sorts ascending, as in OpenSearch (the
        // request-level default of newest-first applies only when no
        // sort is given at all).
        Some(Self {
            field: name.to_string(),
            desc: false,
            missing: None,
            unmapped_type: None,
        })
    }

    /// Whether this is the timestamp field itself.
    pub fn is_timestamp(&self) -> bool {
        TIMESTAMP_ALIASES.contains(&self.field.as_str())
    }
}

fn parse_order(name: &str, order: &str) -> SearchResult<bool> {
    match order {
        "asc" => Ok(false),
        "desc" => Ok(true),
        other => Err(SearchError::BadRequest(format!(
            "sort order [{other}] on [{name}] is not valid (asc or desc)"
        ))),
    }
}

/// The requested sort of a search.
#[derive(Clone, Debug, PartialEq)]
pub enum SortSpec {
    /// Timestamp order only (the default: newest first).
    Timestamp {
        /// Newest first.
        desc: bool,
    },
    /// One or more field clauses, in order (the timestamp may be among
    /// them); ties broken by (timestamp desc, `_seq` desc).
    Fields(Vec<SortField>),
}

impl SortSpec {
    /// Parse a request's `sort` value (a single entry or an array).
    /// Clauses on the timestamp alone keep the timestamp fast path; any
    /// other field switches to a field sort. `_score`/`_doc` are no-ops.
    pub fn parse(sort: Option<&Value>) -> SearchResult<Self> {
        let Some(sort) = sort else {
            return Ok(Self::Timestamp { desc: true });
        };
        let entries: Vec<&Value> = match sort {
            Value::Array(items) => items.iter().collect(),
            single => vec![single],
        };
        let mut fields = Vec::new();
        for entry in entries {
            if let Some(field) = SortField::parse(entry)? {
                fields.push(field);
            }
        }
        if fields.iter().all(|f| f.is_timestamp() && f.missing.is_none()) {
            let desc = fields.last().map(|f| f.desc).unwrap_or(true);
            return Ok(Self::Timestamp { desc });
        }
        Ok(Self::Fields(fields))
    }

    /// Timestamp direction when this is a timestamp sort.
    pub fn timestamp_desc(&self) -> Option<bool> {
        match self {
            Self::Timestamp { desc } => Some(*desc),
            Self::Fields(_) => None,
        }
    }
}

/// Where a resolved sort field's values live.
#[derive(Clone, Debug, PartialEq)]
pub enum SortTarget {
    /// The reserved `_timestamp` column.
    Timestamp,
    /// The reserved `_seq` column.
    Seq,
    /// An explicitly mapped field (by name).
    Mapped(String),
    /// `<path>.keyword` on an unmapped path: the `_dynamic_raw` column.
    DynamicKeyword(String),
    /// A numeric or boolean unmapped path: the `_dynamic` column(s).
    DynamicPath(String),
    /// No document holds it (`unmapped_type`): every value is missing.
    Unmapped,
}

/// A sort clause resolved against the stream.
#[derive(Clone, Debug)]
pub struct ResolvedSort {
    /// Field name as requested.
    pub field: String,
    /// Where the values come from.
    pub target: SortTarget,
    /// Compared/reported type.
    pub ty: SortType,
    /// Descending order.
    pub desc: bool,
    /// Missing documents sort as if they held the maximum value.
    pub null_is_max: bool,
    /// Substitute value for missing documents (`missing: <value>`).
    pub substitute: Option<SortValue>,
}

impl ResolvedSort {
    fn order(&self) -> FieldOrder {
        FieldOrder {
            desc: self.desc,
            null_is_max: self.null_is_max,
        }
    }
}

/// A materialized sort value: comparable across segments and splits.
#[derive(Clone, Debug, PartialEq)]
pub enum SortValue {
    /// The document has no value for the field.
    Missing,
    /// String value.
    Str(String),
    /// Integer (long, date millis, `_seq`).
    I64(i64),
    /// Floating point.
    F64(f64),
    /// Boolean.
    Bool(bool),
    /// IP address.
    Ip(Ipv6Addr),
}

impl SortValue {
    /// Natural (ascending) comparison; missing values are placed by
    /// `null_is_max`.
    pub fn cmp_natural(&self, other: &Self, null_is_max: bool) -> Ordering {
        match (self, other) {
            (SortValue::Missing, SortValue::Missing) => Ordering::Equal,
            (SortValue::Missing, _) => {
                if null_is_max { Ordering::Greater } else { Ordering::Less }
            }
            (_, SortValue::Missing) => {
                if null_is_max { Ordering::Less } else { Ordering::Greater }
            }
            (SortValue::Str(a), SortValue::Str(b)) => a.cmp(b),
            (SortValue::I64(a), SortValue::I64(b)) => a.cmp(b),
            (SortValue::F64(a), SortValue::F64(b)) => a.total_cmp(b),
            (SortValue::I64(a), SortValue::F64(b)) => (*a as f64).total_cmp(b),
            (SortValue::F64(a), SortValue::I64(b)) => a.total_cmp(&(*b as f64)),
            (SortValue::Bool(a), SortValue::Bool(b)) => a.cmp(b),
            (SortValue::Ip(a), SortValue::Ip(b)) => a.cmp(b),
            (a, b) => rank(a).cmp(&rank(b)),
        }
    }

    /// The JSON value a hit reports for this sort key: OpenSearch's
    /// sentinels for a missing value (`null` for keywords, the extreme
    /// long/double in the direction the missing docs were placed).
    pub fn to_json(&self, ty: SortType, null_is_max: bool) -> Value {
        match self {
            SortValue::Missing => match ty {
                SortType::Keyword | SortType::Ip => Value::Null,
                SortType::Long | SortType::Date => {
                    json!(if null_is_max { i64::MAX } else { i64::MIN })
                }
                SortType::Double => json!(if null_is_max { "Infinity" } else { "-Infinity" }),
                SortType::Boolean => json!(if null_is_max { i32::MAX } else { i32::MIN }),
            },
            SortValue::Str(s) => json!(s),
            SortValue::I64(n) => json!(n),
            SortValue::F64(f) => json!(f),
            SortValue::Bool(b) => json!(if *b { 1 } else { 0 }),
            SortValue::Ip(ip) => json!(format_ip(ip)),
        }
    }
}

fn rank(v: &SortValue) -> u8 {
    match v {
        SortValue::Missing => 0,
        SortValue::Bool(_) => 1,
        SortValue::I64(_) | SortValue::F64(_) => 2,
        SortValue::Str(_) => 3,
        SortValue::Ip(_) => 4,
    }
}

fn format_ip(ip: &Ipv6Addr) -> String {
    match ip.to_ipv4_mapped() {
        Some(v4) => v4.to_string(),
        None => ip.to_string(),
    }
}

/// Direction and missing placement of one sort field, all the merge
/// needs to order two keys.
#[derive(Clone, Copy, Debug)]
pub struct FieldOrder {
    /// Descending.
    pub desc: bool,
    /// Missing values sort as the maximum (before the direction flip).
    pub null_is_max: bool,
}

/// Compare two full sort keys in result order (Less = earlier), then by
/// the implicit (timestamp desc, `_seq` desc) tiebreak.
pub fn cmp_hits(
    orders: &[FieldOrder],
    a: (&[SortValue], i64, i64),
    b: (&[SortValue], i64, i64),
) -> Ordering {
    for (i, order) in orders.iter().enumerate() {
        let av = a.0.get(i).unwrap_or(&SortValue::Missing);
        let bv = b.0.get(i).unwrap_or(&SortValue::Missing);
        let natural = av.cmp_natural(bv, order.null_is_max);
        let directed = if order.desc { natural.reverse() } else { natural };
        if directed != Ordering::Equal {
            return directed;
        }
    }
    b.1.cmp(&a.1).then(b.2.cmp(&a.2))
}

/// Resolve the requested clauses against the stream: its mapping plus
/// the inventory of unmapped fields its splits hold (`dynamic`: path →
/// value types, see `Metastore::stream_dynamic_fields`). Errors match
/// OpenSearch's: a text field cannot be sorted, an unknown field needs
/// `unmapped_type`.
pub fn resolve_sort(
    fields: &[SortField],
    schema: &MappedSchema,
    dynamic: &std::collections::BTreeMap<String, Vec<String>>,
) -> SearchResult<Vec<ResolvedSort>> {
    fields
        .iter()
        .map(|field| {
            let name = field.field.as_str();
            let (target, ty) = if field.is_timestamp() {
                (SortTarget::Timestamp, SortType::Date)
            } else if name == rsearch_index::SEQ_FIELD {
                (SortTarget::Seq, SortType::Long)
            } else if let Some((_, mapped)) = schema.fields.get(name) {
                let ty = match mapped {
                    FieldType::Keyword => SortType::Keyword,
                    FieldType::Long => SortType::Long,
                    FieldType::Double => SortType::Double,
                    FieldType::Boolean => SortType::Boolean,
                    FieldType::Date => SortType::Date,
                    FieldType::Ip => SortType::Ip,
                    FieldType::Text => return Err(text_fielddata(name)),
                };
                (SortTarget::Mapped(name.to_string()), ty)
            } else if let Some(base) = name.strip_suffix(".keyword")
                && !base.is_empty()
                && !schema.fields.contains_key(base)
                && dynamic.get(base).is_some_and(|t| t.iter().any(|t| t == "string"))
            {
                (SortTarget::DynamicKeyword(base.to_string()), SortType::Keyword)
            } else if let Some(types) = dynamic.get(name) {
                let has = |t: &str| types.iter().any(|x| x == t);
                if has("string") {
                    return Err(text_fielddata(name));
                }
                let ty = if has("double") {
                    SortType::Double
                } else if has("long") {
                    SortType::Long
                } else if has("boolean") {
                    SortType::Boolean
                } else {
                    SortType::Date
                };
                (SortTarget::DynamicPath(name.to_string()), ty)
            } else if let Some(unmapped) = &field.unmapped_type {
                let ty = SortType::parse_unmapped(unmapped).ok_or_else(|| {
                    SearchError::BadRequest(format!(
                        "unmapped_type [{unmapped}] on [{name}] is not a sortable type"
                    ))
                })?;
                (SortTarget::Unmapped, ty)
            } else {
                return Err(SearchError::BadRequest(format!(
                    "No mapping found for [{name}] in order to sort on"
                )));
            };
            // OpenSearch places missing documents last in either direction
            // unless told `_first`; a concrete `missing` value substitutes.
            let (null_is_max, substitute) = match &field.missing {
                None => (!field.desc, None),
                Some(Value::String(s)) if s == "_last" => (!field.desc, None),
                Some(Value::String(s)) if s == "_first" => (field.desc, None),
                Some(value) => {
                    let substitute = parse_sort_value(value, ty).ok_or_else(|| {
                        SearchError::BadRequest(format!(
                            "missing value {value} for [{name}] is not a valid {ty:?}"
                        ))
                    })?;
                    (!field.desc, Some(substitute))
                }
            };
            Ok(ResolvedSort {
                field: name.to_string(),
                target,
                ty,
                desc: field.desc,
                null_is_max,
                substitute,
            })
        })
        .collect()
}

fn text_fielddata(name: &str) -> SearchError {
    SearchError::BadRequest(format!(
        "Text fields are not optimised for operations that require per-document field data \
         like aggregations and sorting, so these operations are disabled by default. Please \
         use a keyword field instead. Alternatively, set fielddata=true on [{name}] in order \
         to load field data by uninverting the inverted index. Note that this can use \
         significant memory."
    ))
}

/// Parse a JSON sort value of the given type (a `search_after` element
/// or a `missing` substitute). `None` = not a value of that type.
/// OpenSearch's missing-value sentinels parse back as `Missing`.
pub fn parse_sort_value(value: &Value, ty: SortType) -> Option<SortValue> {
    if value.is_null() {
        return Some(SortValue::Missing);
    }
    let whole = |v: &Value| {
        v.as_i64().or_else(|| {
            v.as_f64()
                .filter(|f| f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0)
                .map(|f| f as i64)
        })
    };
    match ty {
        SortType::Keyword => Some(match value {
            Value::String(s) => SortValue::Str(s.clone()),
            other => SortValue::Str(other.to_string()),
        }),
        SortType::Long => {
            let n = whole(value).or_else(|| value.as_str().and_then(|s| s.parse().ok()))?;
            Some(if n == i64::MAX || n == i64::MIN { SortValue::Missing } else { SortValue::I64(n) })
        }
        SortType::Date => {
            let n = whole(value)
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                .or_else(|| crate::query_dsl::parse_time_millis(value))?;
            Some(if n == i64::MAX || n == i64::MIN { SortValue::Missing } else { SortValue::I64(n) })
        }
        SortType::Double => match value {
            Value::Number(n) => n.as_f64().map(SortValue::F64),
            Value::String(s) if s == "Infinity" || s == "-Infinity" => Some(SortValue::Missing),
            Value::String(s) => s.parse::<f64>().ok().map(SortValue::F64),
            _ => None,
        },
        SortType::Boolean => match value {
            Value::Bool(b) => Some(SortValue::Bool(*b)),
            Value::Number(n) => match n.as_i64()? {
                0 => Some(SortValue::Bool(false)),
                1 => Some(SortValue::Bool(true)),
                n if n == i32::MAX as i64 || n == i32::MIN as i64 => Some(SortValue::Missing),
                _ => None,
            },
            Value::String(s) => s.parse::<bool>().ok().map(SortValue::Bool),
            _ => None,
        },
        SortType::Ip => {
            let ip: std::net::IpAddr = value.as_str()?.parse().ok()?;
            Some(SortValue::Ip(match ip {
                std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                std::net::IpAddr::V6(v6) => v6,
            }))
        }
    }
}

/// A `search_after` cursor over a field sort: the previous page's sort
/// values, optionally followed by the implicit (timestamp, `_seq`)
/// tiebreak values the hits reported.
#[derive(Clone, Debug)]
pub struct FieldCursor {
    /// One value per sort field.
    pub values: Vec<SortValue>,
    /// Timestamp tiebreak (epoch millis), when passed back.
    pub timestamp_millis: Option<i64>,
    /// `_seq` tiebreak, when passed back.
    pub seq: Option<i64>,
}

impl FieldCursor {
    /// Parse the `search_after` array against the resolved sort: `n`
    /// values (the field values only: a page of equal keys is skipped,
    /// as in OpenSearch with a non-unique sort), or `n + 2` with the
    /// tiebreak values a hit reported (`n + 1` with the timestamp only).
    pub fn parse(values: &[Value], resolved: &[ResolvedSort]) -> SearchResult<Self> {
        let n = resolved.len();
        if values.len() < n || values.len() > n + 2 {
            return Err(SearchError::BadRequest(format!(
                "search_after has {} value(s) but sort has {n}.",
                values.len()
            )));
        }
        let mut parsed = Vec::with_capacity(n);
        for (value, sort) in values.iter().zip(resolved) {
            parsed.push(parse_sort_value(value, sort.ty).ok_or_else(|| {
                SearchError::BadRequest(format!(
                    "search_after value {value} for [{}] is not a valid {:?}",
                    sort.field, sort.ty
                ))
            })?);
        }
        let as_i64 = |v: &Value, what: &str| {
            parse_sort_value(v, SortType::Long)
                .and_then(|p| match p {
                    SortValue::I64(n) => Some(n),
                    SortValue::Missing => Some(-1),
                    _ => None,
                })
                .ok_or_else(|| {
                    SearchError::BadRequest(format!("search_after {what} must be an integer"))
                })
        };
        let timestamp_millis = values
            .get(n)
            .map(|v| as_i64(v, "timestamp tiebreak"))
            .transpose()?
            .map(|ms| ms.clamp(-rsearch_index::MAX_SAFE_MILLIS, rsearch_index::MAX_SAFE_MILLIS));
        let seq = values.get(n + 1).map(|v| as_i64(v, "_seq tiebreak")).transpose()?;
        Ok(Self {
            values: parsed,
            timestamp_millis,
            seq,
        })
    }
}

/// The complete field-sort plan for one search.
#[derive(Clone, Debug)]
pub struct FieldSortPlan {
    /// Resolved clauses, in order.
    pub fields: Vec<ResolvedSort>,
    /// Resume strictly past this position.
    pub cursor: Option<FieldCursor>,
}

impl FieldSortPlan {
    /// Per-field direction and missing placement.
    pub fn orders(&self) -> Vec<FieldOrder> {
        self.fields.iter().map(ResolvedSort::order).collect()
    }

    /// The `sort` values a hit reports: the field values, then the
    /// (timestamp, `_seq`) tiebreak.
    pub fn hit_sort_json(&self, values: &[SortValue], ts: i64, seq: i64) -> Value {
        let mut out: Vec<Value> = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                values
                    .get(i)
                    .unwrap_or(&SortValue::Missing)
                    .to_json(f.ty, f.null_is_max)
            })
            .collect();
        out.push(json!(ts));
        out.push(json!(seq));
        Value::Array(out)
    }
}

/// A hit as collected by [`FieldSortCollector`].
#[derive(Clone, Debug)]
pub struct SortedHit {
    /// Materialized sort values, one per field.
    pub values: Vec<SortValue>,
    /// Timestamp tiebreak (epoch millis).
    pub timestamp_millis: i64,
    /// `_seq` tiebreak (-1 on legacy splits).
    pub seq: i64,
    /// The document.
    pub doc: DocAddress,
}

/// Cheap per-segment key: term ordinals stand in for strings (doubled,
/// with +1 for an exact term, so a cursor string absent from the
/// dictionary sits between two ordinals), numbers and booleans as they
/// are. Comparable only within one segment.
#[derive(Clone, Copy, Debug, PartialEq)]
enum SegKey {
    Missing,
    Ord(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Ip(u128),
}

impl SegKey {
    fn cmp_natural(&self, other: &Self, null_is_max: bool) -> Ordering {
        match (self, other) {
            (SegKey::Missing, SegKey::Missing) => Ordering::Equal,
            (SegKey::Missing, _) => {
                if null_is_max { Ordering::Greater } else { Ordering::Less }
            }
            (_, SegKey::Missing) => {
                if null_is_max { Ordering::Less } else { Ordering::Greater }
            }
            (SegKey::Ord(a), SegKey::Ord(b)) => a.cmp(b),
            (SegKey::I64(a), SegKey::I64(b)) => a.cmp(b),
            (SegKey::F64(a), SegKey::F64(b)) => a.total_cmp(b),
            (SegKey::I64(a), SegKey::F64(b)) => (*a as f64).total_cmp(b),
            (SegKey::F64(a), SegKey::I64(b)) => a.total_cmp(&(*b as f64)),
            (SegKey::Bool(a), SegKey::Bool(b)) => a.cmp(b),
            (SegKey::Ip(a), SegKey::Ip(b)) => a.cmp(b),
            // Mixed kinds only arise from a cursor typed differently from
            // the segment's column; order them by kind, consistently.
            (a, b) => a.kind().cmp(&b.kind()),
        }
    }

    fn kind(&self) -> u8 {
        match self {
            SegKey::Missing => 0,
            SegKey::Bool(_) => 1,
            SegKey::I64(_) | SegKey::F64(_) => 2,
            SegKey::Ord(_) => 3,
            SegKey::Ip(_) => 4,
        }
    }
}

/// How one sort field reads its value in one segment.
enum SegAccessor {
    /// The field has no column here: every document is missing.
    None,
    Str(StrColumn),
    I64(Column<i64>),
    F64(Column<f64>),
    Bool(Column<bool>),
    Date(Column<tantivy::DateTime>),
    Ip(Column<Ipv6Addr>),
}

impl SegAccessor {
    /// The document's key: min of its values for asc, max for desc.
    fn key(&self, doc: DocId, desc: bool) -> SegKey {
        fn pick<T: Copy, I: Iterator<Item = T>>(
            iter: I,
            desc: bool,
            cmp: impl Fn(&T, &T) -> Ordering,
        ) -> Option<T> {
            if desc {
                iter.max_by(|a, b| cmp(a, b))
            } else {
                iter.min_by(|a, b| cmp(a, b))
            }
        }
        match self {
            SegAccessor::None => SegKey::Missing,
            SegAccessor::Str(col) => pick(col.term_ords(doc), desc, |a, b| a.cmp(b))
                .map(|o| SegKey::Ord(o * 2 + 1))
                .unwrap_or(SegKey::Missing),
            SegAccessor::I64(col) => pick(col.values_for_doc(doc), desc, |a, b| a.cmp(b))
                .map(SegKey::I64)
                .unwrap_or(SegKey::Missing),
            SegAccessor::F64(col) => pick(col.values_for_doc(doc), desc, |a, b| a.total_cmp(b))
                .map(SegKey::F64)
                .unwrap_or(SegKey::Missing),
            SegAccessor::Bool(col) => pick(col.values_for_doc(doc), desc, |a, b| a.cmp(b))
                .map(SegKey::Bool)
                .unwrap_or(SegKey::Missing),
            SegAccessor::Date(col) => pick(
                col.values_for_doc(doc).map(|d| d.into_timestamp_millis()),
                desc,
                |a, b| a.cmp(b),
            )
            .map(SegKey::I64)
            .unwrap_or(SegKey::Missing),
            SegAccessor::Ip(col) => pick(col.values_for_doc(doc), desc, |a, b| a.cmp(b))
                .map(|ip| SegKey::Ip(ip.to_bits()))
                .unwrap_or(SegKey::Missing),
        }
    }

    /// A materialized value's position in this segment's key space.
    fn key_for(&self, value: &SortValue) -> SegKey {
        match (self, value) {
            (_, SortValue::Missing) => SegKey::Missing,
            (SegAccessor::Str(col), SortValue::Str(s)) => {
                match col.dictionary().term_ord_or_next(s.as_bytes()) {
                    Ok(TermOrdHit::Exact(o)) => SegKey::Ord(o * 2 + 1),
                    Ok(TermOrdHit::Next(o)) => SegKey::Ord(o.saturating_mul(2)),
                    Err(_) => SegKey::Ord(0),
                }
            }
            (_, SortValue::Str(_)) => SegKey::Ord(0),
            (_, SortValue::I64(n)) => SegKey::I64(*n),
            (_, SortValue::F64(f)) => SegKey::F64(*f),
            (_, SortValue::Bool(b)) => SegKey::Bool(*b),
            (_, SortValue::Ip(ip)) => SegKey::Ip(ip.to_bits()),
        }
    }

    /// Materialize a key collected from this segment.
    fn materialize(&self, key: SegKey) -> SortValue {
        match (self, key) {
            (_, SegKey::Missing) => SortValue::Missing,
            (SegAccessor::Str(col), SegKey::Ord(o)) => {
                let mut out = String::new();
                match col.ord_to_str(o / 2, &mut out) {
                    Ok(true) => SortValue::Str(out),
                    _ => SortValue::Missing,
                }
            }
            (_, SegKey::Ord(_)) => SortValue::Missing,
            (_, SegKey::I64(n)) => SortValue::I64(n),
            (_, SegKey::F64(f)) => SortValue::F64(f),
            (_, SegKey::Bool(b)) => SortValue::Bool(b),
            (_, SegKey::Ip(bits)) => SortValue::Ip(Ipv6Addr::from_bits(bits)),
        }
    }
}

/// Open the column a resolved sort reads in `segment`, under the split's
/// own schema (a field mapped after the split was written is read from
/// its dynamic path there).
fn open_accessor(
    segment: &SegmentReader,
    schema: &MappedSchema,
    sort: &ResolvedSort,
) -> SegAccessor {
    let ff = segment.fast_fields();
    let typed = |name: &str, ty: SortType| -> SegAccessor {
        match ty {
            SortType::Keyword => ff.str(name).ok().flatten().map(SegAccessor::Str),
            SortType::Long => ff.column_opt::<i64>(name).ok().flatten().map(SegAccessor::I64),
            SortType::Double => ff.column_opt::<f64>(name).ok().flatten().map(SegAccessor::F64),
            SortType::Boolean => ff.column_opt::<bool>(name).ok().flatten().map(SegAccessor::Bool),
            SortType::Date => ff
                .column_opt::<tantivy::DateTime>(name)
                .ok()
                .flatten()
                .map(SegAccessor::Date),
            SortType::Ip => ff.column_opt::<Ipv6Addr>(name).ok().flatten().map(SegAccessor::Ip),
        }
        .unwrap_or(SegAccessor::None)
    };
    // A numeric path in `_dynamic` may hold i64, u64 and f64 columns;
    // read them as one numeric column of the requested kind.
    let dynamic_numeric = |path: &str, ty: SortType| -> SegAccessor {
        let name = format!("{}.{path}", rsearch_index::DYNAMIC_FIELD);
        match ty {
            SortType::Boolean => typed(&name, SortType::Boolean),
            SortType::Long | SortType::Double | SortType::Date => {
                let handles = ff.dynamic_column_handles(&name).unwrap_or_default();
                let numeric = handles
                    .into_iter()
                    .find(|h| h.column_type().numerical_type().is_some());
                let Some(handle) = numeric else { return SegAccessor::None };
                let Ok(column) = handle.open() else { return SegAccessor::None };
                let target = if ty == SortType::Double {
                    tantivy::columnar::NumericalType::F64
                } else {
                    tantivy::columnar::NumericalType::I64
                };
                match column.coerce_numerical(target) {
                    Some(DynamicColumn::I64(col)) => SegAccessor::I64(col),
                    Some(DynamicColumn::F64(col)) => SegAccessor::F64(col),
                    _ => SegAccessor::None,
                }
            }
            _ => SegAccessor::None,
        }
    };
    match &sort.target {
        SortTarget::Timestamp => typed(rsearch_index::TIMESTAMP_FIELD, SortType::Date),
        SortTarget::Seq => typed(rsearch_index::SEQ_FIELD, SortType::Long),
        SortTarget::Mapped(name) => {
            if schema.fields.contains_key(name) {
                typed(name, sort.ty)
            } else if sort.ty == SortType::Keyword {
                typed(&format!("{}.{name}", rsearch_index::DYNAMIC_RAW_FIELD), sort.ty)
            } else {
                dynamic_numeric(name, sort.ty)
            }
        }
        SortTarget::DynamicKeyword(path) => {
            typed(&format!("{}.{path}", rsearch_index::DYNAMIC_RAW_FIELD), SortType::Keyword)
        }
        SortTarget::DynamicPath(path) => dynamic_numeric(path, sort.ty),
        SortTarget::Unmapped => SegAccessor::None,
    }
}

/// Top-k collector ordered by a field sort, with the cursor applied per
/// document. Fruit: the split's page candidates in result order.
pub struct FieldSortCollector {
    plan: Arc<FieldSortPlan>,
    schema: Arc<MappedSchema>,
    limit: usize,
}

impl FieldSortCollector {
    /// Collect at most `limit` hits ordered by `plan` from a split built
    /// under `schema`.
    pub fn new(plan: Arc<FieldSortPlan>, schema: Arc<MappedSchema>, limit: usize) -> Self {
        Self {
            plan,
            schema,
            limit: limit.max(1),
        }
    }
}

/// A collected document, ordered by the segment's key space.
struct Ranked {
    keys: Vec<SegKey>,
    ts: i64,
    seq: i64,
    doc: DocId,
}

/// Compare in result order (Less = earlier), tiebreak by doc id.
fn cmp_ranked(orders: &[FieldOrder], a: &Ranked, b: &Ranked) -> Ordering {
    for (i, order) in orders.iter().enumerate() {
        let natural = a.keys[i].cmp_natural(&b.keys[i], order.null_is_max);
        let directed = if order.desc { natural.reverse() } else { natural };
        if directed != Ordering::Equal {
            return directed;
        }
    }
    b.ts.cmp(&a.ts)
        .then(b.seq.cmp(&a.seq))
        .then(a.doc.cmp(&b.doc))
}

/// Heap element: the result-order comparison makes the heap's max the
/// last-ranked entry, i.e. the one to evict.
struct HeapItem {
    ranked: Ranked,
    orders: Arc<[FieldOrder]>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_ranked(&self.orders, &self.ranked, &other.ranked)
    }
}

/// Per-segment state of [`FieldSortCollector`].
pub struct FieldSortSegmentCollector {
    accessors: Vec<SegAccessor>,
    fields: Arc<FieldSortPlan>,
    orders: Arc<[FieldOrder]>,
    /// Cursor in this segment's key space, with the tiebreak values.
    cursor: Option<(Vec<SegKey>, Option<i64>, Option<i64>)>,
    /// Substitute keys for `missing: <value>` fields, per field.
    substitutes: Vec<Option<SegKey>>,
    ts: Option<Column<tantivy::DateTime>>,
    seq: Option<Column<i64>>,
    segment_ord: SegmentOrdinal,
    limit: usize,
    heap: BinaryHeap<HeapItem>,
}

impl FieldSortSegmentCollector {
    fn keys_for(&self, doc: DocId) -> Vec<SegKey> {
        self.accessors
            .iter()
            .zip(&self.fields.fields)
            .zip(&self.substitutes)
            .map(|((accessor, sort), substitute)| {
                let key = accessor.key(doc, sort.desc);
                match (key, substitute) {
                    (SegKey::Missing, Some(sub)) => *sub,
                    (key, _) => key,
                }
            })
            .collect()
    }

    /// Strictly past the cursor in result order?
    fn after_cursor(&self, ranked: &Ranked) -> bool {
        let Some((keys, ts, seq)) = &self.cursor else { return true };
        for (i, order) in self.orders.iter().enumerate() {
            let natural = ranked.keys[i].cmp_natural(&keys[i], order.null_is_max);
            let directed = if order.desc { natural.reverse() } else { natural };
            match directed {
                Ordering::Less => return false,
                Ordering::Greater => return true,
                Ordering::Equal => {}
            }
        }
        let Some(cursor_ts) = ts else { return false };
        // Tiebreak: timestamp desc, then seq desc.
        match cursor_ts.cmp(&ranked.ts) {
            Ordering::Less => false,
            Ordering::Greater => true,
            Ordering::Equal => match seq {
                Some(cursor_seq) => ranked.seq < *cursor_seq,
                None => false,
            },
        }
    }
}

impl Collector for FieldSortCollector {
    type Fruit = Vec<SortedHit>;
    type Child = FieldSortSegmentCollector;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let accessors: Vec<SegAccessor> = self
            .plan
            .fields
            .iter()
            .map(|sort| open_accessor(segment, &self.schema, sort))
            .collect();
        let substitutes = accessors
            .iter()
            .zip(&self.plan.fields)
            .map(|(accessor, sort)| sort.substitute.as_ref().map(|v| accessor.key_for(v)))
            .collect();
        let cursor = self.plan.cursor.as_ref().map(|c| {
            let keys = accessors
                .iter()
                .zip(&c.values)
                .map(|(accessor, value)| accessor.key_for(value))
                .collect();
            (keys, c.timestamp_millis, c.seq)
        });
        let ff = segment.fast_fields();
        Ok(FieldSortSegmentCollector {
            accessors,
            fields: self.plan.clone(),
            orders: self.plan.orders().into(),
            cursor,
            substitutes,
            ts: ff.column_opt::<tantivy::DateTime>(rsearch_index::TIMESTAMP_FIELD)?,
            seq: ff.column_opt::<i64>(rsearch_index::SEQ_FIELD)?,
            segment_ord,
            limit: self.limit,
            heap: BinaryHeap::with_capacity(self.limit + 1),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Vec<SortedHit>>) -> tantivy::Result<Self::Fruit> {
        let orders = self.plan.orders();
        let mut all: Vec<SortedHit> = segment_fruits.into_iter().flatten().collect();
        all.sort_by(|a, b| {
            cmp_hits(
                &orders,
                (&a.values, a.timestamp_millis, a.seq),
                (&b.values, b.timestamp_millis, b.seq),
            )
            .then(a.doc.cmp(&b.doc))
        });
        all.truncate(self.limit);
        Ok(all)
    }
}

impl SegmentCollector for FieldSortSegmentCollector {
    type Fruit = Vec<SortedHit>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let ranked = Ranked {
            keys: self.keys_for(doc),
            ts: self
                .ts
                .as_ref()
                .and_then(|c| c.first(doc))
                .map(|d| d.into_timestamp_millis())
                .unwrap_or_default(),
            seq: self.seq.as_ref().and_then(|c| c.first(doc)).unwrap_or(-1),
            doc,
        };
        if !self.after_cursor(&ranked) {
            return;
        }
        if self.heap.len() < self.limit {
            self.heap.push(HeapItem {
                ranked,
                orders: self.orders.clone(),
            });
            return;
        }
        if let Some(top) = self.heap.peek()
            && cmp_ranked(&self.orders, &ranked, &top.ranked) == Ordering::Less
        {
            self.heap.pop();
            self.heap.push(HeapItem {
                ranked,
                orders: self.orders.clone(),
            });
        }
    }

    fn harvest(self) -> Self::Fruit {
        let mut out: Vec<SortedHit> = self
            .heap
            .into_sorted_vec()
            .into_iter()
            .map(|item| {
                let values = item
                    .ranked
                    .keys
                    .iter()
                    .zip(&self.accessors)
                    .zip(&self.fields.fields)
                    .zip(&self.substitutes)
                    .map(|(((key, accessor), sort), substitute)| {
                        if substitute.is_some_and(|s| s == *key) {
                            sort.substitute.clone().unwrap_or(SortValue::Missing)
                        } else {
                            accessor.materialize(*key)
                        }
                    })
                    .collect();
                SortedHit {
                    values,
                    timestamp_millis: item.ranked.ts,
                    seq: item.ranked.seq,
                    doc: DocAddress::new(self.segment_ord, item.ranked.doc),
                }
            })
            .collect();
        // into_sorted_vec is ascending by Ord, i.e. result order already.
        out.shrink_to_fit();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsearch_index::{DocIdentity, DocumentConverter, IndexMapping};
    use tantivy::query::AllQuery;

    fn index(mapping: Value, docs: &[Value]) -> (Arc<MappedSchema>, tantivy::Index) {
        let schema = MappedSchema::build(IndexMapping::from_json(&mapping).unwrap());
        let index = schema.create_in_ram();
        let converter = DocumentConverter::new(schema.clone());
        let mut writer = index.writer_with_num_threads(1, 20 << 20).unwrap();
        for (i, doc) in docs.iter().enumerate() {
            let identity = DocIdentity::new(format!("d{i}"), i as i64 + 1);
            let (doc, _) = converter
                .convert_with_source(
                    doc.clone(),
                    None,
                    &identity,
                    tantivy::DateTime::from_timestamp_millis(1_000 + i as i64),
                )
                .unwrap();
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
        (Arc::new(schema), index)
    }

    fn dynamic(entries: &[(&str, &[&str])]) -> std::collections::BTreeMap<String, Vec<String>> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    fn run(
        index: &tantivy::Index,
        schema: &Arc<MappedSchema>,
        plan: FieldSortPlan,
        limit: usize,
    ) -> Vec<SortedHit> {
        let searcher = index.reader().unwrap().searcher();
        searcher
            .search(
                &AllQuery,
                &FieldSortCollector::new(Arc::new(plan), schema.clone(), limit),
            )
            .unwrap()
    }

    fn ids(hits: &[SortedHit]) -> Vec<u32> {
        hits.iter().map(|h| h.doc.doc_id).collect()
    }

    const MAPPING: &str = r#"{"properties": {"name": {"type": "keyword"}, "age": {"type": "long"},
        "score": {"type": "double"}, "ok": {"type": "boolean"}, "born": {"type": "date"},
        "bio": {"type": "text"}, "ip": {"type": "ip"}}}"#;

    fn docs() -> Vec<Value> {
        vec![
            json!({"name": "bob", "age": 30, "score": 1.5, "ok": true, "born": "1990-01-01T00:00:00Z", "role": "tech_admin", "n": 2, "ip": "10.0.0.2"}),
            json!({"name": "alice", "age": 25, "score": 2.0, "ok": false, "born": "1995-06-01T00:00:00Z", "role": "user", "n": 1.5, "ip": "10.0.0.1"}),
            json!({"name": "carol", "bio": "e f", "n": 3}),
        ]
    }

    fn resolve(sort: Value, schema: &MappedSchema) -> SearchResult<Vec<ResolvedSort>> {
        let entries: Vec<Value> = match sort {
            Value::Array(items) => items,
            single => vec![single],
        };
        let fields: Vec<SortField> = entries
            .iter()
            .filter_map(|e| SortField::parse(e).transpose())
            .collect::<SearchResult<_>>()?;
        resolve_sort(
            &fields,
            schema,
            &dynamic(&[("role", &["string"]), ("n", &["long", "double"])]),
        )
    }

    #[test]
    fn parses_sort_entries() {
        let f = SortField::parse(&json!("name")).unwrap().unwrap();
        assert_eq!((f.field.as_str(), f.desc), ("name", false));
        let f = SortField::parse(&json!({"@timestamp": "asc"})).unwrap().unwrap();
        assert_eq!((f.field.as_str(), f.desc), ("@timestamp", false));
        let f = SortField::parse(&json!({"age": {"order": "desc", "missing": "_first", "unmapped_type": "long", "mode": "max"}}))
            .unwrap()
            .unwrap();
        assert!(f.desc && f.missing == Some(json!("_first")) && f.unmapped_type.as_deref() == Some("long"));
        assert!(SortField::parse(&json!("_score")).unwrap().is_none());
        assert!(SortField::parse(&json!({"_doc": "asc"})).unwrap().is_none());
        for bad in [
            json!({"age": "sideways"}),
            json!({"age": {"order": "asc", "nested": {}}}),
            json!({"age": {"mode": "avg"}}),
            json!(42),
        ] {
            assert!(SortField::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn resolves_like_opensearch() {
        let (schema, _) = index(serde_json::from_str(MAPPING).unwrap(), &[]);
        let ok = |sort: Value| resolve(sort, &schema).unwrap();
        assert_eq!(ok(json!([{"name": "asc"}]))[0].ty, SortType::Keyword);
        assert_eq!(ok(json!([{"age": "asc"}]))[0].ty, SortType::Long);
        assert_eq!(ok(json!([{"born": "asc"}]))[0].ty, SortType::Date);
        assert_eq!(ok(json!([{"ip": "asc"}]))[0].ty, SortType::Ip);
        assert_eq!(ok(json!([{"role.keyword": "asc"}]))[0].target, SortTarget::DynamicKeyword("role".into()));
        assert_eq!(ok(json!([{"n": "asc"}]))[0].ty, SortType::Double);
        assert_eq!(ok(json!([{"zzz": {"unmapped_type": "long"}}]))[0].target, SortTarget::Unmapped);
        // Missing placement: last in both directions unless _first.
        assert!(ok(json!([{"age": "asc"}]))[0].null_is_max);
        assert!(!ok(json!([{"age": "desc"}]))[0].null_is_max);
        assert!(!ok(json!([{"age": {"order": "asc", "missing": "_first"}}]))[0].null_is_max);
        assert_eq!(
            ok(json!([{"age": {"order": "asc", "missing": 27}}]))[0].substitute,
            Some(SortValue::I64(27))
        );
        let err = |sort: Value| resolve(sort, &schema).unwrap_err().to_string();
        assert!(err(json!([{"bio": "asc"}])).starts_with("Text fields are not optimised"));
        assert!(err(json!([{"role": "asc"}])).starts_with("Text fields are not optimised"));
        assert_eq!(err(json!([{"zzz": "asc"}])), "No mapping found for [zzz] in order to sort on");
        assert_eq!(err(json!([{"name.keyword": "asc"}])), "No mapping found for [name.keyword] in order to sort on");
    }

    #[test]
    fn sorts_every_type_with_missing_last() {
        let (schema, index) = index(serde_json::from_str(MAPPING).unwrap(), &docs());
        let plan = |sort: Value| FieldSortPlan {
            fields: resolve(sort, &schema).unwrap(),
            cursor: None,
        };
        // docs: 0=bob 1=alice 2=carol(missing most fields)
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"name": "asc"}])), 10)), vec![1, 0, 2]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"name": "desc"}])), 10)), vec![2, 0, 1]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"age": "asc"}])), 10)), vec![1, 0, 2]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"age": "desc"}])), 10)), vec![0, 1, 2]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"age": {"order": "asc", "missing": "_first"}}])), 10)), vec![2, 1, 0]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"age": {"order": "asc", "missing": 27}}])), 10)), vec![1, 2, 0]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"score": "desc"}])), 10)), vec![1, 0, 2]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"ok": "asc"}])), 10)), vec![1, 0, 2]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"born": "asc"}])), 10)), vec![0, 1, 2]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"ip": "desc"}])), 10)), vec![0, 1, 2]);
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"role.keyword": "desc"}])), 10)), vec![1, 0, 2]);
        // Dynamic numeric path mixing integers and fractions.
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"n": "asc"}])), 10)), vec![1, 0, 2]);
        // Unmapped: all missing, falls to the (ts desc) tiebreak.
        assert_eq!(ids(&run(&index, &schema, plan(json!([{"zzz": {"order": "asc", "unmapped_type": "long"}}])), 10)), vec![2, 1, 0]);
        // Reported values.
        let hits = run(&index, &schema, plan(json!([{"age": "asc"}])), 10);
        let p = plan(json!([{"age": "asc"}]));
        assert_eq!(p.hit_sort_json(&hits[0].values, hits[0].timestamp_millis, hits[0].seq), json!([25, 1001, 2]));
        assert_eq!(p.hit_sort_json(&hits[2].values, hits[2].timestamp_millis, hits[2].seq), json!([i64::MAX, 1002, 3]));
        let hits = run(&index, &schema, plan(json!([{"role.keyword": "desc"}])), 10);
        let p = plan(json!([{"role.keyword": "desc"}]));
        assert_eq!(p.hit_sort_json(&hits[2].values, hits[2].timestamp_millis, hits[2].seq), json!([Value::Null, 1002, 3]));
        let hits = run(&index, &schema, plan(json!([{"score": "desc"}])), 10);
        let p = plan(json!([{"score": "desc"}]));
        assert_eq!(p.hit_sort_json(&hits[2].values, hits[2].timestamp_millis, hits[2].seq), json!(["-Infinity", 1002, 3]));
        let hits = run(&index, &schema, plan(json!([{"ip": "desc"}])), 10);
        let p = plan(json!([{"ip": "desc"}]));
        assert_eq!(p.hit_sort_json(&hits[0].values, hits[0].timestamp_millis, hits[0].seq), json!(["10.0.0.2", 1000, 1]));
    }

    #[test]
    fn cursor_pages_without_gaps_or_repeats() {
        let (schema, index) = index(serde_json::from_str(MAPPING).unwrap(), &docs());
        let resolved = resolve(json!([{"age": "asc"}, {"name": "desc"}]), &schema).unwrap();
        let mut seen = Vec::new();
        let mut cursor: Option<FieldCursor> = None;
        loop {
            let plan = FieldSortPlan {
                fields: resolved.clone(),
                cursor: cursor.clone(),
            };
            let page = run(&index, &schema, plan.clone(), 1);
            let Some(hit) = page.first() else { break };
            seen.push(hit.doc.doc_id);
            let sort = plan.hit_sort_json(&hit.values, hit.timestamp_millis, hit.seq);
            cursor = Some(FieldCursor::parse(sort.as_array().unwrap(), &resolved).unwrap());
            assert!(seen.len() <= 3, "looped");
        }
        assert_eq!(seen, vec![1, 0, 2]);
        // Field-only cursor (as an OpenSearch client would pass) at the
        // missing sentinel: the missing doc is not repeated.
        let cursor = FieldCursor::parse(&[json!(i64::MAX), json!(Value::Null)], &resolved).unwrap();
        assert_eq!(cursor.values, vec![SortValue::Missing, SortValue::Missing]);
        let plan = FieldSortPlan { fields: resolved.clone(), cursor: Some(cursor) };
        assert!(run(&index, &schema, plan, 10).is_empty());
        // Wrong arity.
        assert!(FieldCursor::parse(&[json!(1)], &resolved).is_err());
        assert!(FieldCursor::parse(&[json!(1), json!("a"), json!(1), json!(1), json!(1)], &resolved).is_err());
        // A string cursor between two dictionary terms resumes correctly.
        let resolved = resolve(json!([{"name": "asc"}]), &schema).unwrap();
        let cursor = FieldCursor::parse(&[json!("b")], &resolved).unwrap();
        let plan = FieldSortPlan { fields: resolved, cursor: Some(cursor) };
        assert_eq!(ids(&run(&index, &schema, plan, 10)), vec![0, 2]);
    }

    #[test]
    fn heap_keeps_the_right_end_of_large_groups() {
        let docs: Vec<Value> = (0..50).map(|i| json!({"age": i % 5, "name": format!("n{i:02}")})).collect();
        let (schema, index) = index(serde_json::from_str(MAPPING).unwrap(), &docs);
        let resolved = resolve(json!([{"age": "desc"}, {"name": "asc"}]), &schema).unwrap();
        let plan = FieldSortPlan { fields: resolved, cursor: None };
        let hits = run(&index, &schema, plan, 3);
        let names: Vec<String> = hits
            .iter()
            .map(|h| match &h.values[1] {
                SortValue::Str(s) => s.clone(),
                _ => unreachable!(),
            })
            .collect();
        // age 4 is docs 4, 9, 14, …: names n04, n09, n14 first.
        assert_eq!(names, vec!["n04", "n09", "n14"]);
    }
}
