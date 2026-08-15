//! ES/OpenSearch query DSL subset → Tantivy queries.
//!
//! Field name resolution: `@timestamp`/`timestamp` map to the reserved
//! `_timestamp` fast field; mapped fields use their typed Tantivy field;
//! anything else becomes a JSON-path lookup in the `_dynamic` field.

use std::net::IpAddr;
use std::ops::Bound;

use serde_json::Value;
use tantivy::query::{
    AllQuery, BooleanQuery, ExistsQuery, Occur, PhraseQuery, Query, QueryParser, RangeQuery,
    TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption, Term};
use tantivy::time::OffsetDateTime;
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::tokenizer::TextAnalyzer;

use rsearch_index::{FieldType, MappedSchema};

use crate::error::{SearchError, SearchResult};

const TIMESTAMP_ALIASES: [&str; 3] = ["@timestamp", "timestamp", "_timestamp"];

/// Where a queried field lands in the schema.
enum Resolved {
    Timestamp(Field),
    Typed(Field, FieldType),
    Dynamic(Field, String),
}

fn resolve(schema: &MappedSchema, name: &str) -> Resolved {
    if TIMESTAMP_ALIASES.contains(&name) {
        return Resolved::Timestamp(schema.timestamp);
    }
    if let Some((field, ty)) = schema.fields.get(name) {
        return Resolved::Typed(*field, *ty);
    }
    Resolved::Dynamic(schema.dynamic, name.to_string())
}

/// Rewrite aggregation field names the same way queries resolve them, so
/// ES-shaped aggregation requests hit the right Tantivy columns. Also
/// sanitizes parameters ES tolerates but Tantivy rejects (display-only
/// `format`, `time_zone`) and maps the legacy `interval` to
/// `fixed_interval` — Grafana sends all three.
pub fn rewrite_agg_fields(schema: &MappedSchema, aggs: &Value) -> Value {
    match aggs {
        Value::Object(map) => {
            // Only strip display-only params inside an agg parameter block
            // (identified by a sibling "field" key) — never at the aggs
            // level, where "format" could be a user's aggregation name (L4).
            let is_param_block = map.contains_key("field");
            Value::Object(
            map.iter()
                .filter(|(k, _)| {
                    !(is_param_block && (k.as_str() == "format" || k.as_str() == "time_zone"))
                })
                .map(|(k, v)| {
                    if k == "field"
                        && let Some(name) = v.as_str()
                    {
                        let rewritten = match resolve(schema, name) {
                            Resolved::Timestamp(_) => "_timestamp".to_string(),
                            Resolved::Typed(..) => name.to_string(),
                            Resolved::Dynamic(_, path) => format!("_dynamic.{path}"),
                        };
                        (k.clone(), Value::String(rewritten))
                    } else if k == "interval" && v.is_string() {
                        ("fixed_interval".to_string(), v.clone())
                    } else {
                        (k.clone(), rewrite_agg_fields(schema, v))
                    }
                })
                .collect(),
            )
        }
        Value::Array(items) => Value::Array(
            items.iter().map(|v| rewrite_agg_fields(schema, v)).collect(),
        ),
        other => other.clone(),
    }
}

/// Tantivy serializes every histogram bucket key as a JSON float, but
/// Elasticsearch returns integer epoch millis for `date_histogram` — and
/// clients that read the key with `as_i64()` see every bucket as missing
/// otherwise (issue #32). Walk the final aggregation response alongside
/// the aggs request and convert whole-number `date_histogram` keys to
/// integers. Plain `histogram` keys stay doubles, matching ES.
pub fn fix_date_histogram_keys(aggs_request: &Value, response: &mut Value) {
    let Some(requests) = aggs_request.as_object() else {
        return;
    };
    for (name, spec) in requests {
        let (Some(spec), Some(result)) = (spec.as_object(), response.get_mut(name)) else {
            continue;
        };
        let is_date_histogram = spec.contains_key("date_histogram");
        let sub_aggs = spec.get("aggs").or_else(|| spec.get("aggregations"));
        let buckets: Option<Vec<&mut Value>> = match result.get_mut("buckets") {
            Some(Value::Array(items)) => Some(items.iter_mut().collect()),
            // Keyed buckets (e.g. `filters`, keyed ranges) come back as an
            // object of name -> bucket.
            Some(Value::Object(map)) => Some(map.values_mut().collect()),
            _ => None,
        };
        match buckets {
            Some(buckets) => {
                for bucket in buckets {
                    if is_date_histogram
                        && let Some(key) = bucket.get_mut("key")
                        && let Some(millis) = float_as_exact_i64(key)
                    {
                        *key = Value::from(millis);
                    }
                    if let Some(sub) = sub_aggs {
                        fix_date_histogram_keys(sub, bucket);
                    }
                }
            }
            // Single-bucket aggs (e.g. `filter`) nest sub-agg results
            // directly on the result object.
            None => {
                if let Some(sub) = sub_aggs {
                    fix_date_histogram_keys(sub, result);
                }
            }
        }
    }
}

/// A JSON float's value as i64, when it is whole and exactly
/// representable (within 2^53). Values already serialized as integers
/// return `None` — nothing to rewrite.
fn float_as_exact_i64(value: &Value) -> Option<i64> {
    if value.is_i64() || value.is_u64() {
        return None;
    }
    let f = value.as_f64()?;
    (f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0).then_some(f as i64)
}

/// Parse a timestamp literal (RFC 3339, epoch secs/millis, or "now").
fn parse_time_millis(value: &Value) -> Option<i64> {
    match value {
        Value::String(s) => {
            if s == "now" {
                return Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .ok()?
                        .as_millis() as i64,
                );
            }
            if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
                return Some((dt.unix_timestamp_nanos() / 1_000_000) as i64);
            }
            s.parse::<i64>().ok().map(rsearch_index::epoch_to_millis)
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(rsearch_index::epoch_to_millis(i))
            } else {
                // A float epoch is fractional SECONDS — convert directly to
                // millis; do not re-run the unit heuristic (which would
                // double-scale small pre-1973 values). L3.
                n.as_f64()
                    .filter(|f| f.is_finite())
                    .map(|f| (f * 1000.0) as i64)
            }
        }
        _ => None,
    }
}

/// Scan a query tree for range bounds on the timestamp field, for split
/// pruning. Returns (start_millis, end_millis), either side open.
pub fn extract_time_bounds(query: &Value) -> (Option<i64>, Option<i64>) {
    let mut start = None;
    let mut end = None;
    scan_time_bounds(query, &mut start, &mut end);
    (start, end)
}

/// Only harvest bounds from conjunctive context: a `range` at the top
/// level or under `must`/`filter`. A timestamp range under `should` (OR)
/// or `must_not` (negation) must NOT prune splits — doing so drops
/// matching documents (H3/H4). Recursion into `should`/`must_not` stops
/// bound collection for that subtree.
fn scan_time_bounds(node: &Value, start: &mut Option<i64>, end: &mut Option<i64>) {
    match node {
        Value::Object(map) => {
            if let Some(range) = map.get("range").and_then(Value::as_object) {
                for (field, bounds) in range {
                    if TIMESTAMP_ALIASES.contains(&field.as_str())
                        && let Some(bounds) = bounds.as_object()
                    {
                        for (op, value) in bounds {
                            match (op.as_str(), parse_time_millis(value)) {
                                ("gte" | "gt", Some(ms)) => {
                                    *start = Some(start.map_or(ms, |s: i64| s.max(ms)));
                                }
                                ("lte" | "lt", Some(ms)) => {
                                    *end = Some(end.map_or(ms, |e: i64| e.min(ms)));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            if let Some(bool_body) = map.get("bool").and_then(Value::as_object) {
                // Recurse only into conjunctive clauses.
                for key in ["must", "filter"] {
                    if let Some(clause) = bool_body.get(key) {
                        scan_time_bounds(clause, start, end);
                    }
                }
                // should / must_not deliberately skipped.
                return;
            }
            for value in map.values() {
                scan_time_bounds(value, start, end);
            }
        }
        Value::Array(items) => {
            for item in items {
                scan_time_bounds(item, start, end);
            }
        }
        _ => {}
    }
}

/// Translate an ES query object into a Tantivy query against `schema`.
/// `index` supplies tokenizers for match/query_string queries.
pub fn translate_query(
    index: &tantivy::Index,
    schema: &MappedSchema,
    query: &Value,
) -> SearchResult<Box<dyn Query>> {
    let obj = query
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("query must be an object".into()))?;
    let (kind, body) = obj
        .iter()
        .next()
        .ok_or_else(|| SearchError::BadRequest("empty query object".into()))?;
    if obj.len() > 1 {
        return Err(SearchError::BadRequest(format!(
            "query object must have exactly one key, found {}",
            obj.len()
        )));
    }

    match kind.as_str() {
        "match_all" => Ok(Box::new(AllQuery)),
        "bool" => translate_bool(index, schema, body),
        "term" => translate_term(schema, body),
        "terms" => translate_terms(schema, body),
        "range" => translate_range(schema, body),
        "exists" => translate_exists(schema, body),
        "match" => translate_match(index, schema, body, false),
        "match_phrase" => translate_match(index, schema, body, true),
        "query_string" => translate_query_string(index, schema, body),
        other => Err(SearchError::BadRequest(format!(
            "unsupported query type '{other}' (supported: match_all, bool, term, terms, \
             range, exists, match, match_phrase, query_string)"
        ))),
    }
}

fn translate_bool(
    index: &tantivy::Index,
    schema: &MappedSchema,
    body: &Value,
) -> SearchResult<Box<dyn Query>> {
    let obj = body
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("bool body must be an object".into()))?;
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    for (section, occur) in [
        ("must", Occur::Must),
        ("filter", Occur::Must),
        ("should", Occur::Should),
        ("must_not", Occur::MustNot),
    ] {
        if let Some(value) = obj.get(section) {
            let items: Vec<&Value> = match value {
                Value::Array(items) => items.iter().collect(),
                single => vec![single],
            };
            for item in items {
                clauses.push((occur, translate_query(index, schema, item)?));
            }
        }
    }
    if clauses.is_empty() {
        return Ok(Box::new(AllQuery));
    }
    // A bool with only must_not needs a positive base to subtract from.
    if clauses.iter().all(|(occur, _)| *occur == Occur::MustNot) {
        clauses.push((Occur::Must, Box::new(AllQuery)));
    }
    Ok(Box::new(BooleanQuery::new(clauses)))
}

/// Build a typed term for a mapped field from a JSON literal.
fn typed_term(field: Field, ty: FieldType, value: &Value) -> SearchResult<Term> {
    let term = match ty {
        FieldType::Keyword | FieldType::Text => {
            let s = value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            Term::from_field_text(field, &s)
        }
        FieldType::Long => {
            let i = value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                .ok_or_else(|| SearchError::BadRequest("expected integer value".into()))?;
            Term::from_field_i64(field, i)
        }
        FieldType::Double => {
            let f = value
                .as_f64()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                .ok_or_else(|| SearchError::BadRequest("expected numeric value".into()))?;
            Term::from_field_f64(field, f)
        }
        FieldType::Boolean => {
            let b = value
                .as_bool()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                .ok_or_else(|| SearchError::BadRequest("expected boolean value".into()))?;
            Term::from_field_bool(field, b)
        }
        FieldType::Date => {
            let ms = parse_time_millis(value)
                .ok_or_else(|| SearchError::BadRequest("expected date value".into()))?;
            Term::from_field_date(field, tantivy::DateTime::from_timestamp_millis(ms))
        }
        FieldType::Ip => {
            let s = value
                .as_str()
                .ok_or_else(|| SearchError::BadRequest("expected IP string".into()))?;
            let ip: IpAddr = s
                .parse()
                .map_err(|_| SearchError::BadRequest(format!("invalid IP '{s}'")))?;
            let ipv6 = match ip {
                IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                IpAddr::V6(v6) => v6,
            };
            Term::from_field_ip_addr(field, ipv6)
        }
    };
    Ok(term)
}

/// Term for an unmapped field: JSON path into `_dynamic`.
fn dynamic_term(field: Field, path: &str, value: &Value) -> Term {
    let mut term = Term::from_field_json_path(field, path, false);
    match value {
        Value::Number(n) if n.is_i64() => term.append_type_and_fast_value(n.as_i64().unwrap()),
        Value::Number(n) => term.append_type_and_fast_value(n.as_f64().unwrap_or(0.0)),
        Value::Bool(b) => term.append_type_and_fast_value(*b),
        other => {
            let s = other.as_str().map(str::to_string).unwrap_or_else(|| other.to_string());
            term.append_type_and_str(&s);
        }
    }
    term
}

fn term_query_for(schema: &MappedSchema, name: &str, value: &Value) -> SearchResult<Box<dyn Query>> {
    let term = match resolve(schema, name) {
        Resolved::Timestamp(field) => {
            let ms = parse_time_millis(value)
                .ok_or_else(|| SearchError::BadRequest("invalid timestamp value".into()))?;
            Term::from_field_date(field, tantivy::DateTime::from_timestamp_millis(ms))
        }
        Resolved::Typed(field, ty) => typed_term(field, ty, value)?,
        Resolved::Dynamic(field, path) => dynamic_term(field, &path, value),
    };
    Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
}

fn translate_term(schema: &MappedSchema, body: &Value) -> SearchResult<Box<dyn Query>> {
    let obj = body
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("term body must be an object".into()))?;
    let (name, spec) = obj
        .iter()
        .next()
        .ok_or_else(|| SearchError::BadRequest("term query needs a field".into()))?;
    let value = spec.get("value").unwrap_or(spec);
    term_query_for(schema, name, value)
}

fn translate_terms(schema: &MappedSchema, body: &Value) -> SearchResult<Box<dyn Query>> {
    let obj = body
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("terms body must be an object".into()))?;
    let (name, values) = obj
        .iter()
        .find(|(k, _)| *k != "boost")
        .ok_or_else(|| SearchError::BadRequest("terms query needs a field".into()))?;
    let values = values
        .as_array()
        .ok_or_else(|| SearchError::BadRequest("terms values must be an array".into()))?;
    let clauses: Vec<(Occur, Box<dyn Query>)> = values
        .iter()
        .map(|v| term_query_for(schema, name, v).map(|q| (Occur::Should, q)))
        .collect::<SearchResult<_>>()?;
    Ok(Box::new(BooleanQuery::new(clauses)))
}

fn translate_range(schema: &MappedSchema, body: &Value) -> SearchResult<Box<dyn Query>> {
    let obj = body
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("range body must be an object".into()))?;
    let (name, bounds) = obj
        .iter()
        .next()
        .ok_or_else(|| SearchError::BadRequest("range query needs a field".into()))?;
    let bounds = bounds
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("range bounds must be an object".into()))?;

    let make_term = |value: &Value| -> SearchResult<Term> {
        match resolve(schema, name) {
            Resolved::Timestamp(field) => {
                let ms = parse_time_millis(value)
                    .ok_or_else(|| SearchError::BadRequest("invalid timestamp bound".into()))?;
                Ok(Term::from_field_date(
                    field,
                    tantivy::DateTime::from_timestamp_millis(ms),
                ))
            }
            Resolved::Typed(field, ty) => typed_term(field, ty, value),
            Resolved::Dynamic(field, path) => Ok(dynamic_term(field, &path, value)),
        }
    };

    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    for (op, value) in bounds {
        match op.as_str() {
            "gte" => lower = Bound::Included(make_term(value)?),
            "gt" => lower = Bound::Excluded(make_term(value)?),
            "lte" => upper = Bound::Included(make_term(value)?),
            "lt" => upper = Bound::Excluded(make_term(value)?),
            "boost" | "format" | "time_zone" => {}
            other => {
                return Err(SearchError::BadRequest(format!(
                    "unsupported range operator '{other}'"
                )));
            }
        }
    }
    Ok(Box::new(RangeQuery::new(lower, upper)))
}

fn translate_exists(schema: &MappedSchema, body: &Value) -> SearchResult<Box<dyn Query>> {
    let name = body
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| SearchError::BadRequest("exists query needs 'field'".into()))?;
    let field_name = match resolve(schema, name) {
        Resolved::Timestamp(_) => "_timestamp".to_string(),
        Resolved::Typed(..) => name.to_string(),
        Resolved::Dynamic(_, path) => format!("_dynamic.{path}"),
    };
    Ok(Box::new(ExistsQuery::new(field_name, true)))
}

fn tokenize(analyzer: &mut TextAnalyzer, text: &str) -> Vec<String> {
    let mut stream = analyzer.token_stream(text);
    let mut tokens = Vec::new();
    while let Some(token) = stream.next() {
        tokens.push(token.text.clone());
    }
    tokens
}

fn translate_match(
    index: &tantivy::Index,
    schema: &MappedSchema,
    body: &Value,
    phrase: bool,
) -> SearchResult<Box<dyn Query>> {
    let obj = body
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("match body must be an object".into()))?;
    let (name, spec) = obj
        .iter()
        .next()
        .ok_or_else(|| SearchError::BadRequest("match query needs a field".into()))?;
    let text = spec
        .get("query")
        .unwrap_or(spec)
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| spec.get("query").unwrap_or(spec).to_string());
    let operator_and = spec
        .get("operator")
        .and_then(Value::as_str)
        .map(|op| op.eq_ignore_ascii_case("and"))
        .unwrap_or(false);

    match resolve(schema, name) {
        Resolved::Typed(field, FieldType::Text) => {
            // Text fields use the default tokenizer (mapping subset v1).
            let mut analyzer = index
                .tokenizers()
                .get("default")
                .ok_or_else(|| SearchError::Internal("missing tokenizer".into()))?;
            let tokens = tokenize(&mut analyzer, &text);
            if tokens.is_empty() {
                return Ok(Box::new(BooleanQuery::new(vec![])));
            }
            if phrase {
                if tokens.len() == 1 {
                    return Ok(Box::new(TermQuery::new(
                        Term::from_field_text(field, &tokens[0]),
                        IndexRecordOption::WithFreqsAndPositions,
                    )));
                }
                let terms: Vec<Term> = tokens
                    .iter()
                    .map(|t| Term::from_field_text(field, t))
                    .collect();
                Ok(Box::new(PhraseQuery::new(terms)))
            } else {
                let occur = if operator_and { Occur::Must } else { Occur::Should };
                let clauses: Vec<(Occur, Box<dyn Query>)> = tokens
                    .iter()
                    .map(|t| {
                        (
                            occur,
                            Box::new(TermQuery::new(
                                Term::from_field_text(field, t),
                                IndexRecordOption::Basic,
                            )) as Box<dyn Query>,
                        )
                    })
                    .collect();
                Ok(Box::new(BooleanQuery::new(clauses)))
            }
        }
        // Unmapped fields live in the _dynamic JSON field, which is
        // tokenized — so match must tokenize too.
        Resolved::Dynamic(field, path) => {
            let mut analyzer = index
                .tokenizers()
                .get("default")
                .ok_or_else(|| SearchError::Internal("missing tokenizer".into()))?;
            let tokens = tokenize(&mut analyzer, &text);
            if tokens.is_empty() {
                return Ok(Box::new(BooleanQuery::new(vec![])));
            }
            let make_term = |token: &str| {
                let mut term = Term::from_field_json_path(field, &path, false);
                term.append_type_and_str(token);
                term
            };
            if phrase && tokens.len() > 1 {
                let terms: Vec<Term> = tokens.iter().map(|t| make_term(t)).collect();
                Ok(Box::new(PhraseQuery::new(terms)))
            } else {
                let occur = if operator_and || (phrase && tokens.len() == 1) {
                    Occur::Must
                } else {
                    Occur::Should
                };
                let clauses: Vec<(Occur, Box<dyn Query>)> = tokens
                    .iter()
                    .map(|t| {
                        (
                            occur,
                            Box::new(TermQuery::new(make_term(t), IndexRecordOption::Basic))
                                as Box<dyn Query>,
                        )
                    })
                    .collect();
                Ok(Box::new(BooleanQuery::new(clauses)))
            }
        }
        // Non-text mapped fields: match degrades to an exact term.
        _ => term_query_for(schema, name, spec.get("query").unwrap_or(spec)),
    }
}

fn translate_query_string(
    index: &tantivy::Index,
    schema: &MappedSchema,
    body: &Value,
) -> SearchResult<Box<dyn Query>> {
    let query_text = body
        .get("query")
        .and_then(Value::as_str)
        .ok_or_else(|| SearchError::BadRequest("query_string needs 'query'".into()))?;

    // Default search fields: every mapped text field plus the dynamic
    // catch-all, so bare terms search everything tokenized.
    let mut default_fields: Vec<Field> = schema
        .fields
        .values()
        .filter(|(_, ty)| *ty == FieldType::Text)
        .map(|(field, _)| *field)
        .collect();
    default_fields.push(schema.dynamic);

    let mut parser = QueryParser::for_index(index, default_fields);
    parser.set_conjunction_by_default();
    // Lenient parse: log queries from UIs often contain minor syntax slips.
    let (query, _errors) = parser.parse_query_lenient(query_text);
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsearch_index::IndexMapping;

    fn schema() -> MappedSchema {
        MappedSchema::build(
            IndexMapping::from_json(&serde_json::json!({
                "properties": {
                    "service": {"type": "keyword"},
                    "message": {"type": "text"},
                    "status": {"type": "long"},
                }
            }))
            .unwrap(),
        )
    }

    fn index(schema: &MappedSchema) -> tantivy::Index {
        tantivy::Index::create_in_ram(schema.schema.clone())
    }

    #[test]
    fn translates_supported_queries() {
        let s = schema();
        let idx = index(&s);
        for query in [
            serde_json::json!({"match_all": {}}),
            serde_json::json!({"term": {"service": {"value": "api"}}}),
            serde_json::json!({"term": {"service": "api"}}),
            serde_json::json!({"terms": {"status": [200, 500]}}),
            serde_json::json!({"range": {"status": {"gte": 400}}}),
            serde_json::json!({"range": {"@timestamp": {"gte": "2026-07-24T00:00:00Z", "lte": "now"}}}),
            serde_json::json!({"exists": {"field": "service"}}),
            serde_json::json!({"match": {"message": "user login"}}),
            serde_json::json!({"match": {"message": {"query": "user login", "operator": "and"}}}),
            serde_json::json!({"match_phrase": {"message": "user login"}}),
            serde_json::json!({"query_string": {"query": "service:api AND status:500"}}),
            serde_json::json!({"bool": {
                "must": [{"term": {"service": "api"}}],
                "must_not": [{"term": {"status": 200}}],
                "filter": {"range": {"@timestamp": {"gte": 0}}},
            }}),
            serde_json::json!({"term": {"unmapped_field": "value"}}),
        ] {
            translate_query(&idx, &s, &query)
                .unwrap_or_else(|e| panic!("query {query} failed: {e}"));
        }
    }

    #[test]
    fn rejects_unsupported_query_types() {
        let s = schema();
        let idx = index(&s);
        let err = translate_query(
            &idx,
            &s,
            &serde_json::json!({"fuzzy": {"message": "opps"}}),
        )
        .unwrap_err();
        assert!(matches!(err, SearchError::BadRequest(_)));
        assert!(err.to_string().contains("unsupported query type"));
    }

    #[test]
    fn extracts_time_bounds_from_nested_query() {
        let query = serde_json::json!({"bool": {"filter": [
            {"term": {"service": "api"}},
            {"range": {"@timestamp": {"gte": 1_753_300_000_000_i64, "lte": 1_753_300_060_000_i64}}},
        ]}});
        let (start, end) = extract_time_bounds(&query);
        assert_eq!(start, Some(1_753_300_000_000));
        assert_eq!(end, Some(1_753_300_060_000));
    }

    #[test]
    fn time_bounds_ignore_must_not_and_should() {
        // A timestamp range under must_not asks for docs OUTSIDE it — must
        // not prune (H4).
        let must_not = serde_json::json!({"bool": {"must_not": [
            {"range": {"@timestamp": {"gte": 1_753_300_000_000_i64}}}
        ]}});
        assert_eq!(extract_time_bounds(&must_not), (None, None));

        // OR of two ranges: intersecting them would drop hits (H4).
        let should = serde_json::json!({"bool": {"should": [
            {"range": {"@timestamp": {"gte": 100, "lte": 200}}},
            {"range": {"@timestamp": {"gte": 900, "lte": 1000}}},
        ]}});
        assert_eq!(extract_time_bounds(&should), (None, None));

        // But a must/filter range still prunes.
        let must = serde_json::json!({"bool": {"must": [
            {"range": {"@timestamp": {"gte": 1_753_300_000_000_i64}}}
        ]}});
        assert_eq!(extract_time_bounds(&must), (Some(1_753_300_000_000), None));
    }

    #[test]
    fn float_epoch_seconds_not_double_scaled() {
        // Pre-1973 fractional epoch seconds: must become millis once, not
        // twice (L3).
        let bounds = super::parse_time_millis(&serde_json::json!(1000.5));
        assert_eq!(bounds, Some(1_000_500));
    }

    #[test]
    fn agg_named_format_is_not_dropped() {
        // A user aggregation literally named "format" must survive; only a
        // "format" param inside a param block (with "field") is stripped (L4).
        let s = schema();
        let aggs = serde_json::json!({
            "format": {"terms": {"field": "service"}},
            "by_time": {"date_histogram": {
                "field": "@timestamp", "fixed_interval": "1m",
                "format": "epoch_millis", "time_zone": "UTC"
            }},
        });
        let out = rewrite_agg_fields(&s, &aggs);
        assert!(out.get("format").is_some(), "user agg 'format' was dropped");
        assert_eq!(out["by_time"]["date_histogram"]["field"], "_timestamp");
        assert!(out["by_time"]["date_histogram"].get("format").is_none());
        assert!(out["by_time"]["date_histogram"].get("time_zone").is_none());
    }

    #[test]
    fn rewrites_agg_fields() {
        let s = schema();
        let aggs = serde_json::json!({
            "by_time": {
                "date_histogram": {"field": "@timestamp", "fixed_interval": "1m"},
                "aggs": {"by_level": {"terms": {"field": "level"}}}
            },
            "by_service": {"terms": {"field": "service"}}
        });
        let rewritten = rewrite_agg_fields(&s, &aggs);
        assert_eq!(
            rewritten["by_time"]["date_histogram"]["field"],
            "_timestamp"
        );
        assert_eq!(
            rewritten["by_time"]["aggs"]["by_level"]["terms"]["field"],
            "_dynamic.level"
        );
        assert_eq!(rewritten["by_service"]["terms"]["field"], "service");
    }

    #[test]
    fn date_histogram_keys_become_integers() {
        // Tantivy emits float keys; ES clients read them as i64 (#32).
        let aggs = serde_json::json!({
            "hist": {
                "date_histogram": {"field": "_timestamp", "fixed_interval": "1h"},
                "aggs": {"levels": {"terms": {"field": "level", "size": 20}}}
            }
        });
        let mut response = serde_json::json!({
            "hist": {"buckets": [
                {"key": 1_786_669_200_000.0_f64, "key_as_string": "2026-08-14T01:00:00Z",
                 "doc_count": 2_190_507,
                 "levels": {"buckets": [{"key": "info", "doc_count": 5}]}},
                {"key": 1_786_672_800_000.0_f64, "doc_count": 3},
            ]}
        });
        fix_date_histogram_keys(&aggs, &mut response);
        let buckets = response["hist"]["buckets"].as_array().unwrap();
        assert_eq!(buckets[0]["key"].as_i64(), Some(1_786_669_200_000));
        assert_eq!(buckets[1]["key"].as_i64(), Some(1_786_672_800_000));
        // Sub-agg terms keys untouched.
        assert_eq!(buckets[0]["levels"]["buckets"][0]["key"], "info");
        assert_eq!(
            serde_json::to_string(&buckets[0]["key"]).unwrap(),
            "1786669200000"
        );
    }

    #[test]
    fn plain_histogram_keys_stay_floats() {
        // ES returns doubles for numeric `histogram`; only date_histogram
        // keys are converted.
        let aggs = serde_json::json!({
            "sizes": {"histogram": {"field": "bytes", "interval": 100}}
        });
        let mut response = serde_json::json!({
            "sizes": {"buckets": [{"key": 200.0_f64, "doc_count": 1}]}
        });
        fix_date_histogram_keys(&aggs, &mut response);
        assert!(response["sizes"]["buckets"][0]["key"].is_f64());
    }

    #[test]
    fn date_histogram_under_single_bucket_agg_is_fixed() {
        // A `filter` agg nests sub-agg results directly on its object —
        // the walk must still reach the date_histogram inside it.
        let aggs = serde_json::json!({
            "errors": {
                "filter": {"term": {"level": "error"}},
                "aggs": {"hist": {
                    "date_histogram": {"field": "_timestamp", "fixed_interval": "1h"}
                }}
            }
        });
        let mut response = serde_json::json!({
            "errors": {
                "doc_count": 7,
                "hist": {"buckets": [{"key": 1_786_669_200_000.0_f64, "doc_count": 7}]}
            }
        });
        fix_date_histogram_keys(&aggs, &mut response);
        assert_eq!(
            response["errors"]["hist"]["buckets"][0]["key"].as_i64(),
            Some(1_786_669_200_000)
        );
    }
}
