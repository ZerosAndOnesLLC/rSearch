//! `_bulk` NDJSON parsing: alternating action and document lines, per the
//! ES/OpenSearch wire format. All four actions parse; whether `delete`
//! and `update` are *accepted* depends on the target stream's mode (log
//! streams reject them per item, document streams honor them) — that is
//! the handler's call, so the parser stays mode-agnostic.

use serde_json::Value;

/// Bulk action kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BulkAction {
    /// An `{"index": …}` action line: write the document (document-mode
    /// streams replace an existing `_id`).
    Index,
    /// A `{"create": …}` action line: write only if the `_id` is absent
    /// (document mode); indistinguishable from `index` on log streams.
    Create,
    /// An `{"update": …}` action line followed by `{"doc": …}`: partial
    /// update of an existing document (document mode only).
    Update,
    /// A `{"delete": …}` action line (no document line): hide every
    /// version of the `_id` (document mode only).
    Delete,
}

impl BulkAction {
    /// The action name as it appears on the wire (for responses).
    pub fn as_str(&self) -> &'static str {
        match self {
            BulkAction::Index => "index",
            BulkAction::Create => "create",
            BulkAction::Update => "update",
            BulkAction::Delete => "delete",
        }
    }

    /// Whether the action needs a following document line.
    fn has_doc_line(&self) -> bool {
        !matches!(self, BulkAction::Delete)
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "index" => Some(BulkAction::Index),
            "create" => Some(BulkAction::Create),
            "update" => Some(BulkAction::Update),
            "delete" => Some(BulkAction::Delete),
            _ => None,
        }
    }
}

/// One parsed action.
#[derive(Debug)]
pub struct BulkItem {
    /// The action kind.
    pub action: BulkAction,
    /// Target stream: the action line's `_index`, or the URL default.
    pub stream: String,
    /// Document id: the action line's `_id`, or a generated UUID for
    /// `index`/`create` without one (`update`/`delete` require it).
    pub doc_id: String,
    /// Whether the client supplied the `_id` (vs. generated here).
    pub explicit_id: bool,
    /// The parsed body: the document for `index`/`create`, the update
    /// body (`{"doc": …, "doc_as_upsert": …}`) for `update`, `Null` for
    /// `delete`.
    pub doc: Value,
    /// The client's original body line (empty for `delete`), stored
    /// verbatim as `_source` and written to the WAL for `index`/`create` —
    /// avoids re-serializing the parsed value.
    pub raw: std::sync::Arc<str>,
}

/// Parse result: accepted items plus per-line rejections, positioned so
/// the response can interleave them in request order.
#[derive(Debug, Default)]
pub struct BulkParseOutcome {
    /// (request position, item)
    pub items: Vec<(usize, BulkItem)>,
    /// (request position, action name, index name, error reason)
    pub rejections: Vec<(usize, String, String, String)>,
    /// Total actions seen (items + rejections).
    pub total: usize,
}

/// Parse a bulk body. `default_index` comes from `/{index}/_bulk` URLs.
/// Returns an error only for bodies too malformed to produce per-item
/// responses (e.g. an action line that isn't JSON).
pub fn parse_bulk_body(
    body: &str,
    default_index: Option<&str>,
) -> Result<BulkParseOutcome, String> {
    let mut outcome = BulkParseOutcome::default();
    let mut lines = body
        .split('\n')
        .map(str::trim_end)
        .filter(|l| !l.is_empty());

    let mut position = 0usize;
    while let Some(action_line) = lines.next() {
        let action_json: Value = serde_json::from_str(action_line)
            .map_err(|e| format!("action line {} is not valid JSON: {e}", position + 1))?;
        let obj = action_json
            .as_object()
            .ok_or_else(|| format!("action line {} must be an object", position + 1))?;
        let (action_name, meta) = obj
            .iter()
            .next()
            .map(|(k, v)| (k.as_str(), v))
            .ok_or_else(|| format!("action line {} is empty", position + 1))?;

        let index = meta
            .get("_index")
            .and_then(|v| v.as_str())
            .or(default_index)
            .unwrap_or("")
            .to_string();
        // ES coerces numeric ids to strings ({"_id": 42} == {"_id": "42"}).
        let explicit = match meta.get("_id") {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        };
        let explicit_id = explicit.is_some();
        let doc_id = explicit.unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());

        let Some(action) = BulkAction::parse(action_name) else {
            return Err(format!(
                "unknown bulk action '{action_name}' at action {}",
                position + 1
            ));
        };
        let reject = |outcome: &mut BulkParseOutcome, reason: String| {
            outcome
                .rejections
                .push((position, action_name.to_string(), index.clone(), reason));
        };
        if !action.has_doc_line() {
            if index.is_empty() {
                reject(&mut outcome, "no index specified in action or URL".to_string());
            } else if !explicit_id {
                reject(&mut outcome, format!("{action_name} requires an _id"));
            } else {
                outcome.items.push((
                    position,
                    BulkItem {
                        action,
                        stream: index,
                        doc_id,
                        explicit_id,
                        doc: Value::Null,
                        raw: std::sync::Arc::from(""),
                    },
                ));
            }
            position += 1;
            continue;
        }
        let Some(doc_line) = lines.next() else {
            reject(&mut outcome, "missing document line".to_string());
            position += 1; // count this partial action...
            break; // ...then the post-loop `total = position` is correct
        };
        match serde_json::from_str::<Value>(doc_line) {
            Ok(doc) if doc.is_object() => {
                if index.is_empty() {
                    reject(&mut outcome, "no index specified in action or URL".to_string());
                } else if action == BulkAction::Update && !explicit_id {
                    reject(&mut outcome, "update requires an _id".to_string());
                } else {
                    outcome.items.push((
                        position,
                        BulkItem {
                            action,
                            stream: index,
                            doc_id,
                            explicit_id,
                            doc,
                            raw: std::sync::Arc::from(doc_line),
                        },
                    ));
                }
            }
            Ok(_) => reject(&mut outcome, "document must be a JSON object".to_string()),
            Err(e) => reject(&mut outcome, format!("document is not valid JSON: {e}")),
        }
        position += 1;
    }
    outcome.total = position;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_index_and_create_actions() {
        let body = concat!(
            "{\"index\":{\"_index\":\"logs\"}}\n",
            "{\"message\":\"one\"}\n",
            "{\"create\":{\"_index\":\"other\",\"_id\":\"abc\"}}\n",
            "{\"message\":\"two\"}\n",
        );
        let out = parse_bulk_body(body, None).unwrap();
        assert_eq!(out.total, 2);
        assert_eq!(out.items.len(), 2);
        assert!(out.rejections.is_empty());
        assert_eq!(out.items[0].1.stream, "logs");
        assert_eq!(out.items[1].1.doc_id, "abc");
        assert_eq!(out.items[1].1.action, BulkAction::Create);
    }

    #[test]
    fn url_index_is_default_but_action_wins() {
        let body = concat!(
            "{\"index\":{}}\n",
            "{\"m\":1}\n",
            "{\"index\":{\"_index\":\"explicit\"}}\n",
            "{\"m\":2}\n",
        );
        let out = parse_bulk_body(body, Some("from-url")).unwrap();
        assert_eq!(out.items[0].1.stream, "from-url");
        assert_eq!(out.items[1].1.stream, "explicit");
    }

    #[test]
    fn parses_delete_and_update_and_rejects_bad_docs_per_item() {
        let body = concat!(
            "{\"delete\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n",
            "{\"index\":{\"_index\":\"logs\"}}\n",
            "not-json\n",
            "{\"update\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n",
            "{\"doc\":{\"x\":1}}\n",
            "{\"index\":{\"_index\":\"logs\"}}\n",
            "{\"ok\":true}\n",
        );
        let out = parse_bulk_body(body, None).unwrap();
        assert_eq!(out.total, 4);
        assert_eq!(out.items.len(), 3);
        assert_eq!(out.rejections.len(), 1);
        assert_eq!(out.rejections[0].0, 1);
        let (pos, delete) = &out.items[0];
        assert_eq!((*pos, delete.action), (0, BulkAction::Delete));
        assert!(delete.explicit_id && delete.doc.is_null() && delete.raw.is_empty());
        let (pos, update) = &out.items[1];
        assert_eq!((*pos, update.action), (2, BulkAction::Update));
        assert_eq!(update.doc["doc"]["x"], 1);
        assert_eq!(out.items[2].1.action, BulkAction::Index);
        assert!(!out.items[2].1.explicit_id);
    }

    #[test]
    fn numeric_ids_are_coerced_to_strings() {
        let body = concat!(
            "{\"index\":{\"_index\":\"recs\",\"_id\":42}}\n",
            "{\"v\":1}\n",
            "{\"delete\":{\"_index\":\"recs\",\"_id\":7}}\n",
        );
        let out = parse_bulk_body(body, None).unwrap();
        assert!(out.rejections.is_empty());
        assert_eq!(out.items[0].1.doc_id, "42");
        assert!(out.items[0].1.explicit_id);
        assert_eq!(out.items[1].1.doc_id, "7");
    }

    #[test]
    fn delete_and_update_require_an_id() {
        let body = concat!(
            "{\"delete\":{\"_index\":\"logs\"}}\n",
            "{\"update\":{\"_index\":\"logs\"}}\n",
            "{\"doc\":{}}\n",
            "{\"index\":{\"_index\":\"logs\"}}\n",
            "{\"ok\":true}\n",
        );
        let out = parse_bulk_body(body, None).unwrap();
        assert_eq!(out.total, 3);
        assert_eq!(out.items.len(), 1);
        assert_eq!(out.rejections.len(), 2);
        assert!(out.rejections[0].3.contains("requires an _id"));
        assert!(out.rejections[1].3.contains("requires an _id"));
    }

    #[test]
    fn missing_index_is_rejected() {
        let body = "{\"index\":{}}\n{\"m\":1}\n";
        let out = parse_bulk_body(body, None).unwrap();
        assert!(out.items.is_empty());
        assert_eq!(out.rejections.len(), 1);
    }

    #[test]
    fn garbage_action_line_is_a_request_error() {
        assert!(parse_bulk_body("garbage\n{\"m\":1}\n", None).is_err());
    }
}
