//! `_bulk` NDJSON parsing: alternating action and document lines, per the
//! ES/OpenSearch wire format. Log ingestion supports `index` and `create`
//! actions; `delete`/`update` are rejected per item (immutable log store).

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BulkAction {
    Index,
    Create,
}

impl BulkAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            BulkAction::Index => "index",
            BulkAction::Create => "create",
        }
    }
}

/// One accepted document.
#[derive(Debug)]
pub struct BulkItem {
    pub action: BulkAction,
    pub stream: String,
    pub doc_id: String,
    pub doc: Value,
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
        let doc_id = meta
            .get("_id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());

        match action_name {
            "index" | "create" => {
                let action = if action_name == "index" {
                    BulkAction::Index
                } else {
                    BulkAction::Create
                };
                let Some(doc_line) = lines.next() else {
                    outcome.rejections.push((
                        position,
                        action_name.to_string(),
                        index,
                        "missing document line".to_string(),
                    ));
                    outcome.total = position + 1;
                    break;
                };
                match serde_json::from_str::<Value>(doc_line) {
                    Ok(doc) if doc.is_object() => {
                        if index.is_empty() {
                            outcome.rejections.push((
                                position,
                                action_name.to_string(),
                                index,
                                "no index specified in action or URL".to_string(),
                            ));
                        } else {
                            outcome.items.push((
                                position,
                                BulkItem {
                                    action,
                                    stream: index,
                                    doc_id,
                                    doc,
                                },
                            ));
                        }
                    }
                    Ok(_) => {
                        outcome.rejections.push((
                            position,
                            action_name.to_string(),
                            index,
                            "document must be a JSON object".to_string(),
                        ));
                    }
                    Err(e) => {
                        outcome.rejections.push((
                            position,
                            action_name.to_string(),
                            index,
                            format!("document is not valid JSON: {e}"),
                        ));
                    }
                }
            }
            "delete" => {
                outcome.rejections.push((
                    position,
                    "delete".to_string(),
                    index,
                    "delete is not supported on an immutable log store".to_string(),
                ));
            }
            "update" => {
                // Update actions carry a following doc line — consume it.
                let _ = lines.next();
                outcome.rejections.push((
                    position,
                    "update".to_string(),
                    index,
                    "update is not supported on an immutable log store".to_string(),
                ));
            }
            other => {
                return Err(format!(
                    "unknown bulk action '{other}' at action {}",
                    position + 1
                ));
            }
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
    fn rejects_delete_update_and_bad_docs_per_item() {
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
        assert_eq!(out.items.len(), 1);
        assert_eq!(out.rejections.len(), 3);
        assert_eq!(out.rejections[0].1, "delete");
        assert_eq!(out.rejections[2].1, "update");
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
