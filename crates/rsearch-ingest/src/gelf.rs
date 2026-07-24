//! GELF message parsing (Graylog Extended Log Format), TCP null-framed.

use serde_json::{Value, json};

/// Convert a GELF JSON payload into a log document: `short_message`
/// becomes `message`, additional `_fields` lose their underscore, and
/// the epoch-seconds `timestamp` passes through for the converter.
pub fn parse_gelf(payload: &[u8]) -> Option<Value> {
    let gelf: Value = serde_json::from_slice(payload).ok()?;
    let obj = gelf.as_object()?;
    let mut doc = serde_json::Map::new();
    for (key, value) in obj {
        match key.as_str() {
            "version" => {}
            "short_message" => {
                doc.insert("message".to_string(), value.clone());
            }
            "full_message" => {
                doc.insert("full_message".to_string(), value.clone());
            }
            "timestamp" => {
                doc.insert("@timestamp".to_string(), value.clone());
            }
            "level" => {
                doc.insert("severity".to_string(), value.clone());
            }
            other => {
                let name = other.strip_prefix('_').unwrap_or(other);
                doc.insert(name.to_string(), value.clone());
            }
        }
    }
    doc.insert("source".to_string(), json!("gelf"));
    Some(Value::Object(doc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gelf_message() {
        let payload = serde_json::to_vec(&json!({
            "version": "1.1",
            "host": "app01",
            "short_message": "job failed",
            "timestamp": 1753333200.123,
            "level": 3,
            "_service": "billing",
            "_attempt": 4,
        }))
        .unwrap();
        let doc = parse_gelf(&payload).unwrap();
        assert_eq!(doc["message"], "job failed");
        assert_eq!(doc["severity"], 3);
        assert_eq!(doc["service"], "billing");
        assert_eq!(doc["attempt"], 4);
        assert_eq!(doc["host"], "app01");
        assert_eq!(doc["@timestamp"], 1753333200.123);
        assert!(doc.get("version").is_none());
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse_gelf(b"nope").is_none());
    }
}
