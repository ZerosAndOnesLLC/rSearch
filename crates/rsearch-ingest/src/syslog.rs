//! Minimal syslog parsing: RFC 5424 and RFC 3164 (BSD) formats.
//! 5424 timestamps pass through as `@timestamp` strings (the document
//! converter parses RFC 3339); 3164 messages fall back to ingest time
//! (the format has no year or timezone).

use serde_json::{Value, json};

/// Parse one syslog line into a log document. Unparseable lines become
/// raw-message documents rather than being dropped.
pub fn parse_syslog(line: &str) -> Value {
    let line = line.trim();
    match try_parse(line) {
        Some(doc) => doc,
        None => json!({"message": line, "source": "syslog", "parse_error": true}),
    }
}

fn try_parse(line: &str) -> Option<Value> {
    let rest = line.strip_prefix('<')?;
    let (pri, rest) = rest.split_once('>')?;
    let pri: u32 = pri.parse().ok()?;
    let facility = pri / 8;
    let severity = pri % 8;

    if let Some(rest) = rest.strip_prefix("1 ") {
        // RFC 5424: TIMESTAMP HOSTNAME APP-NAME PROCID MSGID SD MSG
        let mut parts = rest.splitn(6, ' ');
        let timestamp = parts.next()?;
        let host = parts.next()?;
        let app = parts.next()?;
        let procid = parts.next()?;
        let msgid = parts.next()?;
        let tail = parts.next().unwrap_or("");
        let message = strip_structured_data(tail);
        let mut doc = json!({
            "host": nil_to_null(host),
            "app": nil_to_null(app),
            "procid": nil_to_null(procid),
            "msgid": nil_to_null(msgid),
            "facility": facility,
            "severity": severity,
            "message": message,
            "source": "syslog",
        });
        if timestamp != "-" {
            doc["@timestamp"] = json!(timestamp);
        }
        Some(doc)
    } else {
        // RFC 3164: "Mmm dd hh:mm:ss host tag: msg"
        let rest = rest.trim_start();
        if rest.len() < 16 {
            return None;
        }
        let (_ts, after_ts) = rest.split_at(15);
        let after_ts = after_ts.trim_start();
        let (host, msg) = after_ts.split_once(' ').unwrap_or((after_ts, ""));
        let (app, message) = match msg.split_once(':') {
            Some((tag, m)) => (tag.trim_end_matches(|c: char| c == '[' || c.is_ascii_digit() || c == ']'), m.trim_start()),
            None => ("", msg),
        };
        Some(json!({
            "host": host,
            "app": app,
            "facility": facility,
            "severity": severity,
            "message": message,
            "source": "syslog",
        }))
    }
}

fn nil_to_null(s: &str) -> Value {
    if s == "-" { Value::Null } else { json!(s) }
}

/// Skip RFC 5424 structured data ("-" or bracketed elements), returning
/// the free-form message that follows.
fn strip_structured_data(tail: &str) -> &str {
    let tail = tail.trim_start();
    if let Some(rest) = tail.strip_prefix('-') {
        return rest.trim_start();
    }
    let mut rest = tail;
    while rest.starts_with('[') {
        // Find the matching close bracket, honoring escaped `\]`.
        let bytes = rest.as_bytes();
        let mut i = 1;
        while i < bytes.len() {
            match bytes[i] {
                b'\\' => i += 2,
                b']' => break,
                _ => i += 1,
            }
        }
        if i >= bytes.len() {
            return "";
        }
        rest = rest[i + 1..].trim_start();
    }
    rest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rfc5424() {
        let doc = parse_syslog(
            "<165>1 2026-07-24T05:00:00.123Z web01 nginx 1234 ID47 \
             [exampleSDID@32473 iut=\"3\"] upstream timed out",
        );
        assert_eq!(doc["host"], "web01");
        assert_eq!(doc["app"], "nginx");
        assert_eq!(doc["facility"], 20);
        assert_eq!(doc["severity"], 5);
        assert_eq!(doc["@timestamp"], "2026-07-24T05:00:00.123Z");
        assert_eq!(doc["message"], "upstream timed out");
    }

    #[test]
    fn parses_rfc5424_nil_fields() {
        let doc = parse_syslog("<34>1 - - su - - - auth failure");
        assert!(doc["host"].is_null());
        assert!(doc.get("@timestamp").is_none());
        assert_eq!(doc["message"], "auth failure");
    }

    #[test]
    fn parses_rfc3164() {
        let doc = parse_syslog("<34>Jul 24 05:00:00 web01 sshd[123]: Failed password for root");
        assert_eq!(doc["host"], "web01");
        assert_eq!(doc["app"], "sshd");
        assert_eq!(doc["severity"], 2);
        assert_eq!(doc["message"], "Failed password for root");
    }

    #[test]
    fn garbage_becomes_raw_message() {
        let doc = parse_syslog("not really syslog");
        assert_eq!(doc["message"], "not really syslog");
        assert_eq!(doc["parse_error"], true);
    }
}
