//! Synthetic log generator: realistic field mix — low-cardinality
//! keywords, weighted statuses, high-cardinality trace ids, and varied
//! natural-language messages.

use std::time::{SystemTime, UNIX_EPOCH};

const MESSAGES: [&str; 12] = [
    "request completed successfully",
    "connection timeout to upstream service",
    "user login succeeded",
    "user login failed invalid credentials",
    "cache miss falling back to database",
    "slow query detected exceeding threshold",
    "payment processed for order",
    "retrying request after transient failure",
    "healthcheck passed",
    "certificate rotation completed",
    "disk usage above warning level",
    "background job finished processing batch",
];

const PATHS: [&str; 8] = [
    "/api/v1/users", "/api/v1/orders", "/api/v1/search", "/healthz",
    "/api/v1/payments", "/api/v2/inventory", "/login", "/api/v1/reports",
];

/// Deterministic xorshift RNG — no crypto claim, benchmark-repeatable.
pub struct LogGenerator {
    index: String,
    state: u64,
    seq: u64,
}

impl LogGenerator {
    pub fn new(index: &str) -> Self {
        Self {
            index: index.to_string(),
            state: 0x9E3779B97F4A7C15,
            seq: 0,
        }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn doc(&mut self) -> String {
        self.seq += 1;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let r = self.next();
        let service = r % 10;
        let host = (r >> 8) % 100;
        let status: u32 = match (r >> 16) % 100 {
            0..=79 => 200,
            80..=89 => 301,
            90..=95 => 404,
            _ => 500,
        };
        let level = match status {
            200 | 301 => "info",
            404 => "warn",
            _ => "error",
        };
        let latency = ((r >> 24) % 2000) as f64 / 10.0;
        let message = MESSAGES[(r >> 32) as usize % MESSAGES.len()];
        let path = PATHS[(r >> 40) as usize % PATHS.len()];
        let trace = self.next();
        format!(
            "{{\"@timestamp\":{now_ms},\"service\":\"svc-{service}\",\"host\":\"host-{host}\",\
             \"level\":\"{level}\",\"status\":{status},\"latency_ms\":{latency},\
             \"path\":\"{path}\",\"trace_id\":\"{trace:016x}\",\"seq\":{seq},\
             \"message\":\"{message}\",\"region\":\"us-east-1\"}}",
            seq = self.seq,
        )
    }

    /// An NDJSON `_bulk` body of `n` index actions.
    pub fn bulk_body(&mut self, n: usize) -> String {
        let action = format!("{{\"index\":{{\"_index\":\"{}\"}}}}\n", self.index);
        let mut body = String::with_capacity(n * 300);
        for _ in 0..n {
            body.push_str(&action);
            body.push_str(&self.doc());
            body.push('\n');
        }
        body
    }
}
