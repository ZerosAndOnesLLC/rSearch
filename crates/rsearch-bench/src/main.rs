//! Benchmark harness: replays synthetic-but-realistic logs against any
//! `_bulk`-compatible endpoint at a controlled rate, and measures query
//! latency percentiles. Plain HTTP only (benchmarks run on localhost).

mod generator;
mod http;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::{Parser, Subcommand};
use serde_json::json;

use crate::generator::LogGenerator;
use crate::http::HttpClient;

#[derive(Parser)]
#[command(name = "rsearch-bench")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Replay synthetic logs via _bulk at a target rate.
    Ingest {
        #[arg(long, default_value = "http://127.0.0.1:9200")]
        endpoint: String,
        #[arg(long, default_value = "bench-logs")]
        index: String,
        /// Target events per second.
        #[arg(long, default_value_t = 5000)]
        rate: u64,
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
        #[arg(long, default_value_t = 500)]
        batch: u64,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },
    /// Measure query latency percentiles.
    Query {
        #[arg(long, default_value = "http://127.0.0.1:9200")]
        endpoint: String,
        #[arg(long, default_value = "bench-logs")]
        index: String,
        #[arg(long, default_value_t = 100)]
        iterations: usize,
        /// Use .keyword suffix for keyword term queries (OpenSearch
        /// dynamic-mapping compatibility when no explicit mapping is set).
        #[arg(long, default_value_t = false)]
        keyword_suffix: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Ingest {
            endpoint,
            index,
            rate,
            duration_secs,
            batch,
            concurrency,
        } => ingest(endpoint, index, rate, duration_secs, batch, concurrency).await,
        Command::Query {
            endpoint,
            index,
            iterations,
            keyword_suffix,
        } => query(endpoint, index, iterations, keyword_suffix).await,
    }
}

async fn ingest(
    endpoint: String,
    index: String,
    rate: u64,
    duration_secs: u64,
    batch: u64,
    concurrency: usize,
) -> anyhow::Result<()> {
    let client = Arc::new(HttpClient::new());
    let url = format!("{endpoint}/_bulk");
    let sent = Arc::new(AtomicU64::new(0));
    let item_errors = Arc::new(AtomicU64::new(0));
    let request_errors = Arc::new(AtomicU64::new(0));
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(concurrency * 2);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let url = url.clone();
        let rx = rx.clone();
        let sent = sent.clone();
        let item_errors = item_errors.clone();
        let request_errors = request_errors.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let body = {
                    let mut rx = rx.lock().await;
                    match rx.recv().await {
                        Some(body) => body,
                        None => break,
                    }
                };
                let docs = body.matches("\"index\"").count() as u64;
                match client.post(&url, body, "application/x-ndjson").await {
                    Ok((status, response)) => {
                        if status != 200 {
                            request_errors.fetch_add(1, Ordering::Relaxed);
                        } else {
                            sent.fetch_add(docs, Ordering::Relaxed);
                            if response.contains("\"errors\":true") {
                                // Count failed items (any status >= 300).
                                let errs = response.matches("\"status\":4").count()
                                    + response.matches("\"status\":5").count();
                                item_errors.fetch_add(errs as u64, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        request_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    // Rate control: emit batches on an even interval.
    let mut generator = LogGenerator::new(&index);
    let total_batches = rate * duration_secs / batch;
    let interval = Duration::from_secs_f64(batch as f64 / rate as f64);
    let started = Instant::now();
    let mut next = Instant::now();
    for _ in 0..total_batches {
        let body = generator.bulk_body(batch as usize);
        if tx.send(body).await.is_err() {
            break;
        }
        next += interval;
        let now = Instant::now();
        if next > now {
            tokio::time::sleep(next - now).await;
        }
    }
    drop(tx);
    for worker in workers {
        let _ = worker.await;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let sent = sent.load(Ordering::Relaxed);
    println!(
        "{}",
        json!({
            "mode": "ingest",
            "target_rate": rate,
            "achieved_rate": (sent as f64 / elapsed).round(),
            "docs_sent": sent,
            "elapsed_secs": elapsed,
            "item_errors": item_errors.load(Ordering::Relaxed),
            "request_errors": request_errors.load(Ordering::Relaxed),
        })
    );
    Ok(())
}

async fn query(
    endpoint: String,
    index: String,
    iterations: usize,
    keyword_suffix: bool,
) -> anyhow::Result<()> {
    let client = HttpClient::new();
    let url = format!("{endpoint}/{index}/_search");
    let service_field = if keyword_suffix { "service.keyword" } else { "service" };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;

    let cases = vec![
        (
            "needle",
            json!({
                "query": {"bool": {"must": [
                    {"term": {service_field: "svc-7"}},
                    {"match": {"message": "timeout"}},
                ]}},
                "size": 10,
            }),
        ),
        (
            "range_scan",
            json!({
                "query": {"range": {"@timestamp": {"gte": now_ms - 600_000, "lte": now_ms}}},
                "size": 100,
            }),
        ),
        (
            "date_histogram",
            json!({
                "size": 0,
                "query": {"match_all": {}},
                "aggs": {
                    "over_time": {"date_histogram": {"field": "@timestamp", "fixed_interval": "60s"}},
                    "by_service": {"terms": {"field": service_field, "size": 20}},
                },
            }),
        ),
    ];

    let mut results = serde_json::Map::new();
    for (name, body) in cases {
        let body_str = body.to_string();
        let mut latencies_us: Vec<u64> = Vec::with_capacity(iterations);
        let mut hits_total = 0i64;
        for i in 0..iterations {
            let started = Instant::now();
            let (status, response) = client
                .post(&url, body_str.clone(), "application/json")
                .await
                .with_context(|| format!("query '{name}' failed"))?;
            anyhow::ensure!(status == 200, "query '{name}' returned {status}: {response}");
            latencies_us.push(started.elapsed().as_micros() as u64);
            if i == 0 {
                hits_total = serde_json::from_str::<serde_json::Value>(&response)
                    .ok()
                    .and_then(|v| v["hits"]["total"]["value"].as_i64())
                    .unwrap_or(-1);
            }
        }
        latencies_us.sort_unstable();
        let pct = |p: f64| {
            let idx = ((latencies_us.len() as f64 * p) as usize).min(latencies_us.len() - 1);
            latencies_us[idx]
        };
        results.insert(
            name.to_string(),
            json!({
                "hits_total": hits_total,
                "p50_ms": pct(0.50) as f64 / 1000.0,
                "p95_ms": pct(0.95) as f64 / 1000.0,
                "p99_ms": pct(0.99) as f64 / 1000.0,
            }),
        );
    }
    println!(
        "{}",
        json!({"mode": "query", "iterations": iterations, "results": results})
    );
    Ok(())
}
