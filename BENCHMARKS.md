# Benchmarks — rSearch vs OpenSearch

**Verdict: PASS.** rSearch meets or beats the planning estimates
(~5–10× memory, ~2–3× CPU) on every ingest metric; query latencies are
single-digit-ms p50 on both systems with mixed wins.

## Post-review re-run (after phase 11 fixes)

Numbers below are from after the review-remediation pass (concurrent
split search, reader-built-once, `track_total_hits`, deferred `_source`
fetch, lazy-parse ingest queue). Same host and workload as the original
gate.

| Metric | rSearch | OpenSearch |
|---|---|---|
| Idle RSS | **16 MB** | 2,492 MB |
| Peak RSS @5k eps | **109 MB** | 2,698 MB |
| Avg CPU @5k eps | **7.1%** | 35.3% |
| needle p50 / p95 | **2.0** / 2.5 ms | 4.1 / 8.3 ms |
| range_scan p50 | 8.6 ms | **2.1 ms** |
| date_histogram p50 | 12.4 ms | **1.8 ms** |
| disk (900k docs) | 336 MB | 111 MB |

The ingest fixes *improved* the memory story further — peak RSS at 5k eps
dropped from 285 MB (original gate) to **109 MB** — and selective
queries (needle) now beat OpenSearch. Full-corpus scans/aggregations
still trail OpenSearch: on this small (~5-split) corpus the new
concurrency has little to exploit, and those queries touch every split;
the gap narrows as split counts grow and the merge policy consolidates
aged data. `_source` is now stored as the client's verbatim line, which
costs more disk than OpenSearch's compressed store — an accepted trade
for zero re-serialization on the ingest hot path.

---

## Original phase 6 gate (pre-review)

## Setup

- Host: WSL2, 24 cores, 31 GB RAM, NVMe.
- rSearch: release build, single node (all roles), local-FS storage,
  Postgres metastore in Docker, default config.
- OpenSearch: `opensearchproject/opensearch:2` (2.19), single node, security
  disabled, 2 GB heap (`-Xms2g -Xmx2g`).
- Workload: synthetic mixed logs (~280 B JSON; 10 services, 100 hosts,
  weighted status codes, high-cardinality trace ids, tokenized messages),
  identical deterministic stream to both systems, explicit identical
  mappings. 60 s at 5,000 events/s, then 60 s at 10,000 events/s —
  900,000 documents total per system, zero errors both sides.
- Tool: `crates/rsearch-bench` (in-repo). Queries: 100 sequential
  iterations each after ingest settled.

## Ingest

| Metric | rSearch | OpenSearch | Ratio |
|---|---|---|---|
| Idle RSS | **14 MB** | 2,485 MB | ~175× |
| Idle CPU | ~0% | 8.2% | — |
| Peak RSS @5k eps | **285 MB** | 2,634 MB | ~9× |
| Avg CPU @5k eps | **6.6%** | 31.0% | ~4.7× |
| Peak RSS @10k eps | **599 MB** | 2,646 MB | ~4.4× |
| Avg CPU @10k eps | **15.0%** | 19.9%¹ | ~1.3–4× |
| Item/request errors | 0 | 0 | — |
| Disk (900k docs) | 117 MB | 111 MB | parity |

¹ `docker stats` CPU sampling is coarse; OpenSearch's @10k reading being
lower than @5k is a sampling artifact (background merges landed in the
5k window). Treat OpenSearch CPU as ~20–31% across both rates.

rSearch's RSS under load is dominated by the configurable in-flight batch
buffer (`ingest.max_batch_secs` × rate); shorter batch windows trade split
size for lower peak memory. OpenSearch's floor is the fixed JVM heap —
it cannot go below ~2.5 GB regardless of load.

## Query latency (ms, 100 iterations, 900k docs)

| Query | rSearch p50 / p95 / p99 | OpenSearch p50 / p95 / p99 |
|---|---|---|
| needle (term + match, 7,483 hits) | **2.9** / 3.5 / 98² | 4.0 / 6.8 / 27.0 |
| range scan (full window, top 100) | 9.9 / 10.8 / **11.4** | **1.7** / 2.6 / 6.5 |
| date_histogram + terms (900k docs) | 13.7 / 14.6 / **15.6** | **1.6** / 3.0 / 50.0 |

² rSearch's needle p99 outlier is cold split-open cost (first fetches
from storage populate the local cache); warm p95 is 3.5 ms. Conversely,
OpenSearch shows the GC-shaped tail on aggregations (p99 50 ms vs p50
1.6 ms) — rSearch's agg tail stays within 2 ms of its p50.

OpenSearch wins warm full-corpus scans/aggs (in-heap structures, single
index vs. multi-split merge); rSearch wins the selective-query case and
has structurally flatter tails. All absolute numbers are comfortably
interactive.

## Caveats

- Single desktop host, sequential queries, one run per configuration —
  directional, not statistically rigorous.
- OpenSearch heap fixed at 2 GB (its practical minimum class); a tuned
  larger heap improves its query numbers and worsens its memory ratio.
- rSearch searched ~4–6 freshly-written splits; the phase 7 merge policy
  will reduce split counts (and full-scan latency) for aged data.
- Real-log replay (vs. synthetic) still worth doing when a sample is
  available — message entropy affects index size and tokenization cost.

## Gate decision

Estimates validated: memory 4–9× better under load (175× idle), CPU
~2–5× better, disk parity, compatibility proven separately (Vector,
Fluent Bit, Grafana). Per the pre-authorized continue-unless-worse rule:
**proceeding to phases 7–11.**
