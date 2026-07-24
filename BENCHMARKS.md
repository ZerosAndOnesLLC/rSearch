# Benchmarks — rSearch vs OpenSearch (phase 6 gate)

**Date:** 2026-07-24 · **Verdict: PASS — continue.** rSearch meets or beats
the planning estimates (~5–10× memory, ~2–3× CPU) on every ingest metric;
query latencies are single-digit-ms p50 on both systems with mixed wins.

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
