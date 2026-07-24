# rSearch Working Plan

FIPS-compliant, cluster-ready log search server in Rust. Single static binary,
OpenSearch-compatible wire subset, Tantivy index engine, Postgres metastore,
object-storage-backed splits. Graylog/OpenSearch replacement for regulated
environments.

## Locked decisions

- **Cluster model**: immutable splits in object storage + Postgres metastore/control
  plane + stateless searchers. No raft/gossip/shard replication — S3 (or equivalent)
  is the durability layer, Postgres is the coordination layer. A single node is a
  cluster of one; scale-out is adding nodes behind a load balancer.
- **Deployment targets**: our AWS (SaaS/internal), customer self-hosted, and
  air-gapped/GovCloud — all first-class. Storage backends S3 / S3-compatible
  (MinIO) / local FS are equal citizens. No external calls, no hard AWS dependency.
  Postgres is a required external dependency (customer-provided in self-hosted;
  FIPS-friendly everywhere).
- **Ingest durability**: local WAL on ingest nodes + shipper retries (Vector /
  Fluent Bit / syslog senders retry on nack). Replicated WAL / Kafka source are
  deferred, behind the same interface.
- **API compat**: OpenSearch wire subset — `_bulk`, `_search`, `_cat`, health —
  compatible enough that Vector, Fluent Bit, Filebeat, and Grafana's ES datasource
  work unmodified.
- **FIPS**: rustls with aws-lc-rs FIPS provider for all TLS (HTTP API, syslog-TLS,
  DB connections where TLS is used). PBKDF2-HMAC-SHA256 for passwords. No `ring`,
  no `md-5`, no argon2/bcrypt — enforced by `cargo deny` in CI from phase 1.

## Workspace layout

Cargo workspace, one binary:

- `crates/rsearch-common` — config, errors, types, telemetry
- `crates/rsearch-storage` — storage trait; local FS, S3/S3-compatible backends
- `crates/rsearch-index` — Tantivy wrapper: mapping→schema, split build/read
- `crates/rsearch-metastore` — sqlx/Postgres: streams, splits, nodes, retention
- `crates/rsearch-ingest` — bulk parsing, WAL, indexer pipeline
- `crates/rsearch-search` — query DSL translation, split search, agg/response merge
- `crates/rsearch-server` — binary: axum routes, role wiring, control loop

Rules: no implementation code in `mod.rs`; async throughout; no `SELECT *`;
cargo check clean before every commit.

---

## Phase 1 — Foundation & FIPS baseline

- [x] 1.1 `git init`; workspace scaffold with the crates above; `rsearch-server`
      boots, loads config (file + env), logs startup; README stub
- [x] 1.2 TLS: axum behind rustls with the aws-lc-rs FIPS provider; serve
      `GET /` health over HTTPS; self-signed dev-cert generation documented
- [ ] 1.3 Compliance gate: `cargo deny` config banning `ring`, `md-5`, `md5`,
      `argon2`, `bcrypt`; CI script (`cargo check` + `cargo deny check` + tests);
      document the aws-lc-rs CMVP certificate reference in README
- [ ] 1.4 Node identity & roles: `--roles ingest,search,control` (default: all);
      config struct plumbed through; `GET /_cluster/health` returns single-node
      green stub

## Phase 2 — Storage & index core

- [ ] 2.1 `Storage` trait (put, get, get_range, delete, list, exists) +
      local-FS implementation + tests
- [ ] 2.2 S3 backend via aws-sdk-s3: custom endpoint support (MinIO), path-style
      addressing option, `use_fips` config, retry policy; integration test
      against MinIO container
- [ ] 2.3 Mapping subset: ES-style mapping JSON → Tantivy schema (keyword, text,
      long, double, boolean, date, ip); dynamic defaults for unmapped fields;
      `_timestamp` fast field mandatory
- [ ] 2.4 Split builder: batch of docs → Tantivy segments → single immutable
      split file (bundle + footer with hotcache metadata) → upload via `Storage`
- [ ] 2.5 Split reader: lazy open from footer, byte-range reads through a local
      disk cache with LRU eviction; search a split without downloading it fully

## Phase 3 — Metastore (Postgres/sqlx)

- [ ] 3.1 sqlx setup + initial migrations (`sqlx migrate`): `streams`,
      `splits` (stream_id, time_range, state, size, doc_count, footer offsets),
      `nodes`, `retention_policies`; indexes for the split-listing query
      (stream + time range + state)
- [ ] 3.2 Metastore API: stage/publish/mark-for-delete split state machine;
      list-splits-for-query; all queries explicit columns, no `SELECT *`
- [ ] 3.3 Node registry: heartbeat upsert loop; stale-node detection;
      `/_cat/nodes`

## Phase 4 — Ingest path

- [ ] 4.1 `POST /_bulk` (and `POST /{index}/_bulk`): NDJSON parsing, per-action
      responses matching ES shape (incl. per-item errors), index-name → stream
      routing
- [ ] 4.2 WAL: append-before-ack on local disk, segment rotation, replay on
      startup, truncate after split publish
- [ ] 4.3 Indexer pipeline: per-stream batching by size/time, timestamp
      extraction (`@timestamp`/`timestamp` fallbacks), doc ID via
      non-cryptographic hash, commit → split build → upload → publish → WAL
      truncate
- [ ] 4.4 Backpressure: bounded queues, 429 with `Retry-After` when saturated;
      ingest metrics (docs/s, bytes/s, queue depth)

## Phase 5 — Search path

- [ ] 5.1 Query DSL subset parser: `bool`, `match`, `match_phrase`, `term`,
      `terms`, `range`, `exists`, `query_string` (via Tantivy query parser),
      `match_all`; unsupported query types → clear 400
- [ ] 5.2 Execution: metastore split pruning by time range → per-split search →
      top-k merge; `from`/`size`, `sort` (timestamp default), `_source`
      filtering
- [ ] 5.3 Aggregations: pass through Tantivy's ES-compatible aggregation module
      (terms, histogram, date_histogram, stats, min/max/avg/sum, percentiles,
      cardinality); merge across splits
- [ ] 5.4 Compat surface: response envelopes (`took`, `hits.total`, `_shards`),
      product/version headers so ES clients and Grafana's datasource accept us;
      `GET /`, `GET /_cat/indices`, `_cluster/health` fleshed out
- [ ] 5.5 End-to-end test: Vector and Fluent Bit ship logs in unmodified;
      Grafana ES datasource queries and renders them

## Phase 6 — Benchmark gate (go/no-go)

- [ ] 6.1 Replay harness: tool that replays a captured day of real logs at
      controlled rates against any `_bulk` endpoint
- [ ] 6.2 Side-by-side vs OpenSearch container: ingest CPU/RSS at 5k and 10k
      eps, query p50/p99 for needle, range-scan, and date_histogram queries,
      disk usage per GB ingested
- [ ] 6.3 Record results in BENCHMARKS.md; validate the ~5–10× memory /
      ~2–3× CPU estimates; decide any architecture corrections before
      continuing

## Phase 7 — Cluster ops

- [ ] 7.1 Role separation verified: ingest-only, search-only, control-only
      nodes run correctly from the same binary
- [ ] 7.2 Control loop with Postgres advisory-lock leader election; only the
      leader runs background jobs; leadership failover test
- [ ] 7.3 Merge policy: combine small published splits per stream/time-bucket
      into larger ones; old splits marked-for-delete after merge publish
- [ ] 7.4 Retention & GC: enforce per-stream retention by marking expired
      splits; grace-period deletion of marked splits from storage + metastore
- [ ] 7.5 Multi-node docker-compose (2 ingest, 2 search, 1 control, Postgres,
      MinIO): kill-a-node tests — searcher death (LB reroutes), ingest death
      (WAL replay on restart, shipper retry covers the gap), leader death
      (lock failover)

## Phase 8 — Inputs & streams

- [ ] 8.1 Syslog input: RFC 5424 + RFC 3164 over TLS (FIPS provider) and UDP;
      parsed fields mapped to stream schema
- [ ] 8.2 GELF TCP input (null-delimited, TLS)
- [ ] 8.3 Stream routing rules: match conditions on fields → route/copy to
      streams; per-stream mapping overrides
- [ ] 8.4 Per-stream retention configuration surfaced through the API
      (enforced by 7.4)

## Phase 9 — AuthN/Z

- [ ] 9.1 Users with PBKDF2-HMAC-SHA256 password storage; session tokens for
      the UI; standalone auth (no tv-api dependency — this ships to customers)
- [ ] 9.2 API keys for shippers/automation, scoped to streams + actions
      (ingest/search/admin)
- [ ] 9.3 RBAC: roles with per-stream read/write grants; enforcement in ingest
      and search paths
- [ ] 9.4 Audit log: auth events + admin mutations to a dedicated internal
      stream

## Phase 10 — Alerting & UI

- [ ] 10.1 Scheduled query alerts: cron-style schedule, query + threshold
      condition, webhook notification (email later); runs on control leader
- [ ] 10.2 UI (separate NextJS project, S3+CloudFront pattern): search screen
      (query, time picker, histogram, results), stream management, retention,
      users/keys, alerts

## Phase 11 — Review pass

Standard review agents only (no multi-agent workflow orchestration). Findings
are presented grouped by severity and wait for per-finding direction — never
auto-applied.

- [ ] 11.1 Security review (`/security-review`): full pass over the codebase —
      auth paths, ingest parsing (untrusted NDJSON/syslog/GELF input), TLS
      config, FIPS algorithm usage, SQL injection surface
- [ ] 11.2 Code review (`/code-review`): correctness pass over the core crates
      (index, metastore, ingest, search)
- [ ] 11.3 Performance/quality review agent: hot paths (bulk parsing, split
      search/merge, WAL), allocation patterns, query efficiency vs the
      benchmark numbers from phase 6
- [ ] 11.4 Action items: user triages findings; approved fixes applied as
      normal sub-phase commits

## Deferred (explicitly out of v1)

- Distributed search fan-out across searchers (any searcher answers alone in v1)
- Replicated ingest WAL; Kafka/Kinesis sources
- Painless scripting, ingest pipelines/extractors beyond routing rules
- OpenSearch Dashboards compatibility (Grafana is the v1 dashboard story)
- kNN/vector search; SQL endpoint
- Embedded metastore option (SQLite) for tiny single-node installs
- License-server integration (ls/api) for commercial self-hosted builds

## Execution rules

One sub-phase at a time; mark [x] when done. `cargo check` clean, then commit
after each sub-phase. Test migrations with `sqlx migrate run` before committing.
Update README on significant changes. Commit progress if context gets long.
