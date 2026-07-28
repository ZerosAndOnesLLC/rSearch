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
- [x] 1.3 Compliance gate: `cargo deny` config banning `ring`, `md-5`, `md5`,
      `argon2`, `bcrypt`; CI script (`cargo check` + `cargo deny check` + tests);
      document the aws-lc-rs CMVP certificate reference in README
- [x] 1.4 Node identity & roles: `--roles ingest,search,control` (default: all);
      config struct plumbed through; `GET /_cluster/health` returns single-node
      green stub

## Phase 2 — Storage & index core

- [x] 2.1 `Storage` trait (put, get, get_range, delete, list, exists) +
      local-FS implementation + tests
- [x] 2.2 S3 backend via aws-sdk-s3: custom endpoint support (MinIO), path-style
      addressing option, `use_fips` config, retry policy; integration test
      against MinIO container
- [x] 2.3 Mapping subset: ES-style mapping JSON → Tantivy schema (keyword, text,
      long, double, boolean, date, ip); dynamic defaults for unmapped fields;
      `_timestamp` fast field mandatory
- [x] 2.4 Split builder: batch of docs → Tantivy segments → single immutable
      split file (bundle + footer with hotcache metadata) → upload via `Storage`
- [x] 2.5 Split reader: lazy open from footer, byte-range reads through a local
      disk cache with LRU eviction; search a split without downloading it fully

## Phase 3 — Metastore (Postgres/sqlx)

- [x] 3.1 sqlx setup + initial migrations (`sqlx migrate`): `streams`,
      `splits` (stream_id, time_range, state, size, doc_count, footer offsets),
      `nodes`, `retention_policies`; indexes for the split-listing query
      (stream + time range + state)
- [x] 3.2 Metastore API: stage/publish/mark-for-delete split state machine;
      list-splits-for-query; all queries explicit columns, no `SELECT *`
- [x] 3.3 Node registry: heartbeat upsert loop; stale-node detection;
      `/_cat/nodes`

## Phase 4 — Ingest path

- [x] 4.1 `POST /_bulk` (and `POST /{index}/_bulk`): NDJSON parsing, per-action
      responses matching ES shape (incl. per-item errors), index-name → stream
      routing
- [x] 4.2 WAL: append-before-ack on local disk, segment rotation, replay on
      startup, truncate after split publish
- [x] 4.3 Indexer pipeline: per-stream batching by size/time, timestamp
      extraction (`@timestamp`/`timestamp` fallbacks), doc ID via
      non-cryptographic hash, commit → split build → upload → publish → WAL
      truncate
- [x] 4.4 Backpressure: bounded queues, 429 with `Retry-After` when saturated;
      ingest metrics (docs/s, bytes/s, queue depth)

## Phase 5 — Search path

- [x] 5.1 Query DSL subset parser: `bool`, `match`, `match_phrase`, `term`,
      `terms`, `range`, `exists`, `query_string` (via Tantivy query parser),
      `match_all`; unsupported query types → clear 400
- [x] 5.2 Execution: metastore split pruning by time range → per-split search →
      top-k merge; `from`/`size`, `sort` (timestamp default), `_source`
      filtering
- [x] 5.3 Aggregations: pass through Tantivy's ES-compatible aggregation module
      (terms, histogram, date_histogram, stats, min/max/avg/sum, percentiles,
      cardinality); merge across splits
- [x] 5.4 Compat surface: response envelopes (`took`, `hits.total`, `_shards`),
      product/version headers so ES clients and Grafana's datasource accept us;
      `GET /`, `GET /_cat/indices`, `_cluster/health` fleshed out
- [x] 5.5 End-to-end test: Vector and Fluent Bit ship logs in unmodified;
      Grafana ES datasource queries and renders them

## Phase 6 — Benchmark gate (go/no-go)

- [x] 6.1 Replay harness: tool that replays a captured day of real logs at
      controlled rates against any `_bulk` endpoint
- [x] 6.2 Side-by-side vs OpenSearch container: ingest CPU/RSS at 5k and 10k
      eps, query p50/p99 for needle, range-scan, and date_histogram queries,
      disk usage per GB ingested
- [x] 6.3 Record results in BENCHMARKS.md; validate the ~5–10× memory /
      ~2–3× CPU estimates; decide any architecture corrections before
      continuing

## Phase 7 — Cluster ops

- [x] 7.1 Role separation verified: ingest-only, search-only, control-only
      nodes run correctly from the same binary
- [x] 7.2 Control loop with Postgres advisory-lock leader election; only the
      leader runs background jobs; leadership failover test
- [x] 7.3 Merge policy: combine small published splits per stream/time-bucket
      into larger ones; old splits marked-for-delete after merge publish
- [x] 7.4 Retention & GC: enforce per-stream retention by marking expired
      splits; grace-period deletion of marked splits from storage + metastore
- [x] 7.5 Multi-node docker-compose (2 ingest, 2 search, 1 control, Postgres,
      MinIO): kill-a-node tests — searcher death (LB reroutes), ingest death
      (WAL replay on restart, shipper retry covers the gap), leader death
      (lock failover)

## Phase 8 — Inputs & streams

- [x] 8.1 Syslog input: RFC 5424 + RFC 3164 over TLS (FIPS provider) and UDP;
      parsed fields mapped to stream schema
- [x] 8.2 GELF TCP input (null-delimited, TLS)
- [x] 8.3 Stream routing rules: match conditions on fields → route/copy to
      streams; per-stream mapping overrides
- [x] 8.4 Per-stream retention configuration surfaced through the API
      (enforced by 7.4)

## Phase 9 — AuthN/Z

- [x] 9.1 Users with PBKDF2-HMAC-SHA256 password storage; session tokens for
      the UI; standalone auth (no tv-api dependency — this ships to customers)
- [x] 9.2 API keys for shippers/automation, scoped to streams + actions
      (ingest/search/admin)
- [x] 9.3 RBAC: roles with per-stream read/write grants; enforcement in ingest
      and search paths
- [x] 9.4 Audit log: auth events + admin mutations to a dedicated internal
      stream

## Phase 10 — Alerting & UI

- [x] 10.1 Scheduled query alerts: cron-style schedule, query + threshold
      condition, webhook notification (email later); runs on control leader
- [x] 10.2 UI (separate NextJS project, S3+CloudFront pattern): search screen
      (query, time picker, histogram, results), stream management, retention,
      users/keys, alerts

## Phase 11 — Review pass

Standard review agents only (no multi-agent workflow orchestration). Findings
are presented grouped by severity and wait for per-finding direction — never
auto-applied.

- [x] 11.1 Security review (`/security-review`): full pass over the codebase —
      auth paths, ingest parsing (untrusted NDJSON/syslog/GELF input), TLS
      config, FIPS algorithm usage, SQL injection surface
- [x] 11.2 Code review (`/code-review`): correctness pass over the core crates
      (index, metastore, ingest, search)
- [x] 11.3 Performance/quality review agent: hot paths (bulk parsing, split
      search/merge, WAL), allocation patterns, query efficiency vs the
      benchmark numbers from phase 6
- [x] 11.4 Action items: all review findings fixed (blanket approval); see git log. Critical + all High/Medium + most Low addressed; two design notes accepted as known v1 behavior (at-least-once on crash-between-publish-and-confirm; stop-at-first-corruption WAL replay).
      normal sub-phase commits

## Phase 12 — Native split replication (`replicated` storage backend)

HA on plain block storage with no external object store: each node keeps a
local fs object root, splits are replicated to `replication_factor` nodes at
upload time, reads fall back to a live holder over HTTP, and the control
leader repairs under-replication. Postgres remains the source of truth
(placement lives in the metastore); splits are immutable so replication is
whole-file copy, never sync.

Design decisions (locked):
- Placement is tracked per storage key in a new `object_locations` table —
  a storage-layer concern keyed by key, not split_id, so the `Storage` trait
  stays object-agnostic.
- Write path: `put_file` writes the local root first, then pushes to
  `replication_factor - 1` live storage-role peers. Publish requires
  `write_quorum` successful copies (default: min(2, replication_factor));
  under quorum the error propagates and the ingest pipeline's existing
  WAL-backed retry handles it. Repair closes the gap to full RF later.
- Read path: local root hit → serve; miss → look up holders in the
  metastore, ranged GET from a live holder. `SplitCache` above the storage
  layer keeps hot ranges local, so peer reads are cold-path only.
- Peer API is internal-only: bearer token (`cluster.internal_token`,
  constant-time compare) over the existing TLS listener; mounted only when
  `backend = "replicated"`.
- Ingest WAL stays node-local and unreplicated (accepted RPO: acked docs on
  a dead node are stranded until its volume returns — same as today, and
  WAL forwarding stays in Deferred).
- Heartbeats must advertise a dialable address: new `node.advertise_addr`
  (falls back to bind_addr) replaces the current `0.0.0.0` heartbeat value.

- [x] 12.1 Metastore + config groundwork: sqlx migration for
      `object_locations (storage_key, node_id, size_bytes, created_at,
      PK(storage_key, node_id))` + index on node_id; metastore methods
      (record/remove location, holders_of, locations_on_node,
      under_replicated_keys); `node.advertise_addr` config wired into the
      heartbeat; `storage.replication_factor`, `storage.write_quorum`,
      `cluster.internal_token` config fields + example toml
- [x] 12.2 Internal object API on the axum server (mounted only for the
      replicated backend, token-gated): ranged
      `GET /_rsearch/internal/objects/{key}` served from the local root;
      streamed `PUT /_rsearch/internal/objects/{key}` (atomic tmp+rename,
      fsync — reuse FsStorage write discipline);
      `POST /_rsearch/internal/replicate` (pull `key` from `source_addr`)
- [x] 12.3 `ReplicatedStorage` backend in rsearch-storage: wraps local
      FsStorage + peer HTTP client + metastore handle; put/put_file with
      peer push + quorum + location records; get/get_range with peer
      fallback; delete fans out to all holders and clears location rows
      (tolerates dead holders); list from `object_locations`; factory
      wiring. Write-target selection is bytes-aware: prefer live,
      non-draining storage nodes holding the fewest total bytes (aggregate
      over `object_locations`), so new nodes absorb writes first
- [x] 12.4 Repair + lifecycle on the control leader: re-replication job
      (scan under-replicated keys, instruct a healthy non-holder to pull
      from a live holder), prioritizing keys with the fewest live holders;
      under-replication counts holders against a short staleness threshold
      (`control.repair_stale_secs`, default 300) — NOT the 3600s dead-node
      expiry, which only governs row cleanup; purge location rows for
      expired nodes inside the dead-node expiry path; GC delete path
      verified against fan-out
- [x] 12.5 Cluster test + docs: extend `tests/cluster/run-cluster-test.sh`
      with a replicated-backend topology (3 nodes, rf=2, kill a holder,
      verify search still answers and repair restores rf); README + example
      config section on HA-on-block-storage
- [x] 12.6 Graceful drain/decommission: `draining` flag on the nodes table
      + admin endpoint (`POST /_rsearch/nodes/{id}/drain`, DELETE cancels);
      draining nodes are excluded from write-target selection; drain job on
      the control leader copies everything in the node's `object_locations`
      to healthy nodes while it still serves reads; node deletes cleanly
      once empty. Implementation note: bulk ingest returns 503 on the
      draining node (flag propagates via heartbeat) so the WAL empties as
      batches age out; syslog/GELF listeners keep running — operators
      repoint shippers (documented in README)

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
