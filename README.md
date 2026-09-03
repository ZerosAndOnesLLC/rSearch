# rSearch

FIPS-compliant, cluster-ready log search server written in Rust. A lightweight
replacement for the Graylog/OpenSearch stack: single static binary, no JVM,
no garbage collector.

## Highlights

- **OpenSearch-compatible wire subset** — `_bulk`, `_search`, `_cat`, and
  cluster health endpoints; works unmodified with Vector, Fluent Bit,
  Filebeat, and Grafana's Elasticsearch datasource
- **FIPS from the ground up** — all TLS via rustls with the aws-lc-rs FIPS
  provider (CMVP-validated), approved algorithms only, enforced by a
  `cargo deny` gate in CI
- **Cluster-ready by design** — immutable index splits in object storage
  (S3, MinIO, or local disk), Postgres metastore/control plane, stateless
  searchers; a single node is just a cluster of one
- **HA on plain block storage** — the `replicated` backend keeps split
  copies on N nodes' local disks with quorum writes, peer reads, and
  automatic re-replication; no external object store required
- **Tantivy index engine** — Lucene-class full-text search and
  ES-compatible aggregations with predictable, GC-free latency
- **Document mode for application data** — per-index `mode: document`
  turns on index-replaces / delete-by-`_id` / `_doc` routes /
  `?refresh=wait_for`, with tombstones applied at query time and made
  physical by compaction; log indices stay append-only and cost nothing
  extra (see [Document mode](#document-mode-application-indices))

## FIPS compliance

All cryptography flows through **aws-lc-rs in FIPS mode** (the AWS-LC
Cryptographic Module, CMVP-validated under FIPS 140-3), pulled in via
rustls's `fips` feature. The exact CMVP certificate is determined by the
pinned `aws-lc-fips-sys` version in `Cargo.lock`; see the
[aws-lc-rs FIPS documentation](https://aws.github.io/aws-lc-rs/fips.html)
for the certificate covering each module release. The server refuses to
start TLS unless `ServerConfig::fips()` reports true.

Enforcement is structural, not procedural:

- `deny.toml` bans `ring`, `openssl`, `md5`/`md-5`, `argon2`, `bcrypt`,
  `scrypt`, and unvalidated RustCrypto implementations from the entire
  dependency graph
- `scripts/ci.sh` runs `cargo deny check bans licenses` on every build;
  a banned crate anywhere in the tree fails CI
- Building the FIPS module from source requires CMake and Go on the
  build host (validated-module build procedure)

Passwords (phase 9) use PBKDF2-HMAC-SHA256. Non-security hashing
(document IDs, cache keys) uses clearly non-cryptographic hashes so
intent stays auditable.

Documented `deny.toml` exceptions (both wire-protocol legacy, not
security functions): S3 Content-MD5 integrity headers
(aws-smithy-checksums), and the unused Postgres legacy md5 auth path in
sqlx — **deployments must configure Postgres with `scram-sha-256`
authentication** (the default since Postgres 14).

## Status

v1 core complete. See `working-plan.md` for the phase breakdown and
`BENCHMARKS.md` for the rSearch-vs-OpenSearch gate results (~9× less
memory, ~5× less CPU at 5k events/s, query-latency parity).

## Architecture (short version)

Ingest nodes accept logs (`_bulk`, syslog-TLS, GELF), write a local WAL,
build immutable Tantivy splits, and publish them to object storage. The
Postgres metastore tracks splits, streams, nodes, and retention. Stateless
search nodes prune splits by time range via the metastore and execute
queries directly against storage through a local cache. One binary runs any
combination of roles: `rsearch --roles ingest,search,control`.

## Install

From crates.io (builds the FIPS module from source — needs CMake, Go,
and clang on the build host):

```bash
CC=clang CXX=clang++ cargo install rsearch-server   # installs the `rsearch` binary
```

Or build from a checkout with `CC=clang CXX=clang++ cargo build
--release` (clang is required: the aws-lc FIPS delocator rejects newer
GCC assembly). Container images build from the included `Dockerfile`.

## Quick start

```bash
# 1. dependencies (Postgres 14+ with scram-sha-256, object storage or local disk)
docker compose up -d postgres minio    # or bring your own

# 2. build (see Install above for toolchain requirements)
CC=clang CXX=clang++ cargo build --release

# 3. run a single-node cluster (all roles)
export DATABASE_URL=postgres://rsearch:rsearch@localhost:5432/rsearch
./target/release/rsearch --roles ingest,search,control
```

Migrations run automatically at startup. On first boot, auth is in
**bootstrap mode** (a startup warning is logged); create the first admin
to arm enforcement cluster-wide:

```bash
curl -XPUT localhost:9200/_rsearch/users/admin \
  -d '{"password":"choose-a-strong-one"}'
```

A reference multi-node topology (2 ingest + 2 search + 1 control over
Postgres and MinIO) is in `docker-compose.yml`; the kill-a-node test
suite that exercises it is `tests/cluster/run-cluster-test.sh`, and
`tests/cluster/run-document-mode-test.sh` walks the document-mode API
end to end on a single node.

## HA on block storage (no object store)

The `replicated` storage backend turns each node's local disk (any block
device — EBS, iSCSI, plain SATA) into cluster storage with no external
object store: only rSearch and Postgres run. Every split is written to
`storage.replication_factor` nodes (quorum-acknowledged before the
ingest WAL lets go of the data), reads fall back to a live holder over
the internal peer API, and the control leader re-replicates the copies
of a node that goes silent for `control.repair_stale_secs` (default
5 minutes). New or empty nodes absorb new writes first, so capacity
rebalances as data churns through retention.

```toml
[node]
advertise_addr = "node1.internal:9200"   # peers must be able to dial this

[storage]
backend = "replicated"
root = "/var/lib/rsearch/objects"        # this node's local object root
replication_factor = 2

[cluster]
internal_token = "<openssl rand -hex 32, same on every node>"
```

Operational notes:

- Scale in gracefully with `POST /_rsearch/nodes/{id}/drain`: the node
  keeps serving reads while the leader copies its objects to the other
  nodes, and it refuses new `_bulk` traffic (503) so its WAL empties out
  (repoint syslog/GELF shippers yourself). When the leader logs "drain
  complete" (its `object_locations` rows are gone) the node can be shut
  down. `DELETE` on the same path cancels a drain. A draining node takes
  no writes or repair copies, so don't leave the flag set on a node you
  mean to keep: `GET /_cat/nodes` reports `draining_since_secs`, and the
  leader warns every tick once a drain outlives
  `control.drain_warn_secs` (default 1h). If under-replicated data has
  nowhere else to go, repair will fall back to a draining node rather
  than leave a key one failure from loss (the drain job moves the copy
  off again later).
- Postgres holds placement and all metadata — run it HA too, or it is
  the single point of failure.
- With factor 2, the window between a node dying and repair completing
  is one further failure away from data loss; size
  `repair_stale_secs` and node count accordingly.
- An hourly reconcile sweep (plus one pass at startup) keeps each node's
  disk and the placement table honest in both directions: local files
  the table still knows are re-announced, orphaned files whose splits
  were deleted while the node was unreachable are removed, and placement
  rows whose file is gone from local disk — a node restarted onto a
  replaced or wiped data volume — are dropped so repair sees the real
  copy count and restores the factor
  (`rsearch_reconcile_phantom_placements_removed_total` counts these).
  Alert on `rsearch_reconcile_last_copy_placements_removed_total`: it
  counts phantom rows that were an object's *last* recorded copy, i.e.
  data that is likely lost — once the row is gone the repair job has
  nothing left to warn about, so this counter (and a per-key error log)
  is the durable signal. One deliberate exception: a sole-copy row on a
  node whose object root is *empty* is kept, not deleted — an unmounted
  volume looks identical to a replaced one, and that row may be the last
  true record (dead-node expiry preserves it for the same reason); it is
  error-logged every sweep until the volume returns or an operator
  intervenes.
- The ingest WAL stays node-local: docs acked but not yet published when
  a node dies are recovered by WAL replay when that node (or its volume)
  returns — same recovery story as the fs backend.
- TLS between peers uses the FIPS provider; set `cluster.peer_ca_file`
  to a PEM bundle when node certificates are signed by an internal CA
  (otherwise the public webpki roots apply). Peer endpoints share the
  API listener — keep the port on a trusted network segment.
- The server refuses to start the replicated backend with a wildcard
  (`0.0.0.0`/`[::]`) advertise address — peers must be able to dial
  `node.advertise_addr`.

A containerized reference topology (3 nodes, per-node volumes standing
in for block devices) is in `docker-compose-replicated.yml`. Two test
suites exercise the backend: `tests/cluster/run-replicated-test.sh`
(process-level: kill-a-holder, repair, drain, fan-out GC) and
`tests/cluster/run-ha-compose-test.sh` (container-level: data-node
death, leader failover, volume-reattach rejoin).

## Configuration

Config loads from an optional TOML file (`--config`) with `RSEARCH_`
environment overrides (nested keys use `__`, e.g.
`RSEARCH_HTTP__TLS__ENABLED=true`). See `rsearch.example.toml` for the
full annotated set. Storage backends — S3, S3-compatible (MinIO), and
local filesystem — are equal citizens; self-hosted/air-gapped
deployments use static credentials in config so the AWS credential chain
and IMDS are never touched.

## API surface (OpenSearch-compatible subset)

| Area | Endpoints |
|------|-----------|
| Ingest | `POST /_bulk`, `POST /{index}/_bulk` (`index`, `create`; plus `update`, `delete` on document-mode indices; `?refresh=true\|wait_for`) |
| Documents | `PUT/POST/GET/HEAD/DELETE /{index}/_doc/{id}`, `POST /{index}/_doc`, `PUT/POST /{index}/_create/{id}`, `POST /{index}/_update/{id}`, `GET /{index}/_source/{id}`, `POST /{index}/_delete_by_query` (document-mode indices) |
| Search | `POST /{index}/_search`, `POST /_msearch`, `GET /{index}/_mapping` |
| Index admin | `PUT /{index}` (settings + mapping), `GET/HEAD /{index}`, `GET /{index}/_settings`, `GET /_cat/indices` |
| Cluster | `GET /`, `GET /_cluster/health`, `GET /_cat/nodes` |
| Streams | `PUT /_rsearch/streams/{name}/retention`, routing rules under `/_rsearch/routing_rules` |
| Alerts | `PUT/GET/DELETE /_rsearch/alerts[/{name}]` (scheduled query → webhook) |
| Auth | `POST /_rsearch/login`, users/api_keys under `/_rsearch/` |
| Observability | `GET /metrics` (Prometheus), `GET /_rsearch/stats` (JSON) |

Query DSL subset: `match_all`, `bool`, `term`, `terms`, `ids`, `range`,
`prefix`, `wildcard`, `exists`, `match`, `match_phrase`, `query_string`, `simple_query_string`
(the same lenient parser — a typo never 400s; `fields` with `^boost`,
`default_field`, and `default_operator` are honored — OR by default as in
Elasticsearch/OpenSearch, `"and"` to require every term; `flags` is
accepted and ignored; bare terms search the mapped text fields, unmapped
fields by `name:term` or via `fields`). Aggregations pass
through Tantivy's ES-compatible module (terms, date_histogram, stats,
percentiles, cardinality, …).

Deep pagination uses `search_after`: every hit's `sort` values are
`[timestamp_millis, _seq]` — the `_seq` element is an implicit unique
tiebreak appended to the timestamp sort, the way Elasticsearch's
point-in-time search appends `_shard_doc` — and passing the last hit's
values back as `search_after` (with `from` = 0) pages strictly past it.
Each page costs only `size` per split, so it pages past
`max_result_window`, which caps plain `from`/`size` at 10k. Totals and
aggregations reflect the full query on every page, as in Elasticsearch;
send `track_total_hits: false` on follow-up pages to skip recounting,
which also lets splits wholly behind the cursor be skipped entirely.
Always pass both sort values back: a one-element `[timestamp]` cursor
pages strictly by timestamp and skips equal-timestamp documents at the
page boundary (the classic ES footgun with a non-unique sort). Hits
from legacy (pre-`_seq`) splits report `-1` as the tiebreak and page by
timestamp only there; merging them to the current split format restores
exact paging.

Inputs beyond HTTP: syslog (RFC 5424 + 3164, UDP/TCP, optional TLS) and
GELF (TCP), each routable to a stream and subject to routing rules.

### Document mode (application indices)

rSearch is a log engine first: an index is append-only, every write is a
new document, and the only deletion is retention by time. Applications
that index *records people edit* need more, so an index can be created
in **document mode**:

```bash
curl -XPUT localhost:9200/items -H 'Content-Type: application/json' -d '{
  "settings": {"index": {"mode": "document"}},
  "mappings": {"properties": {"title": {"type": "keyword"}}}
}'
```

(`mode` defaults to `log`; it can only change while the index is empty.)
On a document-mode index:

- `_id` is honored and persisted; `index` on an existing `_id` **replaces**
  it (reads see exactly the newest version), `delete` hides every version,
  `create` fails with 409 if a live version exists, `update` merges a
  partial `doc` (`doc_as_upsert` / `upsert` supported, no scripts).
- The stock ES document routes work unmodified — `PUT/GET/DELETE
  /{index}/_doc/{id}`, `_create`, `_update`, `_source`,
  `_delete_by_query` — and are one-item `_bulk` requests underneath, so
  they share routing, peer handoff and WAL durability.
- **Visibility**: a write becomes searchable when its split is cut —
  within `ingest.document_max_batch_secs` (default 5s; log indices use
  `ingest.max_batch_secs`, default 30s). `?refresh=true` or
  `?refresh=wait_for` on `_bulk` or any document route cuts the split now
  and returns once it is published, so a save-then-search flow sees its
  own write. Deletes are visible immediately on the node that took them
  and within ~1s elsewhere. `update`/`create`/GET read published splits
  only, so a read-modify-write chain should set `refresh=wait_for` on
  each step.
- Under the hood: deleting or replacing writes a **tombstone** (one row per
  index + `_id` in the metastore: "hide versions older than this write").
  Searches apply tombstones inside the query so hits, counts and
  aggregations agree; the excluded-document set is cached per split and
  extended incrementally. Tombstones become **physical** when compaction
  rewrites the split without the hidden versions: merges always do, and
  a dedicated job rewrites document-mode splits once an index has
  `control.compact_min_tombstones` (default 1000) or its oldest tombstone
  is past `control.compact_max_age_secs` (default 1h). A deleted document
  is therefore physically gone after that age plus the sweep
  (`control.compact_splits_per_tick`, default 8 per control tick, so a
  stream with many splits takes several ticks) plus `control.gc_grace_secs`
  for the old split object — lower those if an erasure SLA requires.
  Tombstone rows are purged once every split of the stream has applied
  them and they are older than `control.tombstone_purge_grace_secs`
  (default 1h); that grace also covers documents still buffered on an
  ingest node, so **drain an ingest node's WAL before taking it down for
  longer than `compact_max_age_secs + tombstone_purge_grace_secs`** (a
  replayed document whose tombstone was purged would reappear).
- Ordering across nodes: each write gets a sequence from a hybrid logical
  clock — wall-clock micros pushed past every sequence the node has
  observed for the ids it writes (their existing tombstone bounds and the
  stream's highest published sequence) — so a replacement taken by a node
  whose clock lags still orders after the version it replaces. Keep node
  clocks NTP-synced anyway; skew only shows up as slightly non-monotonic
  `_version` values.
- Log indices are untouched: they reject `delete`/`update` per item with
  a reason that points at the setting, ignore `?refresh`, and carry no
  query-time filtering.

Hits from document-mode (and new log) indices return the real `_id` and
`_version` (the write sequence); splits written before this feature keep
their synthetic `split:segment:doc` ids.

### Authentication for applications

API keys are accepted as `Authorization: Bearer <key>` (preferred —
`Authorization` is what HTTP clients, tracing middleware and proxies
already redact) or `X-Api-Key: <key>`. A key carries an action set and a
stream list, so an application can hold a least-privilege key: with
`{"actions": ["ingest", "search"], "streams": ["items"]}` it can create
its index (`PUT /items`), write and read documents, and nothing else.
Stream entries are exact names or globs (`*` matches any run of
characters): `"streams": ["acme-*"]` scopes a multi-tenant application to
the indices it derives from its tenant names without `*` or an up-front
list; `*` alone still means every stream (required for `/_bulk` without
an index in the URL, `/_msearch`, and the Loki API).
`PUT /{index}`, the document writes and `_delete_by_query` classify as
stream-scoped `ingest`; document reads, `GET /{index}` and `_settings`
as stream-scoped `search`. Everything under `/_rsearch/` stays admin.

### Loki-compatible API (Grafana Logs Drilldown)

rSearch also speaks a subset of Loki's HTTP query API, so Grafana's
built-in **Loki datasource** — and with it **Logs Drilldown** — works
with no plugins: point a Loki datasource at the rSearch URL (Basic auth
or an API key as a bearer credential) and browse.

| Endpoint | Notes |
|----------|-------|
| `GET/POST /loki/api/v1/query_range` | log selectors → `streams`, metric queries → `matrix` |
| `GET/POST /loki/api/v1/query` | instant queries |
| `GET /loki/api/v1/labels`, `/label/{name}/values` | label discovery |
| `GET/POST /loki/api/v1/series` | series matching |
| `GET/POST /loki/api/v1/index/volume`, `/index/volume_range` | Drilldown's volume breakdowns |
| `GET /loki/api/v1/tail` | WebSocket live tail (poll-backed) |
| `GET /ready` | Loki readiness probe (open, like `/health`) |

Model mapping: the `service_name` label is the stream name (every stream
appears as a browsable service); other labels are the stream's
keyword-mapped fields, with values served from terms aggregations
(capped at 1000). A log line is the doc's `message` field, or the raw
`_source` JSON when there is none.

LogQL coverage is the subset Grafana sends: selectors with
`=`, `!=`, `=~`, `!~`; line filters `|=`, `!=`, `|~`, `!~`;
`count_over_time` and `rate` (correct sliding-window math for any
step/range combination), optionally wrapped in `sum` / `sum by (label)`
— one grouping label. Like `/_msearch`, the Loki surface requires
search-level auth with global stream access, and selectors must contain
at least one matcher that doesn't match the empty string.

Line filters are true substring/regex tests against the rendered line —
never a tokenized index match that could miss "error" when searching
"err". The trade-off: a filtered query (or filtered metric) examines up
to 5000 selector-matching docs per stream, newest first, and sets a
response warning when that scan window saturates. Per-stream failures in
multi-stream queries degrade to `warnings` with partial results instead
of failing the whole query. Tails are capped at 16 concurrent sessions
and 1 hour per session, with WebSocket pings reaping dead peers.

`GET /metrics` serves Prometheus text format: ingest throughput and
queue depth, WAL backlog (`rsearch_wal_outstanding_records` — watch this
for restart-replay memory pressure), cluster node liveness/draining
gauges, and on control nodes leadership plus repair/drain activity. It
requires search-level auth like `/_rsearch/stats`; point a scrape job at
it with an API key:

```yaml
scrape_configs:
  - job_name: rsearch
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials: <api key from POST /_rsearch/api_keys>
    static_configs:
      - targets: ["node-a:9200", "node-b:9200", "node-c:9200"]
```

## Workspace crates

| Crate | What it is |
|-------|------------|
| `rsearch-server` | The `rsearch` binary: HTTP API, roles, control plane |
| `rsearch-common` | Config, roles, FIPS TLS, crypto helpers |
| `rsearch-storage` | Storage backends: local fs, S3/MinIO, node-replicated |
| `rsearch-index` | ES-style mappings on Tantivy; immutable split files |
| `rsearch-metastore` | Postgres metastore: streams, splits, placement, leadership (migrations embedded) |
| `rsearch-ingest` | `_bulk`/syslog/GELF parsing, WAL, indexer pipeline |
| `rsearch-search` | Query-DSL subset executed over published splits |

Release procedure (versioning, publish order) is in `RELEASING.md`.

## Web console

`ui/` is a Next.js app (search, streams/retention, alerts, users & API
keys). `cd ui && npm install && npm run build`. The API base is resolved
at runtime: replace the served `/env.js` (bind-mount, S3 object
overwrite, or a container entrypoint writing it) with

```js
window.__RSEARCH_API__ = "https://rsearch.example.com:9200"; // "" = same origin
```

to point an already-built console at any cluster — no rebuild. When
`/env.js` sets nothing, the `NEXT_PUBLIC_RSEARCH_API` build-time value
applies (default `http://localhost:9200`).

## License

rSearch is free software, licensed under the [GNU General Public
License v3.0 or later](LICENSE).
