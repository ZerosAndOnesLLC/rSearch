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

## Quick start

```bash
# 1. dependencies (Postgres 14+ with scram-sha-256, object storage or local disk)
docker compose up -d postgres minio    # or bring your own

# 2. build (FIPS module needs CMake + Go on the build host)
cargo build --release

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
suite that exercises it is `tests/cluster/run-cluster-test.sh`.

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

- Postgres holds placement and all metadata — run it HA too, or it is
  the single point of failure.
- With factor 2, the window between a node dying and repair completing
  is one further failure away from data loss; size
  `repair_stale_secs` and node count accordingly.
- A node returning after its registry entry expired may hold orphaned
  object files (repair already replaced its copies); these are inert
  and can be cleaned by wiping the object root before restart.
- The ingest WAL stays node-local: docs acked but not yet published when
  a node dies are recovered by WAL replay when that node (or its volume)
  returns — same recovery story as the fs backend.
- TLS between peers uses the FIPS provider with webpki roots, so
  internal certificates must chain to a trusted root.

The 3-node kill-a-holder/repair test suite is
`tests/cluster/run-replicated-test.sh`.

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
| Ingest | `POST /_bulk`, `POST /{index}/_bulk` |
| Search | `POST /{index}/_search`, `POST /_msearch`, `GET /{index}/_mapping` |
| Index admin | `PUT /{index}` (mapping), `GET /_cat/indices` |
| Cluster | `GET /`, `GET /_cluster/health`, `GET /_cat/nodes` |
| Streams | `PUT /_rsearch/streams/{name}/retention`, routing rules under `/_rsearch/routing_rules` |
| Alerts | `PUT/GET/DELETE /_rsearch/alerts[/{name}]` (scheduled query → webhook) |
| Auth | `POST /_rsearch/login`, users/api_keys under `/_rsearch/` |

Query DSL subset: `match_all`, `bool`, `term`, `terms`, `range`,
`exists`, `match`, `match_phrase`, `query_string`. Aggregations pass
through Tantivy's ES-compatible module (terms, date_histogram, stats,
percentiles, cardinality, …).

Inputs beyond HTTP: syslog (RFC 5424 + 3164, UDP/TCP, optional TLS) and
GELF (TCP), each routable to a stream and subject to routing rules.

## Web console

`ui/` is a Next.js app (search, streams/retention, alerts, users & API
keys). `cd ui && npm install && npm run build`. Point it at the API with
`NEXT_PUBLIC_RSEARCH_API`.

## License

rSearch is free software, licensed under the [GNU General Public
License v3.0 or later](LICENSE).
