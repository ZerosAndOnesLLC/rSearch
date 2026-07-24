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

## Status

Early development. See `working-plan.md` for the build plan.

## Architecture (short version)

Ingest nodes accept logs (`_bulk`, syslog-TLS, GELF), write a local WAL,
build immutable Tantivy splits, and publish them to object storage. The
Postgres metastore tracks splits, streams, nodes, and retention. Stateless
search nodes prune splits by time range via the metastore and execute
queries directly against storage through a local cache. One binary runs any
combination of roles: `rsearch --roles ingest,search,control`.
