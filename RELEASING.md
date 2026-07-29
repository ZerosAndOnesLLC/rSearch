# Releasing rSearch

## Versioning

One version for the whole workspace, bumped **only when cutting a
release** (major = breaking, minor = features, patch = fixes). Two places
must agree in the root `Cargo.toml`:

- `[workspace.package] version`
- the `version = "…"` on each `rsearch-*` entry in
  `[workspace.dependencies]` (crates.io requires internal path deps to
  carry a registry version)

## Pre-flight

```bash
cargo check --workspace          # clean, no warnings
cargo test --workspace
./scripts/ci.sh                  # fmt + cargo deny (FIPS bans) gate
sqlx migrate run --source crates/rsearch-metastore/migrations   # against dev PG
./tests/cluster/run-cluster-test.sh          # S3/MinIO topology
./tests/cluster/run-replicated-test.sh       # replicated backend, process-level
./tests/cluster/run-ha-compose-test.sh       # replicated backend, containers
```

Release builds of the FIPS module require CMake, Go, and **clang**
(`CC=clang CXX=clang++ cargo build --release`) — newer GCC is rejected by
the aws-lc delocator.

## Publish (dependency order)

`cargo publish` verifies each crate against crates.io, so dependents can
only be published after their dependencies are live. `rsearch-bench` is
`publish = false` and stays internal.

```bash
cargo publish -p rsearch-common
cargo publish -p rsearch-storage
cargo publish -p rsearch-index
cargo publish -p rsearch-metastore
cargo publish -p rsearch-ingest
cargo publish -p rsearch-search
cargo publish -p rsearch-server      # installs the `rsearch` binary
```

Postgres migrations are embedded in `rsearch-metastore` (its
`migrations/` directory ships in the package and runs at startup), so a
`cargo install rsearch-server` binary needs no migration tooling.

## After publishing

```bash
git tag -a v<version> -m "rSearch v<version>"
git push origin v<version>
```

Then create the GitHub release from the tag with the changelog highlights.
