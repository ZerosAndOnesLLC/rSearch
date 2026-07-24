#!/usr/bin/env bash
# CI gate: warnings are errors, tests must pass, FIPS dependency bans enforced.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> cargo check (deny warnings)"
RUSTFLAGS="-D warnings" cargo check --workspace --all-targets

echo "==> cargo test"
cargo test --workspace

echo "==> cargo deny (FIPS compliance bans)"
cargo deny check bans licenses

echo "CI OK"
