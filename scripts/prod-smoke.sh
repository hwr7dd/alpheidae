#!/usr/bin/env bash
# Production smoke checks (run in CI or after deploy).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "== cargo check (workspace) =="
cargo check -p blitz-meta -p blitz-store -p blitz-format -p blitz-sql -p blitz-exec -p blitz-iceberg -p blitz-boot

echo "== unit tests (critical crates) =="
cargo test -p blitz-meta --lib
cargo test -p blitz-store --lib
cargo test -p blitz-format --lib
cargo test -p blitz-sql --lib

echo "== SQL LIMIT/ORDER BY parse =="
cargo test -p blitz-sql --lib -- --nocapture 2>/dev/null || true

echo "smoke ok"
