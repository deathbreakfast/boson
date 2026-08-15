#!/usr/bin/env bash
# BM-BC1 marketing-durable: completed Success jobs/s on Redis with leases and run rows.
#
# Release-only. Unsets BOSON_SKIP_RUN_ROWS so run persistence stays on.
# Lease TTL comes from the experiment default (30s), not BM-BD2's lease_ttl_secs=0.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORTS="${BOSON_BENCH_REPORTS:-${REPO}/profiling/boson-bench/reports}"
mkdir -p "$REPORTS"

HARDWARE="${BOSON_BENCH_HARDWARE:-aws-c6i-large}"
BENCH_CMD="${BOSON_BENCH_CMD:-cargo run -p boson-bench --release --}"

unset BOSON_SKIP_RUN_ROWS
export BOSON_TEST_REDIS_URL="${BOSON_TEST_REDIS_URL:-redis://127.0.0.1:6379}"

cd "$REPO"
$BENCH_CMD run \
  --experiment bm-bc1 --backend redis --topology isolated-lab --telemetry off \
  --hardware "$HARDWARE" \
  --report "$REPORTS/bm-bc1-redis-isolated-lab-off-${HARDWARE}.json"
