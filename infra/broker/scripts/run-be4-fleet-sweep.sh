#!/usr/bin/env bash
# Local 2-broker NATS fleet smoke: pool_0 → :4222, pool_1 → :4223.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
# Local smoke only — retained decision-grade reports live under reports/ (AWS).
REPORTS="${BOSON_BENCH_REPORTS:-${REPO}/profiling/boson-bench/smoke}"
mkdir -p "$REPORTS"

start_nats() {
  local name="$1"
  local port="$2"
  local ip="${3:-127.0.0.1}"
  docker rm -f "$name" 2>/dev/null || true
  docker run -d --name "$name" --network host nats:2.10-alpine \
    -js -a "$ip" -p "$port" -m "$((port + 1000))"
}

start_nats boson-nats-fleet-0 4222 127.0.0.1
start_nats boson-nats-fleet-1 4223 127.0.0.1
sleep 2

export BOSON_NATS_QUEUE_MODE=workqueue
export BOSON_NATS_ENQUEUE_MODE=stream
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_NATS_URLS="nats://127.0.0.1:4222,nats://127.0.0.1:4223"
export BOSON_TEST_NATS_URL="nats://127.0.0.1:4222"
HARDWARE="${BOSON_BENCH_HARDWARE:-local}"
export BOSON_BENCH_HARDWARE="$HARDWARE"

BENCH_CMD="${BOSON_BENCH_CMD:-cargo run -p boson-bench --release --}"
cd "$REPO"

echo "BE4 fleet K=2 C=64 ..."
$BENCH_CMD run --experiment bm-be4 --backend nats --topology isolated-lab \
  --telemetry off --hardware "$HARDWARE" --warmup 0 --idempotency-mode none \
  --client-count 64 --pool-count 2 --pool-layout distinct \
  --report "$REPORTS/bm-be4-fleet-n2-k2-c64-local.json" || true

echo "done (see $REPORTS/bm-be4-fleet-n2-k2-c64-local.json)"
