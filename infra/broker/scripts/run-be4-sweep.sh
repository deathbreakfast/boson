#!/usr/bin/env bash
# BM-BE4 publisher-count sweep — find JetStream single-stream saturation before sharding.
#
# Mirrors Photon BM-PFH methodology: sweep concurrent publishers (C) at fixed duration,
# record peak achieved_ops_per_sec, emit scaling-curve-be4-publishers-*.json.
#
# Default layout: K=1 shared pool → one WorkQueue stream (comparable to Photon single-topic).
# Set BOSON_BE4_SWEEP_POOLS=distinct for multi-stream BE4 (K=10).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
# Local smoke only — retained decision-grade reports live under reports/ (AWS).
REPORTS="${BOSON_BENCH_REPORTS:-${REPO}/profiling/boson-bench/smoke}"
NATS_BENCH="${REPO}/profiling/nats-bench"
mkdir -p "$REPORTS" "$NATS_BENCH"

SMOKE="${BOSON_BE4_SWEEP_SMOKE:-1}"
HARDWARE="${BOSON_BENCH_HARDWARE:-local}"
BENCH_CMD="${BOSON_BENCH_CMD:-cargo run -p boson-bench --release --}"
POOLS_MODE="${BOSON_BE4_SWEEP_POOLS:-single}"

export BOSON_NATS_QUEUE_MODE=workqueue
export BOSON_NATS_ENQUEUE_MODE="${BOSON_NATS_ENQUEUE_MODE:-stream}"
export BOSON_NATS_SYNC_ACK="${BOSON_NATS_SYNC_ACK:-0}"
export BOSON_NATS_MAX_INFLIGHT="${BOSON_NATS_MAX_INFLIGHT:-256}"
export BOSON_TEST_NATS_URL="${BOSON_TEST_NATS_URL:-nats://127.0.0.1:4222}"

if [[ "$POOLS_MODE" == "distinct" ]]; then
  POOL_COUNT=10
  POOL_LAYOUT="distinct"
  POOL_TAG="k10-distinct"
else
  POOL_COUNT=1
  POOL_LAYOUT="shared"
  POOL_TAG="k1-shared"
fi

run_nats_bench_baseline() {
  local url="${BOSON_TEST_NATS_URL}"
  if ! command -v nats >/dev/null 2>&1; then
    echo "nats CLI not installed; skipping raw bench baseline"
    return 0
  fi
  echo "nats bench pub baseline (512B msgs) ..."
  local out="${NATS_BENCH}/be4-raw-pub.txt"
  nats bench pub "boson.be4.raw" --server "$url" --size 512 --msgs 50000 --pub 32 2>&1 | tee "$out" || true
  if grep -qE '[0-9,]+ msgs/sec' "$out"; then
    local peak
    peak="$(grep -oE '[0-9,]+ msgs/sec' "$out" | tail -1 | tr -d ', msgs/sec')"
    echo "{\"peak_ops_per_sec\":${peak}}" > "${NATS_BENCH}/be4-raw-pub.json"
    export BOSON_BENCH_NATS_BENCH_PEAK="$peak"
  fi
}

run_be4_cell() {
  local clients="$1"
  export BOSON_BENCH_NATS_BENCH_PEAK="${BOSON_BENCH_NATS_BENCH_PEAK:-}"
  local mode="${BOSON_NATS_ENQUEUE_MODE}"
  local ack="${BOSON_NATS_SYNC_ACK}"
  local inflight="${BOSON_NATS_MAX_INFLIGHT}"
  local tag="bm-be4-c${clients}-nats-${mode}-${POOL_TAG}-ack${ack}-i${inflight}-r${HARDWARE}"
  cd "$REPO"
  $BENCH_CMD run \
    --experiment bm-be4 --backend nats --topology isolated-lab --telemetry off \
    --hardware "$HARDWARE" --warmup 0 --idempotency-mode none \
    --client-count "$clients" --pool-count "$POOL_COUNT" --pool-layout "$POOL_LAYOUT" \
    --report "$REPORTS/${tag}.json" || true
}

run_nats_bench_baseline

if [[ "$SMOKE" == "1" ]]; then
  CLIENTS=(1 8 32 64)
else
  CLIENTS=(1 8 32 64 128 256 512)
fi

echo "BE4 publisher sweep: mode=$POOLS_MODE K=$POOL_COUNT layout=$POOL_LAYOUT enqueue=$BOSON_NATS_ENQUEUE_MODE"
for c in "${CLIENTS[@]}"; do
  run_be4_cell "$c"
done

cd "$REPO"
$BENCH_CMD be4-publisher-curve \
  --hardware "$HARDWARE" --backend nats \
  --reports-dir "$REPORTS" \
  --out "$REPORTS/scaling-curve-be4-publishers-${HARDWARE}-nats-${POOL_TAG}.json" || true

echo "BE4 publisher sweep complete. Reports in $REPORTS"
