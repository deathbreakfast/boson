#!/usr/bin/env bash
# BM-BD2 worker-count sweep — find drain saturation vs W on local/dev NATS.
#
# Default: K=1 shared pool, W ∈ {1,2,4,8,16,32,64} (smoke: subset).
# Emits scaling-curve-bd2-workers-*.json via bd2-worker-curve.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
# Local smoke only — retained decision-grade reports live under reports/ (AWS).
REPORTS="${BOSON_BENCH_REPORTS:-${REPO}/profiling/boson-bench/smoke}"
mkdir -p "$REPORTS"

SMOKE="${BOSON_BD2_SWEEP_SMOKE:-1}"
HARDWARE="${BOSON_BENCH_HARDWARE:-local}"
BENCH_CMD="${BOSON_BENCH_CMD:-cargo run -p boson-bench --release --}"
PHASE="${BOSON_BD2_SWEEP_PHASE:-worker}"

export BOSON_NATS_QUEUE_MODE=workqueue
export BOSON_NATS_ENQUEUE_MODE="${BOSON_NATS_ENQUEUE_MODE:-stream}"
export BOSON_NATS_SYNC_ACK="${BOSON_NATS_SYNC_ACK:-0}"
export BOSON_NATS_MAX_INFLIGHT="${BOSON_NATS_MAX_INFLIGHT:-256}"
export BOSON_TEST_NATS_URL="${BOSON_TEST_NATS_URL:-nats://127.0.0.1:4222}"
export BOSON_SKIP_RUN_ROWS=1
export BOSON_BENCH_PREFILL_COUNT="${BOSON_BENCH_PREFILL_COUNT:-10000}"

run_bd2() {
  local tag="$1"
  shift
  cd "$REPO"
  $BENCH_CMD run \
    --experiment bm-bd2 --backend nats --topology isolated-lab --telemetry off \
    --hardware "$HARDWARE" --warmup 0 --idempotency-mode none \
    --worker-poll-ms 0 \
    --report "$REPORTS/${tag}.json" \
    "$@" || true
}

if [[ "$PHASE" == "worker" ]]; then
  if [[ "$SMOKE" == "1" ]]; then
    WORKERS=(1 4 10)
  else
    WORKERS=(1 2 4 8 16 32 64)
  fi
  echo "BD2 worker sweep: W grid on K=1"
  for w in "${WORKERS[@]}"; do
    run_bd2 "bm-bd2-w${w}-nats-isolated-lab-off-${HARDWARE}" \
      --worker-count "$w" --pool-count 1 --pool-layout shared
  done
  cd "$REPO"
  $BENCH_CMD bd2-worker-curve \
    --hardware "$HARDWARE" --backend nats \
    --reports-dir "$REPORTS" \
    --out "$REPORTS/scaling-curve-bd2-workers-${HARDWARE}-nats.json" || true
fi

if [[ "$PHASE" == "shard" ]]; then
  W="${BOSON_BD2_WORKER_COUNT:-32}"
  if [[ "$SMOKE" == "1" ]]; then
    SHARDS=(1 4)
  else
    SHARDS=(1 4 10 32)
  fi
  echo "BD2 shard sweep: K grid @ W=${W}"
  for k in "${SHARDS[@]}"; do
    if [[ "$k" == "1" ]]; then
      LAYOUT=shared
    else
      LAYOUT=distinct
    fi
    run_bd2 "bm-bd2-k${k}-w${W}-nats-isolated-lab-off-${HARDWARE}" \
      --worker-count "$W" --pool-count "$k" --pool-layout "$LAYOUT"
  done
  cd "$REPO"
  $BENCH_CMD bd2-shard-curve \
    --hardware "$HARDWARE" --backend nats \
    --reports-dir "$REPORTS" \
    --out "$REPORTS/scaling-curve-bd2-shards-${HARDWARE}-nats.json" || true
fi

echo "BD2 sweep complete (phase=$PHASE). Reports in $REPORTS"
