#!/usr/bin/env bash
# Phase D3: multi-bench BD2 Redis fleet sweep bc ∈ {2,4} on fixed N=4 standalone brokers.
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/bench-fleet.sh"

export BENCH_COUNT="${BENCH_COUNT:-4}"
export BOSON_FLEET_SIZE="${BOSON_FLEET_SIZE:-4}"
export BOSON_BENCH_INSTANCE_TYPE=c6i.large
export BOSON_BROKER_INSTANCE_TYPE="${BOSON_REDIS_INSTANCE_TYPE:-t3.medium}"
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_BROKER=redis
export BOSON_NATIVE_MANIFEST="boson-redis-multibench-4"
export BOSON_NATIVE_CAMPAIGN="boson-bd2-redis-multibench-$(date -u +%Y%m%d)"
export BOSON_BENCH_STORAGE_TOPOLOGY=redis-fleet-multibench
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-16}"
export BOSON_BENCH_PREFILL_COUNT="${BOSON_BENCH_PREFILL_COUNT:-10000}"
export BOSON_BD2_MULTIBENCH_BC="${BOSON_BD2_MULTIBENCH_BC:-2 4}"

REPORTS="${REPO_ROOT}/profiling/boson-bench/reports"
mkdir -p "$REPORTS"
LOG="$ROOT/state/run-bd2-redis-multibench-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST" 2>/dev/null || true
}
trap cleanup EXIT

echo "========== Boson BD2 Redis multi-bench fleet sweep (D3) =========="

"$BF/provision-multibench-fleet.sh"
"$BF/bootstrap-fleet.sh" "$BOSON_NATIVE_MANIFEST"
eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
"$BF/deploy-all-benches.sh" "$BOSON_NATIVE_MANIFEST"

MANIFEST="$(manifest_read "$BOSON_NATIVE_MANIFEST")"
HARDWARE="aws-c6i-large"
SLUG="redis-isolated-lab-off-${HARDWARE}"
FLEET_N="${BOSON_FLEET_SIZE}"
BD2_W="${BOSON_BD2_WORKER_COUNT}"
PREFILL="${BOSON_BENCH_PREFILL_COUNT}"
TAG_BASE="bm-bd2-fleet-n${FLEET_N}-k${FLEET_N}-w${BD2_W}-c${PREFILL}"

run_cell() {
  local count="$1"
  echo "=== Multi-bench BD2 Redis bc=${count} (N=${FLEET_N} brokers, W=${BD2_W}) ==="
  local start_epoch
  start_epoch=$(( $(date +%s) + 90 ))
  local pids=()

  for i in $(seq 1 "$count"); do
    local host client_idx report
    host="$(resolve_bench_ip "$MANIFEST" "$i")"
    client_idx=$((i - 1))
    report="${REPORTS}/${TAG_BASE}-bc${count}-i${client_idx}-${SLUG}.json"
    drain_only_env=""
    if [[ "${BOSON_BENCH_DRAIN_ONLY:-0}" == "1" ]]; then
      drain_only_env="export BOSON_BENCH_DRAIN_ONLY=1 &&"
      if [[ "$client_idx" -eq 0 ]]; then
        drain_only_env="${drain_only_env} export BOSON_BENCH_CENTRAL_PREFILL=1 &&"
      fi
    fi
    # shellcheck disable=SC1091
    source "$ROOT/lib/ssh.sh"
    ssh_cmd "$host" \
      "export START_EPOCH=${start_epoch} && \
       while [[ \$(date +%s) -lt \$START_EPOCH ]]; do sleep 1; done && \
       ${drain_only_env} \
       export BOSON_BENCH_CLIENT_INDEX=${client_idx} && \
       export BOSON_BENCH_CLIENT_COUNT=${count} && \
       export BOSON_FLEET_SIZE=${FLEET_N} && \
       export BOSON_REDIS_URLS='${BOSON_REDIS_URLS}' && \
       export BOSON_BD2_PIN_WORKER_POOLS='${BOSON_BD2_PIN_WORKER_POOLS:-}' && \
       export BOSON_BENCH_STORAGE_TOPOLOGY=redis-fleet-multibench && \
       export BOSON_SKIP_RUN_ROWS=1 && \
       export BOSON_BENCH_PREFILL_COUNT=${PREFILL} && \
       cd ~/boson-bench && mkdir -p reports && \
       ./boson-bench run --experiment bm-bd2 --backend redis --topology isolated-lab \
         --telemetry off --hardware ${HARDWARE} --warmup 0 --idempotency-mode none \
         --worker-count ${BD2_W} --worker-poll-ms 0 \
         --pool-count ${FLEET_N} --pool-layout distinct \
         --prefill-count ${PREFILL} \
         --storage-topology redis-fleet-multibench \
         --report reports/$(basename "${report}")" &
    pids+=($!)
  done

  local fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  if [[ "$fail" -ne 0 ]]; then
    echo "One or more bench clients failed for bc=${count}" >&2
  fi

  for i in $(seq 1 "$count"); do
    local host client_idx remote local
    host="$(resolve_bench_ip "$MANIFEST" "$i")"
    client_idx=$((i - 1))
    remote="~/boson-bench/reports/${TAG_BASE}-bc${count}-i${client_idx}-${SLUG}.json"
    local="${REPORTS}/${TAG_BASE}-bc${count}-i${client_idx}-${SLUG}.json"
    # shellcheck disable=SC1091
    source "$ROOT/lib/ssh.sh"
    scp_from "$host" "$remote" "$local"
  done

  cd "$REPO_ROOT"
  if [[ "$count" -gt 1 ]]; then
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
      bd2-aggregate --hardware "$HARDWARE" --backend redis \
      --reports-dir "$REPORTS" \
      --cell-prefix "$TAG_BASE" || true
  fi
}

for bc in ${BOSON_BD2_MULTIBENCH_BC}; do
  if [[ "$bc" -gt "$BENCH_COUNT" ]]; then
    echo "skip bc=${bc}: BENCH_COUNT=${BENCH_COUNT}" >&2
    continue
  fi
  run_cell "$bc"
done

cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  bd2-multibench-curve --hardware "$HARDWARE" --backend redis \
  --reports-dir "$REPORTS" || true

echo "Redis multi-bench BD2 sweep done. Log: $LOG"
