#!/usr/bin/env bash
# Phase D2: multi-bench BE4 fleet sweep bc ∈ {1,2,4} on fixed N=4 standalone brokers.
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
export BOSON_BROKER_INSTANCE_TYPE="${BOSON_BROKER_INSTANCE_TYPE:-t3.medium}"
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_NATIVE_MANIFEST="boson-nats-multibench-4"
export BOSON_NATIVE_CAMPAIGN="boson-multibench-$(date -u +%Y%m%d)"
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-fleet-multibench

REPORTS="${REPO_ROOT}/profiling/boson-bench/reports"
mkdir -p "$REPORTS"
LOG="$ROOT/state/run-be4-multibench-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Boson BE4 multi-bench fleet sweep =========="

"$BF/provision-multibench-fleet.sh"
"$BF/bootstrap-fleet.sh" "$BOSON_NATIVE_MANIFEST"
eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
"$BF/deploy-all-benches.sh" "$BOSON_NATIVE_MANIFEST"

MANIFEST="$(manifest_read "$BOSON_NATIVE_MANIFEST")"
HARDWARE="aws-c6i-large"
SLUG="nats-isolated-lab-off-${HARDWARE}"
SHARD_C="${BOSON_BE4_SHARD_CLIENT_COUNT:-256}"
FLEET_N="${BOSON_FLEET_SIZE}"
TAG_BASE="bm-be4-fleet-n${FLEET_N}-k${FLEET_N}"

run_cell() {
  local count="$1"
  echo "=== Multi-bench BE4 bc=${count} (N=${FLEET_N} brokers) ==="
  local start_epoch
  start_epoch=$(( $(date +%s) + 90 ))
  local pids=()

  for i in $(seq 1 "$count"); do
    local host client_idx report
    host="$(resolve_bench_ip "$MANIFEST" "$i")"
    client_idx=$((i - 1))
    report="${REPORTS}/${TAG_BASE}-bc${count}-i${client_idx}-c${SHARD_C}-${SLUG}.json"
    # shellcheck disable=SC1091
    source "$ROOT/lib/ssh.sh"
    ssh_cmd "$host" \
      "export START_EPOCH=${start_epoch} && \
       while [[ \$(date +%s) -lt \$START_EPOCH ]]; do sleep 1; done && \
       export BOSON_BENCH_CLIENT_INDEX=${client_idx} && \
       export BOSON_BENCH_CLIENT_COUNT=${count} && \
       export BOSON_FLEET_SIZE=${FLEET_N} && \
       export BOSON_NATS_URLS='${BOSON_NATS_URLS}' && \
       export BOSON_NATS_ENQUEUE_MODE=stream && \
       export BOSON_NATS_SYNC_ACK=0 && \
       export BOSON_NATS_MAX_INFLIGHT=256 && \
       export BOSON_BENCH_STORAGE_TOPOLOGY=nats-fleet-multibench && \
       export BOSON_SKIP_RUN_ROWS=1 && \
       cd ~/boson-bench && mkdir -p reports && \
       ./boson-bench run --experiment bm-be4 --backend nats --topology isolated-lab \
         --telemetry off --hardware ${HARDWARE} --warmup 0 --idempotency-mode none \
         --client-count ${SHARD_C} --pool-count ${FLEET_N} --pool-layout distinct \
         --storage-topology nats-fleet-multibench \
         --report reports/$(basename "${report}")" &
    pids+=($!)
  done

  local fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  if [[ "$fail" -ne 0 ]]; then
    echo "One or more bench clients failed for bc=${count}" >&2
    exit 1
  fi

  for i in $(seq 1 "$count"); do
    local host client_idx remote local
    host="$(resolve_bench_ip "$MANIFEST" "$i")"
    client_idx=$((i - 1))
    remote="~/boson-bench/reports/${TAG_BASE}-bc${count}-i${client_idx}-c${SHARD_C}-${SLUG}.json"
    local="${REPORTS}/${TAG_BASE}-bc${count}-i${client_idx}-c${SHARD_C}-${SLUG}.json"
    # shellcheck disable=SC1091
    source "$ROOT/lib/ssh.sh"
    scp_from "$host" "~/${remote}" "$local"
  done

  cd "$REPO_ROOT"
  if [[ "$count" -gt 1 ]]; then
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
      be4-aggregate --hardware "$HARDWARE" --backend nats \
      --reports-dir "$REPORTS" \
      --cell-prefix "$TAG_BASE"
  fi
}

for bc in 1 2 4; do
  if [[ "$bc" -gt "$BENCH_COUNT" ]]; then
    echo "skip bc=${bc}: BENCH_COUNT=${BENCH_COUNT}" >&2
    continue
  fi
  run_cell "$bc"
done

cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  be4-multibench-curve --hardware "$HARDWARE" --backend nats \
  --reports-dir "$REPORTS" || true

"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

echo "Multi-bench BE4 sweep done. Log: $LOG"
