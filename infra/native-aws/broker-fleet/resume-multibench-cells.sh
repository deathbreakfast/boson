#!/usr/bin/env bash
# Resume multibench benchmark cells on an already-provisioned fleet.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
ROOT="$REPO_ROOT/infra/native-aws"
BF="$ROOT/broker-fleet"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/bench-fleet.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/ssh.sh"

export BOSON_NATIVE_MANIFEST="${BOSON_NATIVE_MANIFEST:-boson-nats-multibench-4}"
export BOSON_FLEET_SIZE="${BOSON_FLEET_SIZE:-4}"

MANIFEST="$(manifest_read "$BOSON_NATIVE_MANIFEST")"
eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"

REPORTS="$REPO_ROOT/profiling/boson-bench/reports"
mkdir -p "$REPORTS"
HARDWARE="aws-c6i-large"
SLUG="nats-isolated-lab-off-${HARDWARE}"
SHARD_C="${BOSON_BE4_SHARD_CLIENT_COUNT:-256}"
FLEET_N="${BOSON_FLEET_SIZE}"
TAG_BASE="bm-be4-fleet-n${FLEET_N}-k${FLEET_N}"

run_cell() {
  local count="$1"
  echo "=== Multi-bench BE4 bc=${count} ==="
  local start_epoch
  start_epoch=$(( $(date +%s) + 90 ))
  local pids=()

  for i in $(seq 1 "$count"); do
    local host idx
    host="$(resolve_bench_ip "$MANIFEST" "$i")"
    idx=$((i - 1))
    ssh_cmd "$host" \
      "export START_EPOCH=${start_epoch} && \
       while [[ \$(date +%s) -lt \$START_EPOCH ]]; do sleep 1; done && \
       export BOSON_BENCH_CLIENT_INDEX=${idx} && \
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
         --report reports/${TAG_BASE}-bc${count}-i${idx}-c${SHARD_C}-${SLUG}.json" &
    pids+=($!)
  done

  local fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  [[ "$fail" -eq 0 ]] || return 1

  for i in $(seq 1 "$count"); do
    local host idx remote local
    host="$(resolve_bench_ip "$MANIFEST" "$i")"
    idx=$((i - 1))
    remote="~/boson-bench/reports/${TAG_BASE}-bc${count}-i${idx}-c${SHARD_C}-${SLUG}.json"
    local="${REPORTS}/${TAG_BASE}-bc${count}-i${idx}-c${SHARD_C}-${SLUG}.json"
    scp_from "$host" "$remote" "$local"
  done

  cd "$REPO_ROOT"
  if [[ "$count" -gt 1 ]]; then
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
      be4-aggregate --hardware "$HARDWARE" --backend nats \
      --reports-dir "$REPORTS" --cell-prefix "$TAG_BASE"
  fi
}

for bc in 1 2 4; do
  if [[ "$bc" -eq 1 ]] && [[ -f "$REPORTS/${TAG_BASE}-bc1-i0-c${SHARD_C}-${SLUG}.json" ]]; then
    echo "skip bc=1 (report exists)"
    continue
  fi
  run_cell "$bc"
done

cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  be4-multibench-curve --hardware "$HARDWARE" --backend nats \
  --reports-dir "$REPORTS"

"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
echo "Multibench resume complete"
