#!/usr/bin/env bash
# Redis BE4 enqueue (fill) replay on aws-c6i.large — mirrors NATS c6i-full B1+B2.
# Optional F0: be4-gate (BE1/BE2/BE4 headline). Required: F1 publisher + F2 shard sweeps.
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"

export BOSON_BENCH_INSTANCE_TYPE=c6i.large
export BOSON_BROKER_INSTANCE_TYPE="${BOSON_REDIS_INSTANCE_TYPE:-t3.medium}"
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_BROKER=redis
export BOSON_FLEET_SIZE=1
export BOSON_NATIVE_MANIFEST="${BOSON_NATIVE_MANIFEST:-boson-redis-1}"
export BOSON_BENCH_STORAGE_TOPOLOGY=redis-1
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-be4-redis-$(date -u +%Y%m%d)}"
export BOSON_BE4_SWEEP_POOLS="${BOSON_BE4_SWEEP_POOLS:-single}"

LOG="$ROOT/state/run-be4-redis-campaign-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Redis BE4 fill campaign (F0 optional + F1 + F2) =========="

cleanup() {
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST" 2>/dev/null || true
}
trap cleanup EXIT

run_cell() {
  local phase="$1"
  local label="$2"
  echo "========== Cell ${label}: TIER3_PHASE=${phase} =========="
  export BOSON_TIER3_PHASE="$phase"
  "$ROOT/scripts/provision-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"

  if [[ "${BOSON_RUN_E2E_GATE:-0}" == "1" ]]; then
    MANIFEST="$(manifest_read "$BOSON_NATIVE_MANIFEST")"
    BROKER_PRIV="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['private_ip'] for i in m['instances'] if i['role'] == 'redis'))
")"
    export BOSON_TEST_REDIS_URL="redis://${BROKER_PRIV}:6379"
    echo ">>> E2E gate (BOSON_TEST_REDIS_URL=$BOSON_TEST_REDIS_URL)"
    bash "$REPO_ROOT/infra/native-aws/scripts/run-redis-e2e.sh"
  fi

  "${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
  BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
  trap - EXIT
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
  trap cleanup EXIT
}

if [[ "${BOSON_BE4_RUN_F0:-0}" == "1" ]]; then
  run_cell be4-gate "F0"
fi

run_cell publisher "F1"
run_cell shard "F2"

cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  be4-publisher-curve --hardware aws-c6i-large --backend redis \
  --reports-dir profiling/boson-bench/reports || true
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  be4-shard-curve --hardware aws-c6i-large --backend redis \
  --reports-dir profiling/boson-bench/reports || true

echo "Redis BE4 fill complete. Log: $LOG"
echo "Curves: profiling/boson-bench/reports/scaling-curve-be4-publishers-aws-c6i-large-redis-k1-shared.json"
echo "        profiling/boson-bench/reports/scaling-curve-be4-shards-aws-c6i-large-redis.json"
