#!/usr/bin/env bash
# Redis replay of Phase D ladder (D0-D3) after NATS campaign.
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
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-16}"
export BOSON_BD2_WORKER_GRID="${BOSON_BD2_WORKER_GRID:-1 4 16 32}"
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-redis-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-bd2-redis-campaign-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Phase E2: Redis D0–D3 replay =========="

# D0: worker sweep on single redis broker
export BOSON_FLEET_SIZE=1
export BOSON_NATIVE_MANIFEST="boson-redis-1"
export BOSON_BENCH_STORAGE_TOPOLOGY=redis-1
export BOSON_TIER3_PHASE=drain-worker

cleanup() {
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST" 2>/dev/null || true
}
trap cleanup EXIT

"$ROOT/scripts/provision-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST" || true
trap - EXIT
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  bd2-worker-curve --hardware aws-c6i-large --backend redis \
  --reports-dir profiling/boson-bench/reports || true

# D1: shard sweep
export BOSON_TIER3_PHASE=drain-shard
"$ROOT/scripts/provision-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST" || true
trap - EXIT
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  bd2-shard-curve --hardware aws-c6i-large --backend redis \
  --reports-dir profiling/boson-bench/reports || true

# D2: fleet drain sweep N=1,2,4 with K=N pool routing
echo "========== Redis D2: fleet drain sweep =========="
export BOSON_TIER3_PHASE=drain-fleet-shard
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-32}"
export BOSON_BENCH_STORAGE_TOPOLOGY=redis-fleet
export BOSON_FLEET_CURVE=0

for N in 1 2 4; do
  echo "========== Redis fleet size N=$N =========="
  export BOSON_FLEET_SIZE="$N"
  export BOSON_NATIVE_MANIFEST="boson-redis-fleet-${N}"

  "$BF/provision-fleet.sh"
  "$BF/bootstrap-fleet.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE="$N"

  "${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE="$N"
  export BOSON_TIER3_PHASE=drain-fleet-shard
  "$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"

  BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST" || true
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
done

export BOSON_FLEET_CURVE=1
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  bd2-fleet-curve --hardware aws-c6i-large --backend redis \
  --reports-dir profiling/boson-bench/reports || true

echo "Redis D0-D2 complete. Run run-bd2-redis-multibench-sweep-aws.sh for D3. Log: $LOG"
