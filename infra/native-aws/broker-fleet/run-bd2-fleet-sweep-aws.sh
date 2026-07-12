#!/usr/bin/env bash
# Phase D2: BD2 broker fleet drain sweep N=1,2,4 with K=N pool routing.
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
export BOSON_BROKER_INSTANCE_TYPE="${BOSON_BROKER_INSTANCE_TYPE:-t3.medium}"
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_TIER3_PHASE=drain-fleet-shard
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-32}"
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-fleet
export BOSON_BROKER=nats
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-d2-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-bd2-fleet-sweep-aws-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Boson BD2 fleet drain sweep (D2) =========="

for N in 1 2 4; do
  echo "========== Fleet size N=$N =========="
  export BOSON_FLEET_SIZE="$N"
  export BOSON_NATIVE_MANIFEST="boson-nats-fleet-${N}"
  export BOSON_FLEET_CURVE=0

  "$BF/provision-fleet.sh"
  "$BF/bootstrap-fleet.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE="$N"

  "$ROOT/scripts/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE="$N"
  export BOSON_TIER3_PHASE=drain-fleet-shard
  export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-32}"
  "$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"

  BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
done

export BOSON_FLEET_CURVE=1
cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  bd2-fleet-curve --hardware aws-c6i-large --backend nats \
  --reports-dir profiling/boson-bench/reports || true

echo "BD2 fleet sweep done. Log: $LOG"
