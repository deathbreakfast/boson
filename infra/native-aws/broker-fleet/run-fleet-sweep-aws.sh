#!/usr/bin/env bash
# AWS broker fleet sweep: N=1,2,4 standalone NATS + pool routing.
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
export BOSON_TIER3_NATS_ENQUEUE=stream
export BOSON_TIER3_PHASE=fleet-shard
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-fleet
export BOSON_BROKER=nats
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-fleet-sweep-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-fleet-sweep-aws-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Boson NATS broker fleet sweep =========="
echo "bench=$BOSON_BENCH_INSTANCE_TYPE broker=$BOSON_BROKER_INSTANCE_TYPE"

for N in 1 2 4; do
  echo "========== Fleet size N=$N =========="
  export BOSON_FLEET_SIZE="$N"
  export BOSON_NATIVE_MANIFEST="boson-nats-fleet-${N}"
  export BOSON_FLEET_CURVE=0

  "$BF/provision-fleet.sh"
  "$BF/bootstrap-fleet.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE="$N"

  "${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE="$N"
  export BOSON_TIER3_PHASE=fleet-shard
  "$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"

  BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
done

export BOSON_FLEET_CURVE=1
cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  be4-fleet-curve --hardware aws-c6i-large --backend nats \
  --reports-dir profiling/boson-bench/reports || true

echo "Fleet sweep done. Log: $LOG"
