#!/usr/bin/env bash
# Resume Phase D after interrupted campaign: D2 N=4 + D3 multibench.
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
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-64}"
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-fleet
export BOSON_BROKER=nats
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-resume-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-bd2-resume-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

if [[ ! -f "$REPO_ROOT/profiling/boson-bench/reports/bm-bd2-fleet-n4-k4-w"*.json ]]; then
  echo "========== Resume D2: N=4 fleet drain =========="
  export BOSON_FLEET_SIZE=4
  export BOSON_NATIVE_MANIFEST="boson-nats-fleet-4"
  export BOSON_FLEET_CURVE=0
  export BOSON_TIER3_PHASE=drain-fleet-shard

  "$BF/provision-fleet.sh"
  "$BF/bootstrap-fleet.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE=4

  "$ROOT/scripts/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
  eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
  export BOSON_FLEET_SIZE=4
  export BOSON_TIER3_PHASE=drain-fleet-shard
  "$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"

  BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
else
  echo "D2 N=4 report already present; skip"
fi

cd "$REPO_ROOT"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/boson-target}" cargo run -p boson-bench --release -- \
  bd2-fleet-curve --hardware aws-c6i-large --backend nats \
  --reports-dir profiling/boson-bench/reports || true

echo "========== Resume D3: multibench =========="
bash "$BF/run-bd2-multibench-sweep-aws.sh"

echo "BD2 resume complete. Log: $LOG"
