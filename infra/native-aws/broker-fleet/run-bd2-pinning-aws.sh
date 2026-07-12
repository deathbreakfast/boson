#!/usr/bin/env bash
# Phase G1: D9 worker pool pinning vs unpinned on N=4 fleet.
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"

export BOSON_FLEET_SIZE=4
export BOSON_BENCH_INSTANCE_TYPE=c6i.large
export BOSON_BROKER_INSTANCE_TYPE="${BOSON_BROKER_INSTANCE_TYPE:-t3.medium}"
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_TIER3_PHASE=drain-fleet-pin
export BOSON_BD2_WORKER_COUNT=4
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-fleet
export BOSON_BROKER=nats
export BOSON_NATIVE_MANIFEST="boson-nats-fleet-4"
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-g1-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-bd2-pinning-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Phase G1: pool pinning (D9) =========="

"$BF/provision-fleet.sh"
"$BF/bootstrap-fleet.sh" "$BOSON_NATIVE_MANIFEST"
eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
"$ROOT/scripts/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

echo "Phase G1 done. Log: $LOG"
