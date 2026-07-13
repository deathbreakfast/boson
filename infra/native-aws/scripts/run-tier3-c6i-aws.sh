#!/usr/bin/env bash
# Phase 2 AWS campaign: Photon-aligned hardware (c6i.large bench + t3.medium broker).
# Runs publisher saturation (K=1), shard sweep (K=1,4,10,32), BD2 dequeue smoke.
#
# Usage: ./run-tier3-c6i-aws.sh [nats]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"

# Force Photon-aligned topology (ignore inherited env from prior campaigns).
export BOSON_BENCH_INSTANCE_TYPE=c6i.large
export BOSON_BROKER_INSTANCE_TYPE=t3.medium
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_TIER3_NATS_ENQUEUE=stream
export BOSON_TIER3_PHASE=c6i-full
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-tier3-c6i-$(date -u +%Y%m%d)}"

TARGET="${1:-nats}"
LOG="$ROOT/state/run-tier3-c6i-aws-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Phase 2 c6i campaign =========="
echo "bench=$BOSON_BENCH_INSTANCE_TYPE broker=$BOSON_BROKER_INSTANCE_TYPE hardware=$BOSON_BENCH_HARDWARE"
echo "phase=$BOSON_TIER3_PHASE enqueue=$BOSON_TIER3_NATS_ENQUEUE campaign=$BOSON_NATIVE_CAMPAIGN"

case "$TARGET" in
  nats)
    export BOSON_BROKER=nats
    export BOSON_NATIVE_MANIFEST="boson-nats-1"
    echo "========== Tier 3 NATS on $BOSON_BENCH_HARDWARE =========="
    "$ROOT/scripts/provision-broker-1.sh"
    "$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
    "${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
    "$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
    BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
    "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
    ;;
  *)
    echo "usage: $0 [nats]" >&2
    exit 1
    ;;
esac

echo "Phase 2 c6i campaign done. Log: $LOG"
echo "Reports: $REPO_ROOT/profiling/boson-bench/reports/*-${BOSON_BENCH_HARDWARE}*.json"
