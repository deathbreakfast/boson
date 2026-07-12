#!/usr/bin/env bash
# Phase A: c6i.large NATS broker sizing (bench + broker both c6i.large).
# Runs publisher K=1 sweep + shard K=1/4/10/32 @ C=256.
#
# Usage: ./run-tier3-c6i-broker-aws.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"

export BOSON_BENCH_INSTANCE_TYPE=c6i.large
export BOSON_BROKER_INSTANCE_TYPE=c6i.large
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_TIER3_NATS_ENQUEUE=stream
export BOSON_TIER3_PHASE=c6i-broker-sizing
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-1-c6i-broker
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-tier3-c6i-broker-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-tier3-c6i-broker-aws-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Phase A c6i.large broker sizing =========="
echo "bench=$BOSON_BENCH_INSTANCE_TYPE broker=$BOSON_BROKER_INSTANCE_TYPE hardware=$BOSON_BENCH_HARDWARE"
echo "phase=$BOSON_TIER3_PHASE campaign=$BOSON_NATIVE_CAMPAIGN"

export BOSON_BROKER=nats
export BOSON_NATIVE_MANIFEST="boson-nats-1"
"$ROOT/scripts/provision-broker-1.sh"
"$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

echo "Phase A done. Log: $LOG"
echo "Reports: $REPO_ROOT/profiling/boson-bench/reports/*c6i-broker*"
