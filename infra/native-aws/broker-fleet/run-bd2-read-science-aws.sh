#!/usr/bin/env bash
# Phase H: D5 rep sweep, D4 cluster drain, D11 c6i.large broker BD2.
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
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-16}"
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_BROKER=nats
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-h-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-bd2-read-science-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Phase H: read-replica / topology science =========="

# D5: rep sweep on single broker
export BOSON_BROKER_INSTANCE_TYPE=t3.medium
export BOSON_NATIVE_MANIFEST="boson-nats-1"
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-1
export BOSON_TIER3_PHASE=drain-rep-sweep
"$ROOT/scripts/provision-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

# D4: cluster drain
export BOSON_FLEET_SIZE=4
export BOSON_BROKER_INSTANCE_TYPE=t3.medium
export BOSON_NATIVE_MANIFEST="boson-nats-cluster-ref-4"
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-cluster
export BOSON_TIER3_PHASE=drain-cluster
"$BF/provision-fleet.sh"
"$BF/bootstrap-n4-cluster.sh" "$BOSON_NATIVE_MANIFEST"
N0_PRIV="$(manifest_read "$BOSON_NATIVE_MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['private_ip'] for i in m['instances'] if i['role'] == 'nats-0'))
")"
export BOSON_NATS_URLS=""
export BOSON_TEST_NATS_URL="nats://${N0_PRIV}:4222"
"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
export BOSON_NATS_URLS=""
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

# D11: c6i.large broker
export BOSON_BROKER_INSTANCE_TYPE=c6i.large
export BOSON_NATIVE_MANIFEST="boson-nats-1"
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-1-c6i-broker
export BOSON_TIER3_PHASE=drain-c6i-broker
"$ROOT/scripts/provision-broker-1.sh"
"$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

echo "Phase H done. Log: $LOG"
echo "Reports: $REPO_ROOT/profiling/boson-bench/reports/bm-bd2-rep* bm-bd2-cluster* bm-bd2-c6i-broker*"
