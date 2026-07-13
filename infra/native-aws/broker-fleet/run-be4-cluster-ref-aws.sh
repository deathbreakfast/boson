#!/usr/bin/env bash
# Optional: BE4 K=4 on 4-node JetStream cluster (rep=1 sharded, single bench, NOT pool-routed fleet).
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"

export BOSON_FLEET_SIZE=4
export BOSON_BENCH_INSTANCE_TYPE=c6i.large
export BOSON_BROKER_INSTANCE_TYPE=t3.medium
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_TIER3_NATS_ENQUEUE=stream
export BOSON_TIER3_PHASE=cluster-shard
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_NATIVE_MANIFEST="boson-nats-cluster-ref-4"
export BOSON_NATIVE_CAMPAIGN="boson-cluster-ref-$(date -u +%Y%m%d)"
export BOSON_BROKER=nats
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-cluster

LOG="$ROOT/state/run-be4-cluster-ref-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Boson BE4 cluster reference (K=4, single bench) =========="

"$BF/provision-fleet.sh"
"$BF/bootstrap-n4-cluster.sh" "$BOSON_NATIVE_MANIFEST"

# Single NATS client to cluster (not pool-routed multi-URL fleet).
N0_PRIV="$(manifest_read "$BOSON_NATIVE_MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['private_ip'] for i in m['instances'] if i['role'] == 'nats-0'))
")"
export BOSON_NATS_URLS=""
export BOSON_TEST_NATS_URL="nats://${N0_PRIV}:4222"

"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
export BOSON_TIER3_PHASE=cluster-shard
export BOSON_NATS_URLS=""
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"

BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

echo "Cluster reference BE4 done. Log: $LOG"
