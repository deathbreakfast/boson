#!/usr/bin/env bash
# Optional RAFT cluster comparison: provision 2 brokers, bootstrap cluster, run BE4 K=1.
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"

export BOSON_FLEET_SIZE=2
export BOSON_BENCH_INSTANCE_TYPE=c6i.large
export BOSON_BROKER_INSTANCE_TYPE=t3.medium
export BOSON_BENCH_HARDWARE=aws-c6i-large
export BOSON_TIER3_NATS_ENQUEUE=stream
export BOSON_TIER3_PHASE=publisher
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATIVE_MANIFEST=boson-nats-cluster-2
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-cluster-n2-$(date -u +%Y%m%d)}"
export BOSON_BROKER=nats
export BOSON_BE4_SWEEP_POOLS=single

"$BF/provision-fleet.sh"
"$BF/bootstrap-n2-cluster.sh" "$BOSON_NATIVE_MANIFEST"
eval "$("$BF/export-fleet-env.sh" "$BOSON_NATIVE_MANIFEST")"
export BOSON_TIER3_PHASE=publisher

"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

echo "RAFT n=2 cluster BE4 publisher sweep done"
