#!/usr/bin/env bash
# Phase F: D7 fetch batch + D8 skip-claim-KV A/B on single NATS broker.
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
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-16}"
export BOSON_NATS_SYNC_ACK=0
export BOSON_NATS_MAX_INFLIGHT=256
export BOSON_BENCH_STORAGE_TOPOLOGY=nats-1
export BOSON_BROKER=nats
export BOSON_NATIVE_MANIFEST="boson-nats-1"
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-f-$(date -u +%Y%m%d)}"

LOG="$ROOT/state/run-bd2-claim-path-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Phase F: claim path (D7 + D8) =========="

"$ROOT/scripts/provision-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"

export BOSON_TIER3_PHASE=drain-fetch-batch
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST" || true

# Pick best batch from F1 reports (default 1 if curve not parsed).
BEST_BATCH=1
BEST_RATE=0
REPORTS="$REPO_ROOT/profiling/boson-bench/reports"
for f in "$REPORTS"/bm-bd2-fetch-b*-w"${BOSON_BD2_WORKER_COUNT}"-*-nats-*.json; do
  [[ -f "$f" ]] || continue
  rate="$(python3 -c "import json; d=json.load(open('$f')); print(d.get('metrics',{}).get('drain_ops_per_sec') or 0)" 2>/dev/null || echo 0)"
  batch="$(basename "$f" | sed -n 's/.*-fetch-b\([0-9]*\)-.*/\1/p')"
  if python3 -c "import sys; sys.exit(0 if float('$rate') > float('$BEST_RATE') else 1)"; then
    BEST_RATE="$rate"
    BEST_BATCH="$batch"
  fi
done
echo "F1 best batch=$BEST_BATCH rate=$BEST_RATE/s"

export BOSON_NATS_FETCH_BATCH="$BEST_BATCH"
export BOSON_TIER3_PHASE=drain-skip-claim-kv
"$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST" || true
"$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"

echo "Phase F done. Log: $LOG"
