#!/usr/bin/env bash
# Phase D full campaign: D0 worker → D1 shard → D2 fleet → D3 multibench (NATS).
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$BF/../../.." && pwd)"
CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-campaign-$(date -u +%Y%m%d)}"
export BOSON_NATIVE_CAMPAIGN="$CAMPAIGN"

echo "========== Phase D NATS campaign ($CAMPAIGN) =========="

bash "$BF/run-bd2-worker-sweep-aws.sh"
# Set W* from worker curve if available; default 32
W_STAR="${BOSON_BD2_WORKER_COUNT:-32}"
CURVE="${REPO_ROOT}/profiling/boson-bench/reports/scaling-curve-bd2-workers-aws-c6i-large-nats.json"
if [[ -f "$CURVE" ]]; then
  W_STAR="$(python3 -c "
import json
with open('$CURVE') as f:
    c = json.load(f)
print(c.get('saturation_worker_count') or c.get('peak_worker_count', $W_STAR))
" 2>/dev/null || echo "$W_STAR")"
fi
export BOSON_BD2_WORKER_COUNT="$W_STAR"
echo "Using W*=$W_STAR for D1-D3"

bash "$BF/run-bd2-shard-sweep-aws.sh"
bash "$BF/run-bd2-fleet-sweep-aws.sh"
bash "$BF/run-bd2-multibench-sweep-aws.sh"

echo "Phase D NATS campaign complete: $CAMPAIGN"
