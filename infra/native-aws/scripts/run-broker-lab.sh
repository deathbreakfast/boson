#!/usr/bin/env bash
# Tier 3 capacity campaign: Phase A (BE/BD gates) + Phase B (W=1,4 drain sweeps).
# Usage: BOSON_BROKER=redis|nats ./run-broker-lab.sh [manifest-name]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/ssh.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/hardware.sh"

BROKER="${BOSON_BROKER:?set BOSON_BROKER to redis or nats}"
MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-${BROKER}-1}}"
LOG_DIR="$ROOT/state/${MANIFEST_NAME}"
mkdir -p "$LOG_DIR"

MANIFEST="$(manifest_read "$MANIFEST_NAME")"
BENCH_TYPE="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(m.get('bench_instance_type') or next(
    (i.get('instance_type') for i in m['instances'] if i['role'] == 'bench'), m.get('instance_type', 't3.medium')))
")"
HARDWARE="${BOSON_BENCH_HARDWARE:-$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(m.get('hardware', ''))
")}"
if [[ -z "$HARDWARE" ]]; then
  HARDWARE="$(boson_hardware_tag_from_instance_type "$BENCH_TYPE")"
fi
BENCH_HOST="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == 'bench'))
")"
BROKER_PRIV="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
role = sys.argv[1]
broker = m.get('broker', 'nats')
try:
    print(next(i['private_ip'] for i in m['instances'] if i['role'] == role))
except StopIteration:
    prefix = f'{broker}-'
    brokers = sorted((i for i in m['instances'] if i['role'].startswith(prefix)), key=lambda i: i['role'])
    print(brokers[0]['private_ip'] if brokers else '')
" "$BROKER")"
BROKER_PUB="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
role = sys.argv[1]
broker = m.get('broker', 'nats')
try:
    print(next(i['public_ip'] for i in m['instances'] if i['role'] == role))
except StopIteration:
    prefix = f'{broker}-'
    brokers = sorted((i for i in m['instances'] if i['role'].startswith(prefix)), key=lambda i: i['role'])
    print(brokers[0]['public_ip'] if brokers else '')
" "$BROKER")"

case "$BROKER" in
  redis) BROKER_URL="redis://${BROKER_PRIV}:6379" ;;
  nats) BROKER_URL="nats://${BROKER_PRIV}:4222" ;;
esac

STORAGE_TOPO="${BOSON_BENCH_STORAGE_TOPOLOGY:-${BROKER}-1}"
FLEET_NATS_URLS="${BOSON_NATS_URLS:-}"
FLEET_REDIS_URLS="${BOSON_REDIS_URLS:-}"
FLEET_SIZE_ENV="${BOSON_FLEET_SIZE:-}"
FLEET_CURVE_ENV="${BOSON_FLEET_CURVE:-0}"
BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-32}"
BD2_WORKER_GRID="${BOSON_BD2_WORKER_GRID:-}"
NATS_FETCH_BATCH="${BOSON_NATS_FETCH_BATCH:-}"
NATS_STREAM_REPLICAS="${BOSON_NATS_STREAM_REPLICAS:-}"
BD2_PIN_POOLS="${BOSON_BD2_PIN_WORKER_POOLS:-}"
SKIP_CLAIM_KV="${BOSON_BENCH_SKIP_CLAIM_KV:-}"
REPORT_PREFIX="${BROKER}-isolated-lab-off-${HARDWARE}"

TIER3_PHASE="${BOSON_TIER3_PHASE:-full}"
echo ">>> tier3-capacity ${BROKER} phase=${TIER3_PHASE} bench=$BENCH_HOST broker=$BROKER_PRIV url=$BROKER_URL"

ssh_wait_ready "$BENCH_HOST"
ssh_cmd "$BENCH_HOST" "mkdir -p ~/boson-bench/reports"
ssh_cmd "$BENCH_HOST" "bash -c 'killall -9 boson-bench 2>/dev/null || true; sleep 1'"

# Deploy gate: binary must include BE/BD experiments.
EXP_LIST="$(ssh_cmd "$BENCH_HOST" "~/boson-bench/boson-bench experiments 2>/dev/null" || true)"
for req in bm-be1 bm-bd1; do
  if ! grep -q "$req" <<< "$EXP_LIST"; then
    echo "deploy gate failed: boson-bench on $BENCH_HOST missing $req" >&2
    echo "Run deploy-bench-binary.sh first" >&2
    exit 1
  fi
done

ssh_cmd_stdin "$BENCH_HOST" "cat > ~/boson-bench/run-campaign.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
TIER3_PHASE=${TIER3_PHASE}
export BOSON_BENCH_HARDWARE=$HARDWARE
export BOSON_BENCH_STORAGE_TOPOLOGY=$STORAGE_TOPO
export BOSON_BENCH_PREFILL_COUNT=\${BOSON_BENCH_PREFILL_COUNT:-10000}
export BOSON_SKIP_RUN_ROWS=1
if [[ "$BROKER" == "nats" ]]; then
  export BOSON_NATS_QUEUE_MODE=workqueue
  if [[ "${BOSON_TIER3_NATS_ENQUEUE:-}" == "stream" ]]; then
    export BOSON_NATS_ENQUEUE_MODE=stream
    export BOSON_NATS_MAX_INFLIGHT=${BOSON_NATS_MAX_INFLIGHT:-256}
    export BOSON_NATS_SYNC_ACK=${BOSON_NATS_SYNC_ACK:-0}
  fi
fi
export BOSON_TEST_REDIS_URL=$BROKER_URL
export BOSON_TEST_NATS_URL=$BROKER_URL
export BOSON_NATS_URLS='${FLEET_NATS_URLS}'
export BOSON_REDIS_URLS='${FLEET_REDIS_URLS}'
export BOSON_FLEET_SIZE='${FLEET_SIZE_ENV}'
export BOSON_FLEET_CURVE='${FLEET_CURVE_ENV}'
export BOSON_BD2_WORKER_COUNT='${BD2_WORKER_COUNT}'
export BOSON_BD2_WORKER_GRID='${BD2_WORKER_GRID}'
export BOSON_NATS_FETCH_BATCH='${NATS_FETCH_BATCH}'
export BOSON_NATS_STREAM_REPLICAS='${NATS_STREAM_REPLICAS}'
export BOSON_BD2_PIN_WORKER_POOLS='${BD2_PIN_POOLS}'
export BOSON_BENCH_SKIP_CLAIM_KV='${SKIP_CLAIM_KV}'
cd ~/boson-bench
BENCH=./boson-bench
REPORTS=~/boson-bench/reports
SLUG=$REPORT_PREFIX
mkdir -p "\$REPORTS"
rm -f campaign.done campaign.failed
trap 'touch campaign.failed' ERR

run_one() {
  local id="\$1"
  local report="\$2"
  shift 2
  echo "=== \$id backend=$BROKER report=\$report \$* ==="
  "\$BENCH" run --experiment "\$id" --backend $BROKER --topology isolated-lab \\
    --telemetry off --hardware $HARDWARE --warmup 0 \\
    --idempotency-mode none \\
    --report "\$REPORTS/\${report}.json" \\
    "\$@"
}

if [[ "\$TIER3_PHASE" == "be4-gate" ]]; then
  echo "========== BE4 enqueue gate (BE1/BE2/BE4 only, no drain) =========="
  run_one bm-be1 "bm-be1-\$SLUG"
  run_one bm-be2 "bm-be2-\$SLUG"
  run_one bm-be4 "bm-be4-\$SLUG" --pool-layout distinct
fi

if [[ "\$TIER3_PHASE" == "full" || "\$TIER3_PHASE" == "a-only" ]]; then
  export BOSON_BENCH_WORKER_COUNT=\${BOSON_BENCH_WORKER_COUNT:-10}
  echo "========== Phase A: core gate =========="
  run_one bm-b0 "bm-b0-\$SLUG"
  run_one bm-be1 "bm-be1-\$SLUG"
  run_one bm-be2 "bm-be2-\$SLUG"
  run_one bm-be4 "bm-be4-\$SLUG" --pool-layout distinct
  run_one bm-bd1 "bm-bd1-\$SLUG" --worker-count 10 --worker-poll-ms 0
  run_one bm-bd2 "bm-bd2-\$SLUG" --worker-count 10 --worker-poll-ms 0
fi

if [[ "\$TIER3_PHASE" == "full" || "\$TIER3_PHASE" == "sweep" ]]; then
  echo "========== Phase B: drain worker sweep =========="
  unset BOSON_BENCH_WORKER_COUNT
  for W in 1 4; do
    run_one bm-bd1 "bm-bd1-w\${W}-\$SLUG" --worker-count "\$W" --worker-poll-ms 0
    run_one bm-bd2 "bm-bd2-w\${W}-\$SLUG" --worker-count "\$W" --worker-poll-ms 0
  done
fi

if [[ "\$TIER3_PHASE" == "partition" ]]; then
  echo "========== Phase C: BE4 partition sweep (redis) =========="
  for K in 1 4 10; do
    run_one bm-be4 "bm-be4-k\${K}-\$SLUG" --pool-count "\$K" --pool-layout distinct --client-count 64
  done
fi

if [[ "\$TIER3_PHASE" == "shard" ]]; then
  echo "========== Phase C2: BE4 shard sweep (NATS multi-stream scaling) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  SHARD_C=\${BOSON_BE4_SHARD_CLIENT_COUNT:-256}
  for K in 1 4 10 32; do
    if [[ "\$K" == "1" ]]; then
      LAYOUT=shared
    else
      LAYOUT=distinct
    fi
    run_one bm-be4 "bm-be4-k\${K}-c\${SHARD_C}-\$SLUG" \\
      --pool-count "\$K" --pool-layout "\$LAYOUT" --client-count "\$SHARD_C"
  done
  "\$BENCH" be4-shard-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-be4-shards-${HARDWARE}-${BROKER}.json" || true
fi

if [[ "\$TIER3_PHASE" == "publisher" ]]; then
  echo "========== Phase D: BE4 publisher sweep (NATS stream saturation) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  POOLS_MODE=\${BOSON_BE4_SWEEP_POOLS:-single}
  if [[ "\$POOLS_MODE" == "distinct" ]]; then
    POOL_COUNT=10
    POOL_LAYOUT=distinct
    POOL_TAG=k10-distinct
  else
    POOL_COUNT=1
    POOL_LAYOUT=shared
    POOL_TAG=k1-shared
  fi
  for C in 1 8 32 64 128 256 512; do
    run_one bm-be4 "bm-be4-c\${C}-\${POOL_TAG}-\$SLUG" \\
      --client-count "\$C" --pool-count "\$POOL_COUNT" --pool-layout "\$POOL_LAYOUT"
  done
  "\$BENCH" be4-publisher-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-be4-publishers-${HARDWARE}-${BROKER}-\${POOL_TAG}.json" || true
fi

if [[ "\$TIER3_PHASE" == "c6i-full" ]]; then
  echo "========== Phase E: c6i-full campaign (publisher + shard + BD2 smoke) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}

  echo "--- B1: publisher saturation K=1 ---"
  POOL_COUNT=1
  POOL_LAYOUT=shared
  POOL_TAG=k1-shared
  for C in 1 8 32 64 128 256 512; do
    run_one bm-be4 "bm-be4-c\${C}-\${POOL_TAG}-\$SLUG" \\
      --client-count "\$C" --pool-count "\$POOL_COUNT" --pool-layout "\$POOL_LAYOUT"
  done
  "\$BENCH" be4-publisher-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-be4-publishers-${HARDWARE}-${BROKER}-\${POOL_TAG}.json" || true

  echo "--- B2: shard sweep K=1,4,10,32 @ C=256 ---"
  SHARD_C=\${BOSON_BE4_SHARD_CLIENT_COUNT:-256}
  for K in 1 4 10 32; do
    if [[ "\$K" == "1" ]]; then
      LAYOUT=shared
    else
      LAYOUT=distinct
    fi
    run_one bm-be4 "bm-be4-k\${K}-c\${SHARD_C}-\$SLUG" \\
      --pool-count "\$K" --pool-layout "\$LAYOUT" --client-count "\$SHARD_C"
  done
  "\$BENCH" be4-shard-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-be4-shards-${HARDWARE}-${BROKER}.json" || true

  echo "--- B3: BD2 dequeue smoke ---"
  run_one bm-bd2 "bm-bd2-w10-\$SLUG" --worker-count 10 --worker-poll-ms 0
  run_one bm-bd2 "bm-bd2-w4-\$SLUG" --worker-count 4 --worker-poll-ms 0
fi

if [[ "\$TIER3_PHASE" == "c6i-broker-sizing" ]]; then
  echo "========== Phase A: c6i.large broker sizing =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  export BOSON_BENCH_STORAGE_TOPOLOGY=\${BOSON_BENCH_STORAGE_TOPOLOGY:-nats-1-c6i-broker}

  echo "--- publisher K=1 ---"
  for C in 1 8 32 64 128 256 512; do
    run_one bm-be4 "bm-be4-c\${C}-k1-shared-c6i-broker-\$SLUG" \\
      --client-count "\$C" --pool-count 1 --pool-layout shared
  done
  "\$BENCH" be4-publisher-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-be4-publishers-${HARDWARE}-${BROKER}-k1-shared-c6i-broker.json" || true

  echo "--- shard K=1,4,10,32 @ C=256 ---"
  SHARD_C=\${BOSON_BE4_SHARD_CLIENT_COUNT:-256}
  for K in 1 4 10 32; do
    if [[ "\$K" == "1" ]]; then LAYOUT=shared; else LAYOUT=distinct; fi
    run_one bm-be4 "bm-be4-k\${K}-c\${SHARD_C}-c6i-broker-\$SLUG" \\
      --pool-count "\$K" --pool-layout "\$LAYOUT" --client-count "\$SHARD_C"
  done
  "\$BENCH" be4-shard-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-be4-shards-${HARDWARE}-${BROKER}-c6i-broker.json" || true
fi

if [[ "\$TIER3_PHASE" == "fleet-shard" ]]; then
  echo "========== Phase F: BE4 broker fleet sweep (pool routing) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  FLEET_N=\${BOSON_FLEET_SIZE:-1}
  SHARD_C=\${BOSON_BE4_SHARD_CLIENT_COUNT:-256}
  run_one bm-be4 "bm-be4-fleet-n\${FLEET_N}-k\${FLEET_N}-c\${SHARD_C}-\$SLUG" \\
    --pool-count "\$FLEET_N" --pool-layout distinct --client-count "\$SHARD_C"
  if [[ "\${BOSON_FLEET_CURVE:-0}" == "1" ]]; then
    "\$BENCH" be4-fleet-curve --hardware $HARDWARE --backend $BROKER \\
      --reports-dir "\$REPORTS" \\
      --out "\$REPORTS/scaling-curve-be4-fleet-${HARDWARE}-${BROKER}.json" || true
  fi
fi

if [[ "\$TIER3_PHASE" == "cluster-shard" ]]; then
  echo "========== Phase G: BE4 JetStream cluster (K pools, single client) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  unset BOSON_NATS_URLS
  CLUSTER_K=\${BOSON_CLUSTER_POOL_COUNT:-4}
  SHARD_C=\${BOSON_BE4_SHARD_CLIENT_COUNT:-256}
  run_one bm-be4 "bm-be4-cluster-k\${CLUSTER_K}-c\${SHARD_C}-\$SLUG" \\
    --pool-count "\$CLUSTER_K" --pool-layout distinct --client-count "\$SHARD_C" \\
    --storage-topology nats-cluster
fi

if [[ "\$TIER3_PHASE" == "drain-worker" ]]; then
  echo "========== Phase D0: BD2 worker sweep (W grid, K=1) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  unset BOSON_BENCH_WORKER_COUNT
  W_GRID=\${BOSON_BD2_WORKER_GRID:-1 2 4 8 16 32 64}
  for W in \$W_GRID; do
    run_one bm-bd2 "bm-bd2-w\${W}-\$SLUG" \\
      --worker-count "\$W" --worker-poll-ms 0 \\
      --pool-count 1 --pool-layout shared
  done
  "\$BENCH" bd2-worker-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-bd2-workers-${HARDWARE}-${BROKER}.json" || true
fi

if [[ "\$TIER3_PHASE" == "drain-shard" ]]; then
  echo "========== Phase D1: BD2 shard sweep (K grid @ W*) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-32}
  for K in 1 4 10 32; do
    if [[ "\$K" == "1" ]]; then
      LAYOUT=shared
    else
      LAYOUT=distinct
    fi
    run_one bm-bd2 "bm-bd2-k\${K}-w\${BD2_W}-\$SLUG" \\
      --worker-count "\$BD2_W" --worker-poll-ms 0 \\
      --pool-count "\$K" --pool-layout "\$LAYOUT"
  done
  "\$BENCH" bd2-shard-curve --hardware $HARDWARE --backend $BROKER \\
    --reports-dir "\$REPORTS" \\
    --out "\$REPORTS/scaling-curve-bd2-shards-${HARDWARE}-${BROKER}.json" || true
fi

if [[ "\$TIER3_PHASE" == "drain-fleet-shard" ]]; then
  echo "========== Phase D2: BD2 broker fleet drain (pool routing) =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  FLEET_N=\${BOSON_FLEET_SIZE:-1}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-32}
  PREFILL=\${BOSON_BENCH_PREFILL_COUNT:-10000}
  run_one bm-bd2 "bm-bd2-fleet-n\${FLEET_N}-k\${FLEET_N}-w\${BD2_W}-c\${PREFILL}-\$SLUG" \\
    --worker-count "\$BD2_W" --worker-poll-ms 0 \\
    --pool-count "\$FLEET_N" --pool-layout distinct \\
    --prefill-count "\$PREFILL"
  if [[ "\${BOSON_FLEET_CURVE:-0}" == "1" ]]; then
    "\$BENCH" bd2-fleet-curve --hardware $HARDWARE --backend $BROKER \\
      --reports-dir "\$REPORTS" \\
      --out "\$REPORTS/scaling-curve-bd2-fleet-${HARDWARE}-${BROKER}.json" || true
  fi
fi

if [[ "\$TIER3_PHASE" == "drain-fetch-batch" ]]; then
  echo "========== Phase F1: BD2 fetch batch sweep @ W* =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-16}
  FETCH_GRID=\${BOSON_BD2_FETCH_BATCH_GRID:-1 8 64}
  for BATCH in \$FETCH_GRID; do
    export BOSON_NATS_FETCH_BATCH="\$BATCH"
    run_one bm-bd2 "bm-bd2-fetch-b\${BATCH}-w\${BD2_W}-\$SLUG" \\
      --worker-count "\$BD2_W" --worker-poll-ms 0 \\
      --pool-count 1 --pool-layout shared
  done
fi

if [[ "\$TIER3_PHASE" == "drain-skip-claim-kv" ]]; then
  echo "========== Phase F2: BD2 skip claim KV A/B @ W* =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-16}
  BEST_BATCH=\${BOSON_NATS_FETCH_BATCH:-1}
  export BOSON_NATS_FETCH_BATCH="\$BEST_BATCH"
  for SKIP in 0 1; do
    if [[ "\$SKIP" == "1" ]]; then
      export BOSON_BENCH_SKIP_CLAIM_KV=1
      TAG=skipkv1
    else
      unset BOSON_BENCH_SKIP_CLAIM_KV
      TAG=skipkv0
    fi
    run_one bm-bd2 "bm-bd2-\${TAG}-b\${BEST_BATCH}-w\${BD2_W}-\$SLUG" \\
      --worker-count "\$BD2_W" --worker-poll-ms 0 \\
      --pool-count 1 --pool-layout shared
  done
fi

if [[ "\$TIER3_PHASE" == "drain-fleet-pin" ]]; then
  echo "========== Phase G1: BD2 fleet pool pinning N=4 W=4 =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  FLEET_N=\${BOSON_FLEET_SIZE:-4}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-4}
  PREFILL=\${BOSON_BENCH_PREFILL_COUNT:-10000}
  unset BOSON_BD2_PIN_WORKER_POOLS
  run_one bm-bd2 "bm-bd2-fleet-n\${FLEET_N}-k\${FLEET_N}-w\${BD2_W}-unpinned-\$SLUG" \\
    --worker-count "\$BD2_W" --worker-poll-ms 0 \\
    --pool-count "\$FLEET_N" --pool-layout distinct \\
    --prefill-count "\$PREFILL"
  export BOSON_BD2_PIN_WORKER_POOLS=1
  run_one bm-bd2 "bm-bd2-fleet-n\${FLEET_N}-k\${FLEET_N}-w\${BD2_W}-pinned-\$SLUG" \\
    --worker-count "\$BD2_W" --worker-poll-ms 0 \\
    --pool-count "\$FLEET_N" --pool-layout distinct \\
    --prefill-count "\$PREFILL"
fi

if [[ "\$TIER3_PHASE" == "drain-rep-sweep" ]]; then
  echo "========== Phase H D5: JetStream num_replicas sweep =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-16}
  for REP in 1 2 3; do
    export BOSON_NATS_STREAM_REPLICAS="\$REP"
    run_one bm-bd2 "bm-bd2-rep\${REP}-w\${BD2_W}-\$SLUG" \\
      --worker-count "\$BD2_W" --worker-poll-ms 0 \\
      --pool-count 1 --pool-layout shared
  done
fi

if [[ "\$TIER3_PHASE" == "drain-cluster" ]]; then
  echo "========== Phase H D4: BD2 JetStream cluster drain K=N =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  CLUSTER_K=\${BOSON_CLUSTER_K:-4}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-16}
  PREFILL=\${BOSON_BENCH_PREFILL_COUNT:-10000}
  run_one bm-bd2 "bm-bd2-cluster-k\${CLUSTER_K}-w\${BD2_W}-\$SLUG" \\
    --worker-count "\$BD2_W" --worker-poll-ms 0 \\
    --pool-count "\$CLUSTER_K" --pool-layout distinct \\
    --prefill-count "\$PREFILL" \\
    --storage-topology nats-cluster
fi

if [[ "\$TIER3_PHASE" == "drain-c6i-broker" ]]; then
  echo "========== Phase H D11: BD2 on c6i.large broker =========="
  export BOSON_NATS_ENQUEUE_MODE=\${BOSON_NATS_ENQUEUE_MODE:-stream}
  export BOSON_NATS_SYNC_ACK=\${BOSON_NATS_SYNC_ACK:-0}
  export BOSON_NATS_MAX_INFLIGHT=\${BOSON_NATS_MAX_INFLIGHT:-256}
  BD2_W=\${BOSON_BD2_WORKER_COUNT:-16}
  run_one bm-bd2 "bm-bd2-c6i-broker-w\${BD2_W}-\$SLUG" \\
    --worker-count "\$BD2_W" --worker-poll-ms 0 \\
    --pool-count 1 --pool-layout shared
fi

echo "DONE phase=\$TIER3_PHASE reports=\$(ls -1 \$REPORTS | wc -l)"
ls -1 "\$REPORTS"
touch campaign.done
EOF

ssh_cmd "$BENCH_HOST" "chmod +x ~/boson-bench/run-campaign.sh"
ssh_cmd_stdin "$BENCH_HOST" "cat > ~/boson-bench/start-campaign.sh" <<'START'
#!/usr/bin/env bash
set -euo pipefail
cd ~/boson-bench
if [[ -f campaign.pid ]]; then kill -9 "$(cat campaign.pid)" 2>/dev/null || true; fi
killall -9 boson-bench 2>/dev/null || true
sleep 1
rm -rf reports
mkdir -p reports
rm -f campaign.done campaign.failed campaign.log
nohup ./run-campaign.sh > campaign.log 2>&1 &
echo $! > campaign.pid
echo "started pid=$(cat campaign.pid)"
START
ssh_cmd "$BENCH_HOST" "chmod +x ~/boson-bench/start-campaign.sh && ~/boson-bench/start-campaign.sh"

echo "campaign started; polling (deadline 7200s)..."
DEADLINE=$((SECONDS + 7200))
while (( SECONDS < DEADLINE )); do
  state="$(ssh_cmd "$BENCH_HOST" "if test -f ~/boson-bench/campaign.done; then echo done; elif test -f ~/boson-bench/campaign.failed; then echo failed; elif test -f ~/boson-bench/campaign.pid && kill -0 \$(cat ~/boson-bench/campaign.pid) 2>/dev/null; then echo running; else echo dead; fi" 2>/dev/null || echo unknown)"
  echo "  state=$state elapsed=$((SECONDS))s"
  case "$state" in
    done)
      scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-${BROKER}-lab.log" || true
      echo "campaign complete"
      exit 0
      ;;
    failed|dead)
      scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-${BROKER}-lab.log" || true
      echo "campaign failed (state=$state)" >&2
      exit 1
      ;;
  esac
  scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-${BROKER}-lab.log" 2>/dev/null || true
  ssh_cmd "$BROKER_PUB" "free -m | head -2; sudo docker stats --no-stream 2>/dev/null | head -5" \
    > "$LOG_DIR/${BROKER}-resources.log" 2>/dev/null || true
  sleep 20
done
echo "timeout after 7200s" >&2
scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-${BROKER}-lab.log" || true
exit 1
