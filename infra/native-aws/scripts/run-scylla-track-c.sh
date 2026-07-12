#!/usr/bin/env bash
# Track C: adapter concurrency levers on scylla-1 (does not overwrite baseline reports).
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
source "$ROOT/lib/artifact.sh"

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-scylla-1}}"
BINARY="$(artifact_local_path)"
HARDWARE="aws-t3-medium"
LOG_DIR="$ROOT/state/${MANIFEST_NAME}"
mkdir -p "$LOG_DIR"

if [[ ! -x "$BINARY" ]]; then
  echo "binary not found: $BINARY (run build-al2023-local.sh first)" >&2
  exit 1
fi

MANIFEST="$(manifest_read "$MANIFEST_NAME")"
BENCH_HOST="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == 'bench'))
")"
SCYLLA_PRIV="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['private_ip'] for i in m['instances'] if i['role'] == 'scylla'))
")"
SCYLLA_PUB="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == 'scylla'))
")"

CONTACT="${SCYLLA_PRIV}:9042"
echo ">>> Track C deploy bench=$BENCH_HOST contact=$CONTACT"

ssh_wait_ready "$BENCH_HOST"
ssh_cmd "$BENCH_HOST" "mkdir -p ~/boson-bench/reports"
ssh_cmd "$BENCH_HOST" "bash -c 'killall -9 boson-bench 2>/dev/null || true; sleep 1'"
scp_to "$BENCH_HOST" "$BINARY" "~/boson-bench/boson-bench.new"
ssh_cmd "$BENCH_HOST" "mv -f ~/boson-bench/boson-bench.new ~/boson-bench/boson-bench && chmod +x ~/boson-bench/boson-bench"

ssh_cmd_stdin "$BENCH_HOST" "cat > ~/boson-bench/run-track-c.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export BOSON_BENCH_HARDWARE=$HARDWARE
export BOSON_TEST_SCYLLA_CONTACT_POINTS=$CONTACT
export BOSON_BENCH_STORAGE_TOPOLOGY=scylla-1
export BOSON_SCYLLA_SHARD_CONCURRENCY=32
export BOSON_SCYLLA_PARALLEL_WRITES=1
export BOSON_SCYLLA_POOL_PER_SHARD=1
cd ~/boson-bench
BENCH=./boson-bench
REPORTS=~/boson-bench/reports
mkdir -p "\$REPORTS"
rm -f campaign.done campaign.failed
trap 'touch campaign.failed' ERR

run_one() {
  local id="\$1"
  local suffix="\$2"
  echo "=== Track C \$id -> \$suffix ==="
  "\$BENCH" run --experiment "\$id" --backend scylla --topology isolated-lab \\
    --telemetry off --hardware $HARDWARE --warmup 0 \\
    --report "\$REPORTS/\${suffix}.json"
}

run_one bm-b0 bm-b0-scylla-isolated-lab-off-$HARDWARE-track-c
run_one bm-bl1 bm-bl1-scylla-isolated-lab-off-$HARDWARE-track-c
run_one bm-bl2 bm-bl2-scylla-isolated-lab-off-$HARDWARE-track-c
run_one bm-bl3 bm-bl3-scylla-isolated-lab-off-$HARDWARE-track-c

echo "=== BM-C3 hot pool shards=1 ==="
BOSON_BENCH_READY_SHARD_COUNT=1 run_one bm-bm3 bm-bm3-scylla-isolated-lab-off-$HARDWARE-track-c

echo "=== BM-C4 spread shards=256 ==="
BOSON_BENCH_READY_SHARD_COUNT=256 run_one bm-bm4 bm-bm4-scylla-isolated-lab-off-$HARDWARE-track-c

echo "=== BM-C5 BI1 lwt ==="
BOSON_BENCH_IDEMPOTENCY_MODE=lwt run_one bm-bi1 bm-bi1-scylla-isolated-lab-off-$HARDWARE-track-c-lwt

echo "=== BM-C5 BI1 none ==="
BOSON_BENCH_IDEMPOTENCY_MODE=none run_one bm-bi1 bm-bi1-scylla-isolated-lab-off-$HARDWARE-track-c-none

echo "DONE reports=\$(ls -1 \$REPORTS/*track-c* 2>/dev/null | wc -l)"
ls -1 "\$REPORTS"/*track-c* 2>/dev/null || true
touch campaign.done
EOF

ssh_cmd "$BENCH_HOST" "chmod +x ~/boson-bench/run-track-c.sh"
ssh_cmd_stdin "$BENCH_HOST" "cat > ~/boson-bench/start-campaign.sh" <<'START'
#!/usr/bin/env bash
set -euo pipefail
cd ~/boson-bench
if [[ -f campaign.pid ]]; then kill -9 "$(cat campaign.pid)" 2>/dev/null || true; fi
killall -9 boson-bench 2>/dev/null || true
sleep 1
rm -f campaign.done campaign.failed campaign.log
# Keep prior baseline reports; only remove track-c outputs
rm -f reports/*track-c*.json
nohup ./run-track-c.sh > campaign.log 2>&1 &
echo $! > campaign.pid
echo "started pid=$(cat campaign.pid)"
START
ssh_cmd "$BENCH_HOST" "chmod +x ~/boson-bench/start-campaign.sh && ~/boson-bench/start-campaign.sh"

echo "Track C started; polling..."
DEADLINE=$((SECONDS + 3600))
while (( SECONDS < DEADLINE )); do
  state="$(ssh_cmd "$BENCH_HOST" "if test -f ~/boson-bench/campaign.done; then echo done; elif test -f ~/boson-bench/campaign.failed; then echo failed; elif test -f ~/boson-bench/campaign.pid && kill -0 \$(cat ~/boson-bench/campaign.pid) 2>/dev/null; then echo running; else echo dead; fi" 2>/dev/null || echo unknown)"
  echo "  state=$state"
  case "$state" in
    done)
      scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-track-c.log" || true
      echo "Track C complete"
      exit 0
      ;;
    failed|dead)
      scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-track-c.log" || true
      echo "Track C failed (state=$state)" >&2
      exit 1
      ;;
  esac
  scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-track-c.log" 2>/dev/null || true
  ssh_cmd "$SCYLLA_PUB" "free -m | head -2; sudo docker stats --no-stream boson-scylla0 2>/dev/null || true" \
    > "$LOG_DIR/scylla-resources-track-c.log" 2>/dev/null || true
  sleep 20
done
echo "timeout" >&2
exit 1
