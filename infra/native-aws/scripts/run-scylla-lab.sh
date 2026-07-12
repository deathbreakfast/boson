#!/usr/bin/env bash
# Deploy boson-bench and run single-node Scylla lab experiments (Track A/M/P/I/F).
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
echo ">>> deploy to bench=$BENCH_HOST contact=$CONTACT"

ssh_wait_ready "$BENCH_HOST"
ssh_cmd "$BENCH_HOST" "mkdir -p ~/boson-bench/reports"
ssh_cmd "$BENCH_HOST" "bash -c 'killall -9 boson-bench 2>/dev/null || true; sleep 1'"
scp_to "$BENCH_HOST" "$BINARY" "~/boson-bench/boson-bench.new"
ssh_cmd "$BENCH_HOST" "mv -f ~/boson-bench/boson-bench.new ~/boson-bench/boson-bench && chmod +x ~/boson-bench/boson-bench"

# Remote campaign script
ssh_cmd_stdin "$BENCH_HOST" "cat > ~/boson-bench/run-campaign.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export BOSON_BENCH_HARDWARE=$HARDWARE
export BOSON_TEST_SCYLLA_CONTACT_POINTS=$CONTACT
export BOSON_BENCH_STORAGE_TOPOLOGY=scylla-1
cd ~/boson-bench
BENCH=./boson-bench
REPORTS=~/boson-bench/reports
mkdir -p "\$REPORTS"
rm -f campaign.done campaign.failed
trap 'touch campaign.failed' ERR

run_one() {
  local id="\$1"
  echo "=== \$id (shards=\${BOSON_BENCH_READY_SHARD_COUNT:-256} idem=\${BOSON_BENCH_IDEMPOTENCY_MODE:-default} tasks=\${BOSON_BENCH_TASK_COUNT:-}) ==="
  "\$BENCH" run --experiment "\$id" --backend scylla --topology isolated-lab \\
    --telemetry off --hardware $HARDWARE --warmup 0 \\
    --report "\$REPORTS/\${id}-scylla-isolated-lab-off-$HARDWARE.json"
}

echo "=== scylla-lab subset ==="
"\$BENCH" matrix --subset scylla-lab --backend scylla --topology isolated-lab \\
  --telemetry off --hardware $HARDWARE --warmup 0 --reports-dir "\$REPORTS"

echo "=== Track M BM-BM3 ready_shard_count=1 ==="
BOSON_BENCH_READY_SHARD_COUNT=1 run_one bm-bm3

echo "=== Track P BM-BM4 default shards=256 ==="
BOSON_BENCH_READY_SHARD_COUNT=256 run_one bm-bm4

# Optional shard sweep (light levers)
for shards in 64 1024; do
  echo "=== Track P BM-BM4 ready_shard_count=\$shards ==="
  BOSON_BENCH_READY_SHARD_COUNT=\$shards \\
    "\$BENCH" run --experiment bm-bm4 --backend scylla --topology isolated-lab \\
      --telemetry off --hardware $HARDWARE --warmup 0 \\
      --report "\$REPORTS/bm-bm4-scylla-isolated-lab-off-$HARDWARE-shards\${shards}.json"
done

echo "=== Track I BM-BI1 lwt ==="
BOSON_BENCH_IDEMPOTENCY_MODE=lwt run_one bm-bi1
# overwrite report with mode suffix
mv -f "\$REPORTS/bm-bi1-scylla-isolated-lab-off-$HARDWARE.json" \\
  "\$REPORTS/bm-bi1-scylla-isolated-lab-off-$HARDWARE-lwt.json" 2>/dev/null || true

echo "=== Track I BM-BI1 none ==="
BOSON_BENCH_IDEMPOTENCY_MODE=none run_one bm-bi1
mv -f "\$REPORTS/bm-bi1-scylla-isolated-lab-off-$HARDWARE.json" \\
  "\$REPORTS/bm-bi1-scylla-isolated-lab-off-$HARDWARE-none.json" 2>/dev/null || true

echo "=== Track F BM-BF2 T=1 ==="
BOSON_BENCH_TASK_COUNT=1 BOSON_BENCH_IDEMPOTENCY_MODE=lwt run_one bm-bf2
mv -f "\$REPORTS/bm-bf2-scylla-isolated-lab-off-$HARDWARE.json" \\
  "\$REPORTS/bm-bf2-scylla-isolated-lab-off-$HARDWARE-t1.json" 2>/dev/null || true

echo "=== Track F BM-BF2 T=64 ==="
BOSON_BENCH_TASK_COUNT=64 BOSON_BENCH_IDEMPOTENCY_MODE=lwt run_one bm-bf2
mv -f "\$REPORTS/bm-bf2-scylla-isolated-lab-off-$HARDWARE.json" \\
  "\$REPORTS/bm-bf2-scylla-isolated-lab-off-$HARDWARE-t64.json" 2>/dev/null || true

echo "DONE reports=\$(ls -1 \$REPORTS | wc -l)"
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

echo "campaign started; polling..."
DEADLINE=$((SECONDS + 3600))
while (( SECONDS < DEADLINE )); do
  state="$(ssh_cmd "$BENCH_HOST" "if test -f ~/boson-bench/campaign.done; then echo done; elif test -f ~/boson-bench/campaign.failed; then echo failed; elif test -f ~/boson-bench/campaign.pid && kill -0 \$(cat ~/boson-bench/campaign.pid) 2>/dev/null; then echo running; else echo dead; fi" 2>/dev/null || echo unknown)"
  echo "  state=$state"
  case "$state" in
    done)
      scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-scylla-lab.log" || true
      echo "campaign complete"
      exit 0
      ;;
    failed|dead)
      scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-scylla-lab.log" || true
      echo "campaign failed (state=$state)" >&2
      exit 1
      ;;
  esac
  scp_from "$BENCH_HOST" "~/boson-bench/campaign.log" "$LOG_DIR/run-scylla-lab.log" 2>/dev/null || true
  # Sample scylla host resources periodically
  ssh_cmd "$SCYLLA_PUB" "free -m | head -2; sudo docker stats --no-stream boson-scylla0 2>/dev/null || true" \
    > "$LOG_DIR/scylla-resources.log" 2>/dev/null || true
  sleep 30
done
echo "timeout" >&2
exit 1
