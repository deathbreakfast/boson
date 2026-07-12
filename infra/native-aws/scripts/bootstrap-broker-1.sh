#!/usr/bin/env bash
# Install docker on bench + broker; start Redis or NATS on the broker host.
# Usage: BOSON_BROKER=redis|nats ./bootstrap-broker-1.sh [manifest-name]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/ssh.sh"

BROKER="${BOSON_BROKER:?set BOSON_BROKER to redis or nats}"
MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-${BROKER}-1}}"
MANIFEST="$(manifest_read "$MANIFEST_NAME")"

host_for() {
  local role="$1"
  echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == sys.argv[1]))
" "$role"
}

priv_for() {
  local role="$1"
  echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['private_ip'] for i in m['instances'] if i['role'] == sys.argv[1]))
" "$role"
}

BENCH_HOST="$(host_for bench)"
BROKER_HOST="$(host_for "$BROKER")"
BROKER_PRIV="$(priv_for "$BROKER")"

bootstrap_docker() {
  local host="$1"
  ssh_wait_ready "$host"
  ssh_cmd_stdin "$host" "bash -s" <<'EOF'
set -euo pipefail
if ! command -v docker >/dev/null 2>&1; then
  sudo dnf install -y docker
  sudo systemctl enable --now docker
  sudo usermod -aG docker ec2-user || true
fi
EOF
}

echo ">>> docker on bench ($BENCH_HOST)"
bootstrap_docker "$BENCH_HOST"
echo ">>> docker on ${BROKER} ($BROKER_HOST)"
bootstrap_docker "$BROKER_HOST"

case "$BROKER" in
  redis)
    scp_to "$BROKER_HOST" "$ROOT/templates/run-redis.sh" "~/run-redis.sh"
    ssh_cmd "$BROKER_HOST" "chmod +x ~/run-redis.sh"
    ssh_cmd "$BROKER_HOST" "bash -lc 'export REDIS_IP=$BROKER_PRIV; ~/run-redis.sh'"
    echo "waiting for Redis on $BROKER_PRIV:6379 ..."
    for _ in $(seq 1 30); do
      if ssh_cmd "$BROKER_HOST" "sudo docker exec boson-redis redis-cli ping" 2>/dev/null | grep -q PONG; then
        echo "Redis ready"
        exit 0
      fi
      sleep 3
    done
    ;;
  nats)
    scp_to "$BROKER_HOST" "$ROOT/templates/run-nats.sh" "~/run-nats.sh"
    ssh_cmd "$BROKER_HOST" "chmod +x ~/run-nats.sh"
    ssh_cmd "$BROKER_HOST" "bash -lc 'export NATS_IP=$BROKER_PRIV; ~/run-nats.sh'"
    echo "waiting for NATS on $BROKER_PRIV:4222 ..."
    for _ in $(seq 1 30); do
      if ssh_cmd "$BROKER_HOST" "sudo docker logs boson-nats 2>&1 | tail -5" | grep -qi listening; then
        echo "NATS ready"
        exit 0
      fi
      sleep 3
    done
    ;;
esac
echo "${BROKER} did not become ready" >&2
exit 1
