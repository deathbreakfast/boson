#!/usr/bin/env bash
# Bootstrap Docker + standalone brokers (NATS or Redis) on each broker host in a fleet manifest.
# Usage: ./bootstrap-fleet.sh [manifest-name]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BF="$(cd "$(dirname "$0")" && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/ssh.sh"

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-nats-fleet-4}}"
MANIFEST="$(manifest_read "$MANIFEST_NAME")"
BROKER="$(echo "$MANIFEST" | python3 -c "import json,sys; print(json.load(sys.stdin).get('broker','nats'))")"

host_for() {
  echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == sys.argv[1]))
" "$1"
}

priv_for() {
  echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['private_ip'] for i in m['instances'] if i['role'] == sys.argv[1]))
" "$1"
}

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

BENCH_HOSTS="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
for i in sorted(m['instances'], key=lambda x: x['role']):
    if i['role'] == 'bench' or i['role'].startswith('bench-'):
        print(i['public_ip'])
")"

while IFS= read -r host; do
  [[ -z "$host" ]] && continue
  echo ">>> docker on bench ($host)"
  bootstrap_docker "$host"
done <<< "$BENCH_HOSTS"

BROKER_ROLES="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
broker = m.get('broker', 'nats')
for i in sorted(m['instances'], key=lambda x: x['role']):
    if i['role'].startswith(f'{broker}-'):
        print(i['role'])
")"

while IFS= read -r role; do
  [[ -z "$role" ]] && continue
  host="$(host_for "$role")"
  priv="$(priv_for "$role")"
  bootstrap_docker "$host"
  if [[ "$BROKER" == "redis" ]]; then
    echo ">>> docker + Redis on $role ($host $priv)"
    scp_to "$host" "$ROOT/templates/run-redis.sh" "~/run-redis.sh"
    ssh_cmd "$host" "chmod +x ~/run-redis.sh"
    ssh_cmd "$host" "bash -lc 'export REDIS_IP=$priv; ~/run-redis.sh'"
    for _ in $(seq 1 30); do
      if ssh_cmd "$host" "sudo docker logs boson-redis 2>&1 | tail -5" | grep -qi 'Ready to accept connections'; then
        echo "Redis ready on $role"
        break
      fi
      sleep 3
    done
  else
    echo ">>> docker + NATS on $role ($host $priv)"
    scp_to "$host" "$ROOT/templates/run-nats.sh" "~/run-nats.sh"
    ssh_cmd "$host" "chmod +x ~/run-nats.sh"
    ssh_cmd "$host" "bash -lc 'export NATS_IP=$priv; ~/run-nats.sh'"
    for _ in $(seq 1 30); do
      if ssh_cmd "$host" "sudo docker logs boson-nats 2>&1 | tail -5" | grep -qi listening; then
        echo "NATS ready on $role"
        break
      fi
      sleep 3
    done
  fi
done <<< "$BROKER_ROLES"

echo "fleet bootstrap complete (broker=$BROKER)"
