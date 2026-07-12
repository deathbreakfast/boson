#!/usr/bin/env bash
# Bootstrap 4-node NATS JetStream RAFT cluster (Photon-style, AL2023).
# Usage: ./bootstrap-n4-cluster.sh [manifest-name]
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/ssh.sh"

if [[ ! -f "$BOSON_NATIVE_AWS_KEY_PATH" ]]; then
  echo "SSH key not found: $BOSON_NATIVE_AWS_KEY_PATH" >&2
  exit 1
fi

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-nats-cluster-4}}"
MANIFEST="$(manifest_read "$MANIFEST_NAME")"

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

NODES=(nats-0 nats-1 nats-2 nats-3)
PRIVS=()
PUBS=()
for role in "${NODES[@]}"; do
  PRIVS+=("$(priv_for "$role")")
  PUBS+=("$(host_for "$role")")
done

bootstrap_node() {
  local host="$1"
  local priv="$2"
  local routes="$3"
  local name="$4"
  ssh_wait_ready "$host"
  ssh_cmd_stdin "$host" "bash -s" <<EOF
set -euo pipefail
if ! command -v docker >/dev/null 2>&1; then
  sudo dnf install -y docker && sudo systemctl enable --now docker
fi
sudo docker rm -f boson-nats 2>/dev/null || true
sudo docker pull nats:2.10-alpine
cat > /tmp/nats-cluster.conf <<CONF
listen: 0.0.0.0:4222
http: 8222
server_name: ${name}
jetstream { store_dir: /data/jetstream }
cluster {
  name: boson-js
  listen: 0.0.0.0:6222
  advertise: ${priv}:6222
  routes: [ ${routes} ]
}
CONF
sudo docker run -d --name boson-nats --restart unless-stopped --network host -v /tmp/nats-cluster.conf:/etc/nats/nats.conf \
  nats:2.10-alpine -c /etc/nats/nats.conf
EOF
}

wait_client_port() {
  local host="$1"
  local name="$2"
  local i
  for ((i = 1; i <= 30; i++)); do
    if ssh_cmd "$host" "curl -sf http://127.0.0.1:8222/varz >/dev/null 2>&1"; then
      echo "NATS listening on $name ($host)"
      return 0
    fi
    sleep 2
  done
  echo "NATS varz not up on $name ($host)" >&2
  return 1
}

wait_cluster_js() {
  local host="$1"
  local i
  for ((i = 1; i <= 60; i++)); do
    if ssh_cmd "$host" "curl -sf http://127.0.0.1:8222/jsz?acc=1 2>/dev/null | grep -q meta_leader"; then
      echo "JetStream meta leader elected on $host"
      return 0
    fi
    if ssh_cmd "$host" "curl -sf http://127.0.0.1:8222/healthz?js-server-only=1 >/dev/null 2>&1"; then
      echo "JetStream cluster healthy on $host"
      return 0
    fi
    sleep 5
  done
  echo "JetStream cluster not ready on $host after 300s" >&2
  ssh_cmd "$host" "sudo docker logs boson-nats 2>&1 | tail -40" >&2 || true
  return 1
}

for idx in "${!NODES[@]}"; do
  routes=""
  for j in "${!PRIVS[@]}"; do
    [[ "$j" -eq "$idx" ]] && continue
    [[ -n "$routes" ]] && routes+=", "
    routes+="nats-route://${PRIVS[$j]}:6222"
  done
  bootstrap_node "${PUBS[$idx]}" "${PRIVS[$idx]}" "$routes" "${NODES[$idx]}"
done

for idx in "${!NODES[@]}"; do
  wait_client_port "${PUBS[$idx]}" "${NODES[$idx]}"
done

echo "Waiting for JetStream RAFT meta leader..."
wait_cluster_js "${PUBS[0]}"

URLS=""
for priv in "${PRIVS[@]}"; do
  URLS+="nats://${priv}:4222,"
done
URLS="${URLS%,}"

echo "NATS RAFT n=4 cluster ready"
echo "export BOSON_NATS_URLS='${URLS}'"
echo "export BOSON_NATS_CLUSTER_NODES=4"
