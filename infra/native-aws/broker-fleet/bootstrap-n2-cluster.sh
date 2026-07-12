#!/usr/bin/env bash
# Bootstrap 2-node NATS JetStream RAFT cluster (Photon-style, AL2023).
# Usage: ./bootstrap-n2-cluster.sh [manifest-name]
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

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-nats-cluster-2}}"
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

N0_PRIV="$(priv_for nats-0)"
N1_PRIV="$(priv_for nats-1)"
N0_PUB="$(host_for nats-0)"
N1_PUB="$(host_for nats-1)"

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

bootstrap_node "$N0_PUB" "$N0_PRIV" "nats-route://${N1_PRIV}:6222" "nats-0"
bootstrap_node "$N1_PUB" "$N1_PRIV" "nats-route://${N0_PRIV}:6222" "nats-1"

echo "NATS RAFT n=2 cluster ready"
echo "export BOSON_NATS_URLS='nats://${N0_PRIV}:4222,nats://${N1_PRIV}:4222'"
echo "export BOSON_NATS_CLUSTER_NODES=2"
