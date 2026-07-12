#!/usr/bin/env bash
# Install docker on both hosts; start Scylla on the storage host.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/ssh.sh"

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-scylla-1}}"
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
SCYLLA_HOST="$(host_for scylla)"
SCYLLA_PRIV="$(priv_for scylla)"

bootstrap_docker() {
  local host="$1"
  ssh_wait_ready "$host"
  ssh_cmd_stdin "$host" "bash -s" <<'EOF'
set -euo pipefail
if ! command -v docker >/dev/null 2>&1; then
  sudo dnf install -y docker
  sudo systemctl enable --now docker
fi
EOF
}

echo ">>> docker on bench ($BENCH_HOST)"
bootstrap_docker "$BENCH_HOST"
echo ">>> docker on scylla ($SCYLLA_HOST)"
bootstrap_docker "$SCYLLA_HOST"

# Upload and run Scylla template
scp_to "$SCYLLA_HOST" "$ROOT/templates/run-scylla.sh" "~/run-scylla.sh"
ssh_cmd "$SCYLLA_HOST" "chmod +x ~/run-scylla.sh"
ssh_cmd "$SCYLLA_HOST" "bash -lc '
  export SCYLLA_IP=$SCYLLA_PRIV
  export SEED_IP=$SCYLLA_PRIV
  export SCYLLA_INDEX=0
  export SCYLLA_IMAGE=$BOSON_NATIVE_SCYLLA_IMAGE
  ~/run-scylla.sh
'"

echo "waiting for Scylla CQL on $SCYLLA_PRIV:9042 ..."
for _ in $(seq 1 60); do
  if ssh_cmd "$SCYLLA_HOST" "sudo docker exec boson-scylla0 cqlsh $SCYLLA_PRIV -e 'SELECT now() FROM system.local'" >/dev/null 2>&1; then
    echo "Scylla ready"
    exit 0
  fi
  sleep 5
done
echo "Scylla did not become ready" >&2
exit 1
