#!/usr/bin/env bash
# Sync workspace to bench EC2 and build boson-bench there (no local Docker).
# Usage: ./build-on-ec2.sh [manifest-name]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/ssh.sh"

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-redis-1}}"
MANIFEST="$(manifest_read "$MANIFEST_NAME")"
BENCH_HOST="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == 'bench'))
")"

echo ">>> rsync repo to bench $BENCH_HOST"
ssh_wait_ready "$BENCH_HOST"
ssh_cmd "$BENCH_HOST" "mkdir -p ~/boson-src"
rsync -az --delete \
  --exclude target --exclude '.git' --exclude profiling --exclude 'infra/native-aws/state' \
  -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $BOSON_NATIVE_AWS_KEY_PATH" \
  "$REPO_ROOT/" "ec2-user@${BENCH_HOST}:~/boson-src/"

echo ">>> install rust + build on bench"
ssh_cmd_stdin "$BENCH_HOST" "bash -s" <<'EOF'
set -euo pipefail
cd ~/boson-src
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env" 2>/dev/null || true
export CARGO_BUILD_JOBS="$(nproc)"
cargo build --release -p boson-bench -j "$CARGO_BUILD_JOBS"
mkdir -p ~/boson-bench
cp -f target/release/boson-bench ~/boson-bench/boson-bench
chmod +x ~/boson-bench/boson-bench
~/boson-bench/boson-bench experiments >/dev/null
echo "built $(~/boson-bench/boson-bench experiments 2>&1 | head -1 || true)"
EOF

echo "build complete on $BENCH_HOST"
