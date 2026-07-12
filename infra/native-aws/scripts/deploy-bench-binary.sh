#!/usr/bin/env bash
# Cross-build boson-bench for AL2023 (one-shot container; no local services left running).
# Usage: ./deploy-bench-binary.sh [manifest-name]
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

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-redis-1}}"
MANIFEST="$(manifest_read "$MANIFEST_NAME")"
BENCH_HOST="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == 'bench'))
")"

echo ">>> cross-build boson-bench (one-shot docker)"
"$ROOT/scripts/build-al2023-local.sh"

BINARY="$(artifact_local_path)"
echo ">>> deploy binary to bench $BENCH_HOST"
ssh_wait_ready "$BENCH_HOST"
ssh_cmd "$BENCH_HOST" "mkdir -p ~/boson-bench && rm -rf ~/boson-src"
scp_to "$BENCH_HOST" "$BINARY" "~/boson-bench/boson-bench.new"
ssh_cmd "$BENCH_HOST" "mv -f ~/boson-bench/boson-bench.new ~/boson-bench/boson-bench && chmod +x ~/boson-bench/boson-bench"
EXP_LIST="$(ssh_cmd "$BENCH_HOST" "~/boson-bench/boson-bench experiments")"
for req in bm-be1 bm-bd1; do
  if ! grep -q "$req" <<< "$EXP_LIST"; then
    echo "deploy gate failed: binary missing $req" >&2
    exit 1
  fi
done
echo "deploy gate ok: bm-be1 bm-bd1 registered"
echo "deployed $(artifact_key_slug) to $BENCH_HOST"
