#!/usr/bin/env bash
# Deploy boson-bench binary to all bench hosts in a manifest.
# Usage: ./deploy-all-benches.sh [manifest-name]
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
# shellcheck disable=SC1091
source "$ROOT/lib/bench-fleet.sh"

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-nats-multibench-4}}"
MANIFEST="$(manifest_read "$MANIFEST_NAME")"

echo ">>> cross-build boson-bench (one-shot docker)"
"${BOSON_AWS_ADAPTER:-$HOME/aws/boson}/build-al2023-local.sh"

BINARY="$(artifact_local_path)"

while IFS=$'\t' read -r role host; do
  [[ -n "$role" ]] || continue
  echo ">>> deploy binary to $role ($host)"
  ssh_wait_ready "$host"
  ssh_cmd "$host" "mkdir -p ~/boson-bench && rm -rf ~/boson-bench/reports"
  scp_to "$host" "$BINARY" "~/boson-bench/boson-bench.new"
  ssh_cmd "$host" "mv -f ~/boson-bench/boson-bench.new ~/boson-bench/boson-bench && chmod +x ~/boson-bench/boson-bench"
  EXP_LIST="$(ssh_cmd "$host" "~/boson-bench/boson-bench experiments 2>/dev/null" || true)"
  for req in bm-be1 bm-bd1; do
    if ! grep -q "$req" <<< "$EXP_LIST"; then
      echo "deploy gate failed: boson-bench on $host missing $req" >&2
      exit 1
    fi
  done
  echo "deploy gate ok on $host"
done < <(bench_hosts_from_manifest "$MANIFEST")

echo "deployed $(artifact_key_slug) to all bench hosts"
