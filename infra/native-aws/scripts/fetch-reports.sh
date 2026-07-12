#!/usr/bin/env bash
# SCP reports from fleet hosts into profiling/boson-bench/reports/.
# Usage: fetch-reports.sh [manifest-name]
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

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-baseline-t3m}}"
DEST="${BOSON_BENCH_REPORTS_DIR:-$REPO_ROOT/profiling/boson-bench/reports}"
mkdir -p "$DEST"

MANIFEST="$(manifest_read "$MANIFEST_NAME")"
COUNT_BEFORE="$(find "$DEST" -maxdepth 1 -name '*.json' 2>/dev/null | wc -l)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

while IFS= read -r line; do
  backend="$(echo "$line" | cut -d' ' -f1)"
  host="$(echo "$line" | cut -d' ' -f2)"
  echo "Fetching reports from $backend ($host)"
  ssh_wait_ready "$host"
  mkdir -p "$TMP/$backend"
  # Tar on remote and stream locally (handles globs cleanly).
  if ssh_cmd "$host" "bash -lc 'cd ~/boson-bench/reports && tar czf - ./*.json 2>/dev/null'" \
      > "$TMP/$backend/reports.tgz" 2>/dev/null; then
    if [[ -s "$TMP/$backend/reports.tgz" ]]; then
      tar xzf "$TMP/$backend/reports.tgz" -C "$DEST"
      echo "  extracted $(tar tzf "$TMP/$backend/reports.tgz" | wc -l) files from $backend"
    fi
  else
    echo "  warning: no reports on $backend" >&2
  fi
done < <(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
for i in m['instances']:
    if i.get('role') == 'bench':
        print(i.get('backend', 'bench'), i['public_ip'])
")

COUNT_AFTER="$(find "$DEST" -maxdepth 1 -name '*.json' 2>/dev/null | wc -l)"
echo "Reports in $DEST (before=$COUNT_BEFORE after=$COUNT_AFTER)"
if [[ "$COUNT_AFTER" -le "$COUNT_BEFORE" ]]; then
  echo "fetch-reports: no new reports" >&2
  exit 1
fi
