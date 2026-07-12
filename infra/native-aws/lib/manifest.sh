#!/usr/bin/env bash
# Manifest read/write for native-aws fleets.
set -euo pipefail

manifest_dir() {
  local root="${BOSON_NATIVE_AWS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
  echo "$root/manifests"
}

manifest_path() {
  local name="$1"
  echo "$(manifest_dir)/${name}.json"
}

manifest_write() {
  local name="$1"
  local json="$2"
  mkdir -p "$(manifest_dir)"
  printf '%s\n' "$json" > "$(manifest_path "$name")"
}

manifest_read() {
  local name="$1"
  local path
  path="$(manifest_path "$name")"
  if [[ ! -f "$path" ]]; then
    echo "manifest not found: $path (run provision first)" >&2
    return 1
  fi
  cat "$path"
}
