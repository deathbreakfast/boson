#!/usr/bin/env bash
# Local artifact helpers (S3 unavailable for this IAM principal).
set -euo pipefail

artifact_repo_root() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd
}

artifact_git_sha() {
  git -C "$(artifact_repo_root)" rev-parse HEAD 2>/dev/null || echo "unknown"
}

artifact_lock_hash() {
  sha256sum "$(artifact_repo_root)/Cargo.lock" | awk '{print substr($1,1,8)}'
}

artifact_local_dir() {
  local root
  root="$(artifact_repo_root)"
  if [[ -n "${BOSON_NATIVE_ARTIFACT_DIR:-}" ]]; then
    echo "$BOSON_NATIVE_ARTIFACT_DIR"
  else
    echo "${root}/target/al2023"
  fi
}

artifact_local_path() {
  echo "$(artifact_local_dir)/boson-bench"
}

artifact_key_slug() {
  echo "$(artifact_git_sha)-$(artifact_lock_hash)"
}
