#!/usr/bin/env bash
# Line coverage for the Boson workspace (PR slice excludes e2e/bench).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target-boson-cov}"

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov not found — install with: cargo install cargo-llvm-cov"
  exit 1
fi

SCOPE=(
  --workspace
  --exclude boson-e2e
  --exclude boson-bench
  --features mem
)

if [[ "${1:-}" == "--full" ]]; then
  SCOPE=(--workspace --features mem)
fi

cargo llvm-cov "${SCOPE[@]}" --summary-only

if [[ "${1:-}" == "--lcov" || "${1:-}" == "--full" ]]; then
  cargo llvm-cov "${SCOPE[@]}" --lcov --output-path lcov.info
  echo "Wrote lcov.info"
fi
