#!/usr/bin/env bash
# Mirror PR CI jobs that do not require live broker containers.
# Full postgres/redis/nats matrix runs on GitHub Actions service containers
# or via run-redis-e2e.sh / run-nats-e2e.sh against a provisioned fleet.
#
# Usage: ./run-remote-ci.sh [manifest-name]
# Mirrors the PR CI subset on a provisioned native-aws bench host.
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
REMOTE_DIR="${BOSON_REMOTE_CI_DIR:-/tmp/boson-remote-ci}"

echo ">>> rsync repo to bench $BENCH_HOST:$REMOTE_DIR"
ssh_wait_ready "$BENCH_HOST"
ssh_cmd "$BENCH_HOST" "mkdir -p $REMOTE_DIR"
rsync -az --delete \
  --exclude target --exclude 'target-*' --exclude '.git' \
  --exclude profiling --exclude 'infra/native-aws/state' \
  -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $BOSON_NATIVE_AWS_KEY_PATH" \
  "$REPO_ROOT/" "ec2-user@${BENCH_HOST}:${REMOTE_DIR}/"

echo ">>> run PR CI subset on $BENCH_HOST"
ssh_cmd_stdin "$BENCH_HOST" "bash -s" <<REMOTE
set -euo pipefail
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
fi
# shellcheck disable=SC1091
source "\$HOME/.cargo/env" 2>/dev/null || true
rustup component add clippy rustfmt 2>/dev/null || true

# Fresh AL2023 AMIs lack a C toolchain and often git (needed for cargo git deps).
if ! command -v cc >/dev/null 2>&1 || ! command -v git >/dev/null 2>&1; then
  sudo dnf install -y gcc git openssl-devel pkgconf-pkg-config
fi

export CARGO_TARGET_DIR=/tmp/boson-remote-ci-target
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS="\${CARGO_BUILD_JOBS:-\$(nproc)}"
export RUST_BACKTRACE=1
export CARGO_PROFILE_TEST_DEBUG="\${CARGO_PROFILE_TEST_DEBUG:-0}"
export CARGO_PROFILE_DEV_DEBUG="\${CARGO_PROFILE_DEV_DEBUG:-0}"
export RUSTFLAGS="\${RUSTFLAGS:-} -C link-arg=-fuse-ld=bfd"
cd "$REMOTE_DIR"

rm -rf "\$CARGO_TARGET_DIR"
mkdir -p "\$CARGO_TARGET_DIR"

echo "=== check (boson facade) ==="
cargo check -p uf-boson --features mem

echo "=== deny ==="
if ! command -v cargo-deny >/dev/null 2>&1; then
  cargo install cargo-deny --locked
fi
cargo deny check

echo "=== clippy (workspace) ==="
cargo clippy --workspace --all-targets -- -D warnings

echo "=== testkit ==="
cargo test -p boson-testkit

echo "=== backend-mem ==="
cargo test -p boson-backend-mem

echo "=== backend-sqlite ==="
cargo test -p boson-backend-sqlite

echo "=== backend-sql-common ==="
cargo test -p boson-backend-sql-common

echo "=== core ==="
cargo test -p boson-core

echo "=== runtime + macros ==="
cargo test -p boson-runtime
cargo test -p boson-macros

echo "=== telemetry ==="
cargo test -p boson-telemetry

echo "=== e2e smoke (mem/sqlite active) ==="
cargo test -p boson-e2e -- --test-threads=1

echo "=== axum ==="
cargo test -p boson-axum

echo "=== bench-smoke BM-B0/BM-B1 ==="
cargo run -p boson-bench -- experiments
cargo run -p boson-bench -- run --experiment bm-b0 --backend mem --topology isolated-lab --telemetry off --ops 1000
cargo run -p boson-bench -- run --experiment bm-b1 --backend mem --topology isolated-lab --telemetry off

echo "=== examples ==="
cargo run -p uf-boson --example minimal_enqueue --features mem
cargo run -p uf-boson --example task_macro --features mem
cargo run -p uf-boson --example idempotency_and_rate_limit --features mem
cargo run -p uf-boson --example axum_admin --features mem,axum

echo "=== docs ==="
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --doc -p boson-core
cargo test --doc -p boson-runtime
cargo test --doc -p boson-backend-mem
cargo test --doc -p uf-boson --features mem
cargo test --doc -p boson-telemetry

echo "Remote CI subset passed (broker live jobs excluded)."
echo "For postgres/redis/nats: use GitHub Actions or run-redis-e2e.sh / run-nats-e2e.sh."
REMOTE

echo "Remote CI subset passed on $BENCH_HOST"
