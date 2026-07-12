#!/usr/bin/env bash
# Cross-build boson-bench for Amazon Linux 2023 via local docker.
# Used when EC2 builder instance types exceed account vCPU limits.
# Mounts host cargo registry caches for crates.io dependencies.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/artifact.sh"

OUT="$(artifact_local_path)"
mkdir -p "$(dirname "$OUT")"

echo "building boson-bench in amazonlinux:2023 docker -> $OUT"
docker run --rm \
  -v "${REPO_ROOT}:/work:rw" \
  -v "${HOME}/.cargo/registry:/cargo/registry:rw" \
  -v "${HOME}/.cargo/git:/cargo/git:rw" \
  -w /work \
  amazonlinux:2023 \
  bash -lc '
    set -euo pipefail
    dnf install -y gcc openssl-devel pkgconfig git clang which >/tmp/dnf.log 2>&1
    export RUSTUP_HOME=/opt/rustup
    export CARGO_HOME=/opt/cargo
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --no-modify-path
    rm -rf /opt/cargo/registry /opt/cargo/git
    ln -s /cargo/registry /opt/cargo/registry
    ln -s /cargo/git /opt/cargo/git
    export PATH="/opt/cargo/bin:$PATH"
    export CARGO_BUILD_JOBS="$(nproc)"
    export CARGO_TARGET_DIR=/work/target-fleet-build
    echo "CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS"
    cargo build --release -p boson-bench -j "$CARGO_BUILD_JOBS"
    test -x "$CARGO_TARGET_DIR/release/boson-bench"
    cp "$CARGO_TARGET_DIR/release/boson-bench" /work/target/release/boson-bench
  '

cp "$REPO_ROOT/target/release/boson-bench" "$OUT"
chmod +x "$OUT"
test -x "$OUT"
echo "Built $OUT ($(artifact_key_slug))"
