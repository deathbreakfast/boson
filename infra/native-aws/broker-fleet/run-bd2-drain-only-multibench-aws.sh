#!/usr/bin/env bash
# Phase G2 D10: drain-only multibench (central prefill on client 0).
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"

export BOSON_BENCH_DRAIN_ONLY=1
export BOSON_BD2_MULTIBENCH_BC="${BOSON_BD2_MULTIBENCH_BC:-2 4}"
export BOSON_BD2_WORKER_COUNT="${BOSON_BD2_WORKER_COUNT:-16}"
export BOSON_NATIVE_CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-bd2-g2-$(date -u +%Y%m%d)}"

exec "$BF/run-bd2-multibench-sweep-aws.sh"
