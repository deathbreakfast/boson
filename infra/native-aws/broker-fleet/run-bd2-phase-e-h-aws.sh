#!/usr/bin/env bash
# Phase E–H master orchestrator (sequential; teardown between cells).
set -euo pipefail

BF="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BF/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"

LOG="$ROOT/state/run-bd2-phase-e-h-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

echo "========== Phase E–H campaign start =========="

echo "--- E1: NATS D3 multibench bc=2,4 (W=16, pool partition) ---"
"$BF/run-bd2-multibench-sweep-aws.sh"

echo "--- E2: Redis D0–D1 on aws-c6i.large ---"
"$BF/run-bd2-redis-replay-aws.sh"

echo "--- F: claim path D7 + D8 ---"
"$BF/run-bd2-claim-path-aws.sh"

echo "--- G1: pool pinning D9 ---"
"$BF/run-bd2-pinning-aws.sh"

echo "--- G2: drain-only multibench D10 ---"
"$BF/run-bd2-drain-only-multibench-aws.sh"

echo "--- H: rep sweep, cluster drain, c6i broker ---"
"$BF/run-bd2-read-science-aws.sh"

echo "Phase E–H complete. Log: $LOG"
