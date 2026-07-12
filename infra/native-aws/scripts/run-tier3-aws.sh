#!/usr/bin/env bash
# Full Tier 3 AWS campaign: redis then nats on aws-t3-medium (dedicated broker hosts).
# Usage: ./run-tier3-aws.sh [redis|nats|both]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
LOG="$ROOT/state/run-tier3-aws-$(date -u +%Y%m%d-%H%M%S).log"
mkdir -p "$ROOT/state"
exec > >(tee -a "$LOG") 2>&1

TARGET="${1:-both}"
CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-tier3-t3m-$(date -u +%Y%m%d)}"
export BOSON_NATIVE_CAMPAIGN="$CAMPAIGN"

export BOSON_TIER3_PHASE="${BOSON_TIER3_PHASE:-full}"
run_one() {
  local broker="$1"
  export BOSON_BROKER="$broker"
  export BOSON_NATIVE_MANIFEST="boson-${broker}-1"

  echo "========== Tier 3 ${broker} on aws-t3-medium =========="
  "$ROOT/scripts/provision-broker-1.sh"
  "$ROOT/scripts/bootstrap-broker-1.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/deploy-bench-binary.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/run-broker-lab.sh" "$BOSON_NATIVE_MANIFEST"
  BOSON_NATIVE_MANIFEST="$BOSON_NATIVE_MANIFEST" "$ROOT/scripts/fetch-reports.sh" "$BOSON_NATIVE_MANIFEST"
  "$ROOT/scripts/teardown-fleet.sh" "$BOSON_NATIVE_MANIFEST"
  echo "========== ${broker} complete =========="
}

case "$TARGET" in
  redis) run_one redis ;;
  nats) run_one nats ;;
  both)
    run_one redis
    run_one nats
    ;;
  *)
    echo "usage: $0 [redis|nats|both]" >&2
    exit 1
    ;;
esac

echo "Tier 3 AWS campaign done. Log: $LOG"
echo "Reports: $REPO_ROOT/profiling/boson-bench/reports/*-{redis,nats}-*-aws-t3-medium.json"
