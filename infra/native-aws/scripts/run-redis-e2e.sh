#!/usr/bin/env bash
# Run Redis contract + catalog E2E (mirror run-scylla-e2e.sh).
# Prefer prebuilt binaries under CARGO_TARGET_DIR when cargo cannot fetch private git deps.
set -euo pipefail

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target-boson-redis}"
unset BOSON_SKIP_RUN_ROWS
if [[ -z "${BOSON_TEST_REDIS_URL:-}" && -z "${BOSON_REDIS_URLS:-}" ]]; then
  echo "set BOSON_TEST_REDIS_URL or BOSON_REDIS_URLS" >&2
  exit 1
fi

echo "run-redis-e2e: url=${BOSON_TEST_REDIS_URL:-} urls=${BOSON_REDIS_URLS:-}"

run_redis_contracts() {
  cargo test -p boson-backend-redis --offline -- "$@" || \
    cargo test -p boson-backend-redis -- "$@"
}

if [[ -d "$CARGO_TARGET_DIR/debug/deps" ]]; then
  CONTRACT=$(ls "$CARGO_TARGET_DIR"/debug/deps/redis_queue_backend-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  FLEET=$(ls "$CARGO_TARGET_DIR"/debug/deps/redis_fleet_routing-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  SCENARIOS=$(ls "$CARGO_TARGET_DIR"/debug/deps/scenarios_full-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  if [[ -n "${CONTRACT:-}" && -x "$CONTRACT" ]]; then
    echo "running $CONTRACT"
    "$CONTRACT" --ignored --test-threads=1
  else
    run_redis_contracts --ignored --test-threads=1 --test redis_queue_backend
  fi
  if [[ -n "${BOSON_REDIS_URLS:-}" ]] && [[ "$(echo "$BOSON_REDIS_URLS" | tr ',' '\n' | grep -c .)" -ge 2 ]]; then
    if [[ -n "${FLEET:-}" && -x "$FLEET" ]]; then
      echo "running $FLEET"
      "$FLEET" --ignored --test-threads=1
    else
      run_redis_contracts --ignored --test-threads=1 --test redis_fleet_routing
    fi
  else
    echo "skip redis_fleet_routing (set BOSON_REDIS_URLS with 2+ brokers to enable)"
  fi
  if [[ -n "${SCENARIOS:-}" && -x "$SCENARIOS" ]]; then
    echo "running $SCENARIOS"
    "$SCENARIOS" --include-ignored --test-threads=1 redis
  else
    cargo test -p boson-e2e --test scenarios_full --offline -- --include-ignored --test-threads=1 redis
  fi
else
  run_redis_contracts --ignored --test-threads=1 --test redis_queue_backend
  if [[ -n "${BOSON_REDIS_URLS:-}" ]] && [[ "$(echo "$BOSON_REDIS_URLS" | tr ',' '\n' | grep -c .)" -ge 2 ]]; then
    run_redis_contracts --ignored --test-threads=1 --test redis_fleet_routing
  else
    echo "skip redis_fleet_routing (set BOSON_REDIS_URLS with 2+ brokers to enable)"
  fi
  cargo test -p boson-e2e --test scenarios_full -- --include-ignored --test-threads=1 redis
fi
