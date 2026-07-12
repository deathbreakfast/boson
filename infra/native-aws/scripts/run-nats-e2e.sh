#!/usr/bin/env bash
# Run NATS contract + catalog E2E (mirror run-scylla-e2e.sh).
# Prefer prebuilt binaries under CARGO_TARGET_DIR when cargo cannot fetch private git deps.
set -euo pipefail

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target-boson-nats}"
unset BOSON_SKIP_RUN_ROWS
export BOSON_NATS_STREAM_REPLICAS="${BOSON_NATS_STREAM_REPLICAS:-1}"
export BOSON_NATS_QUEUE_MODE="${BOSON_NATS_QUEUE_MODE:-workqueue}"
if [[ -z "${BOSON_TEST_NATS_URL:-}" && -z "${BOSON_NATS_URLS:-}" ]]; then
  echo "set BOSON_TEST_NATS_URL or BOSON_NATS_URLS" >&2
  exit 1
fi

echo "run-nats-e2e: url=${BOSON_TEST_NATS_URL:-} urls=${BOSON_NATS_URLS:-}"

run_nats_contracts() {
  cargo test -p boson-backend-nats --offline -- "$@" || \
    cargo test -p boson-backend-nats -- "$@"
}

if [[ -d "$CARGO_TARGET_DIR/debug/deps" ]]; then
  KV=$(ls "$CARGO_TARGET_DIR"/debug/deps/nats_queue_backend-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  WQ=$(ls "$CARGO_TARGET_DIR"/debug/deps/nats_workqueue_* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  FLEET=$(ls "$CARGO_TARGET_DIR"/debug/deps/nats_fleet_routing-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  SCENARIOS=$(ls "$CARGO_TARGET_DIR"/debug/deps/scenarios_full-* 2>/dev/null | grep -v '\.d$' | head -1 || true)

  if [[ -n "${KV:-}" && -x "$KV" ]]; then
    echo "running $KV"
    "$KV" --ignored --test-threads=1
  else
    run_nats_contracts --ignored --test-threads=1 --test nats_queue_backend
  fi
  if [[ -n "${WQ:-}" && -x "$WQ" ]]; then
    echo "running $WQ"
    "$WQ" --ignored --test-threads=1
  else
    run_nats_contracts --ignored --test-threads=1 --test nats_workqueue_stream
    run_nats_contracts --ignored --test-threads=1 --test nats_workqueue_dual
  fi
  if [[ -n "${BOSON_NATS_URLS:-}" ]] && [[ "$(echo "$BOSON_NATS_URLS" | tr ',' '\n' | grep -c .)" -ge 2 ]]; then
    if [[ -n "${FLEET:-}" && -x "$FLEET" ]]; then
      echo "running $FLEET"
      "$FLEET" --ignored --test-threads=1
    else
      run_nats_contracts --ignored --test-threads=1 --test nats_fleet_routing
    fi
  else
    echo "skip nats_fleet_routing (set BOSON_NATS_URLS with 2+ brokers to enable)"
  fi
  if [[ -n "${SCENARIOS:-}" && -x "$SCENARIOS" ]]; then
    echo "running $SCENARIOS"
    "$SCENARIOS" --include-ignored --test-threads=1 nats
  else
    cargo test -p boson-e2e --test scenarios_full --offline -- --include-ignored --test-threads=1 nats
  fi
else
  run_nats_contracts --ignored --test-threads=1 --test nats_queue_backend
  run_nats_contracts --ignored --test-threads=1 --test nats_workqueue_stream
  run_nats_contracts --ignored --test-threads=1 --test nats_workqueue_dual
  cargo test -p boson-e2e --test scenarios_full -- --include-ignored --test-threads=1 nats
fi
