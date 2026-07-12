#!/usr/bin/env bash
# Run Scylla contract + catalog E2E on the bench host.
# Prefer prebuilt binaries under CARGO_TARGET_DIR when cargo cannot fetch private git deps.
set -euo pipefail

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target-boson-scylla}"
: "${BOSON_TEST_SCYLLA_CONTACT_POINTS:?set contact points host:port,...}"

echo "run-scylla-e2e: contact_points=$BOSON_TEST_SCYLLA_CONTACT_POINTS"

if [[ -d "$CARGO_TARGET_DIR/debug/deps" ]]; then
  CONTRACT=$(ls "$CARGO_TARGET_DIR"/debug/deps/scylla_queue_backend-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  SCENARIOS=$(ls "$CARGO_TARGET_DIR"/debug/deps/scenarios_full-* 2>/dev/null | grep -v '\.d$' | head -1 || true)
  if [[ -n "${CONTRACT:-}" && -x "$CONTRACT" ]]; then
    echo "running $CONTRACT"
    "$CONTRACT" --ignored --test-threads=1
  else
    cargo test -p boson-backend-scylla --offline -- --ignored --test-threads=1
  fi
  if [[ -n "${SCENARIOS:-}" && -x "$SCENARIOS" ]]; then
    echo "running $SCENARIOS"
    "$SCENARIOS" --include-ignored --test-threads=1 scylla
  else
    cargo test -p boson-e2e --test scenarios_full --offline -- --include-ignored --test-threads=1 scylla
  fi
else
  cargo test -p boson-backend-scylla -- --ignored --test-threads=1
  cargo test -p boson-e2e --test scenarios_full -- --include-ignored --test-threads=1 scylla
fi
