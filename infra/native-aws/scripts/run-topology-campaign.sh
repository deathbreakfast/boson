#!/usr/bin/env bash
# Run distributed-scale Track T campaign on a topology (BM-BM4 adaptive sweep).
set -euo pipefail
TOPO="${1:?topology name}"
HARDWARE="${2:-aws-t3-medium}"
SUBSET="${3:-distributed-scale}"
echo "run-topology-campaign: topo=$TOPO hardware=$HARDWARE subset=$SUBSET"
echo "(stub — deploy boson-bench binary and run: cargo run -p boson-bench -- matrix --subset mem-scale --hardware $HARDWARE)"
