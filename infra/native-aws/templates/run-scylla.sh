#!/usr/bin/env bash
# Start Scylla container on host network (one node per EC2).
set -euo pipefail

SCYLLA_IP="${SCYLLA_IP:?}"
SEED_IP="${SEED_IP:?}"
SCYLLA_INDEX="${SCYLLA_INDEX:-0}"
DATA_DIR="/var/lib/boson/scylla${SCYLLA_INDEX}"
CONTAINER="boson-scylla${SCYLLA_INDEX}"
IMAGE="${SCYLLA_IMAGE:?}"

sudo mkdir -p "$DATA_DIR"
sudo docker rm -f "$CONTAINER" 2>/dev/null || true

sudo docker run -d --name "$CONTAINER" --restart unless-stopped --network host \
  -v "${DATA_DIR}:/var/lib/scylla" \
  "$IMAGE" \
  --smp 1 --memory 750M --overprovisioned 1 \
  --listen-address "$SCYLLA_IP" \
  --rpc-address "$SCYLLA_IP" \
  --broadcast-address "$SCYLLA_IP" \
  --broadcast-rpc-address "$SCYLLA_IP" \
  --seeds "$SEED_IP"

echo "Scylla ${SCYLLA_INDEX} on ${SCYLLA_IP}:9042 seeds=${SEED_IP}"
