#!/usr/bin/env bash
# Start NATS JetStream on broker host (dedicated t3.medium).
set -euo pipefail

NATS_IP="${NATS_IP:?}"
CONTAINER="${NATS_CONTAINER:-boson-nats}"
IMAGE="${NATS_IMAGE:-nats:2.10-alpine}"

if ! command -v docker >/dev/null 2>&1; then
  sudo dnf install -y docker
  sudo systemctl enable --now docker
fi

sudo docker rm -f "$CONTAINER" 2>/dev/null || true
sudo docker pull "$IMAGE"
sudo docker run -d --name "$CONTAINER" --restart unless-stopped \
  --network host \
  "$IMAGE" \
  -js -a "${NATS_IP}" -p 4222

echo "NATS JetStream listening on ${NATS_IP}:4222"
