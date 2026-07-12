#!/usr/bin/env bash
# Start Redis on broker host (dedicated t3.medium, not colocated with bench CPU).
set -euo pipefail

REDIS_IP="${REDIS_IP:?}"
CONTAINER="${REDIS_CONTAINER:-boson-redis}"
IMAGE="${REDIS_IMAGE:-redis:7-alpine}"

if ! command -v docker >/dev/null 2>&1; then
  sudo dnf install -y docker
  sudo systemctl enable --now docker
fi

sudo docker rm -f "$CONTAINER" 2>/dev/null || true
sudo docker pull "$IMAGE"
sudo docker run -d --name "$CONTAINER" --restart unless-stopped \
  -p "${REDIS_IP}:6379:6379" \
  "$IMAGE" \
  redis-server --bind 0.0.0.0 --protected-mode no

echo "Redis listening on ${REDIS_IP}:6379"
