#!/usr/bin/env bash
# Export fleet broker URLs from manifest private IPs.
# Usage: eval "$(./export-fleet-env.sh boson-nats-fleet-4)"
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-nats-fleet-4}}"
MANIFEST="$(manifest_read "$MANIFEST_NAME")"

BROKER="$(echo "$MANIFEST" | python3 -c "import json,sys; print(json.load(sys.stdin).get('broker','nats'))")"

if [[ "$BROKER" == "redis" ]]; then
  URLS="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
urls = []
for i in sorted(m['instances'], key=lambda x: x['role']):
    if i['role'].startswith('redis-'):
        urls.append(f\"redis://{i['private_ip']}:6379\")
print(','.join(urls))
")"
  echo "export BOSON_REDIS_URLS='${URLS}'"
else
  URLS="$(echo "$MANIFEST" | python3 -c "
import json, sys
m = json.load(sys.stdin)
urls = []
for i in sorted(m['instances'], key=lambda x: x['role']):
    if i['role'].startswith('nats-'):
        urls.append(f\"nats://{i['private_ip']}:4222\")
print(','.join(urls))
")"
  echo "export BOSON_NATS_URLS='${URLS}'"
fi

FLEET_SIZE="$(echo "$MANIFEST" | python3 -c "import json,sys; print(json.load(sys.stdin).get('fleet_size', 1))")"

echo "export BOSON_FLEET_SIZE='${FLEET_SIZE}'"
echo "export BOSON_NATIVE_MANIFEST='${MANIFEST_NAME}'"
echo "export BOSON_BROKER='${BROKER}'"
