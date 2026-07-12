#!/usr/bin/env bash
# Terminate EC2 instances from a manifest (and any leftover builder for the campaign).
# Usage: teardown-fleet.sh [manifest-name]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"

MANIFEST_NAME="${1:-${BOSON_NATIVE_MANIFEST:-boson-baseline-t3m}}"

terminate_manifest() {
  local name="$1"
  local path
  path="$(manifest_path "$name")"
  if [[ ! -f "$path" ]]; then
    return 0
  fi
  local ids
  ids="$(python3 - <<PY
import json
m=json.load(open("$path"))
print(" ".join(i["instance_id"] for i in m.get("instances", []) if i.get("instance_id")))
PY
)"
  if [[ -n "$ids" ]]; then
    echo "Terminating $name: $ids"
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --region "$BOSON_NATIVE_AWS_REGION" --instance-ids $ids
  fi
  rm -f "$path"
}

terminate_manifest "$MANIFEST_NAME"

# Also terminate any boson-bench tagged running instances for this campaign if manifest missing entries.
CAMPAIGN="$(python3 - <<PY
import json, os
path="$(manifest_path "$MANIFEST_NAME")"
# manifest may already be deleted
print(os.environ.get("BOSON_NATIVE_CAMPAIGN", ""))
PY
)"

# Safety: terminate any remaining Project=boson-bench Role=builder|bench instances that are running.
EXTRA="$(aws ec2 describe-instances \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --filters \
    "Name=tag:Project,Values=${BOSON_NATIVE_AWS_PROJECT_TAG}" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].InstanceId' \
  --output text)"
if [[ -n "$EXTRA" && "$EXTRA" != "None" ]]; then
  echo "Terminating remaining boson-bench instances: $EXTRA"
  # shellcheck disable=SC2086
  aws ec2 terminate-instances --region "$BOSON_NATIVE_AWS_REGION" --instance-ids $EXTRA
fi

# Clean builder manifests
for f in "$(manifest_dir)"/boson-builder-*.json; do
  [[ -f "$f" ]] || continue
  name="$(basename "$f" .json)"
  terminate_manifest "$name"
done

echo "teardown complete"
