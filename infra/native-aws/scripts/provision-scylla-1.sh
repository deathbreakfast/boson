#!/usr/bin/env bash
# Launch 1× bench + 1× scylla t3.medium for single-node Scylla lab.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"

CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-scylla-1-$(date -u +%Y%m%d)}"
MANIFEST_NAME="${BOSON_NATIVE_MANIFEST:-boson-scylla-1}"

if [[ ! -f "$BOSON_NATIVE_AWS_KEY_PATH" ]]; then
  echo "SSH key not found: $BOSON_NATIVE_AWS_KEY_PATH" >&2
  exit 1
fi

SG_ID="$(aws ec2 describe-security-groups \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --filters "Name=group-name,Values=$BOSON_NATIVE_AWS_SG_NAME" \
  --query 'SecurityGroups[0].GroupId' --output text)"

# Intra-SG CQL for bench → scylla
aws ec2 authorize-security-group-ingress \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --group-id "$SG_ID" \
  --protocol tcp --port 9042 --source-group "$SG_ID" 2>/dev/null || true

AMI_ID="$(aws ec2 describe-images \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --owners amazon \
  --filters $BOSON_NATIVE_AMI_X86_FILTER \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
  --output text)"

launch_one() {
  local role="$1"
  local name="boson-${role}-${CAMPAIGN}"
  aws ec2 run-instances \
    --region "$BOSON_NATIVE_AWS_REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$BOSON_NATIVE_AWS_INSTANCE_TYPE" \
    --key-name "$BOSON_NATIVE_AWS_KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":${BOSON_NATIVE_AWS_EBS_GB},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${name}},{Key=Project,Value=${BOSON_NATIVE_AWS_PROJECT_TAG}},{Key=Role,Value=${role}},{Key=Campaign,Value=${CAMPAIGN}}]" \
    --query 'Instances[0].InstanceId' --output text
}

wait_ips() {
  local instance_id="$1"
  local pub="" priv=""
  for _ in $(seq 1 60); do
    read -r pub priv < <(aws ec2 describe-instances \
      --region "$BOSON_NATIVE_AWS_REGION" \
      --instance-ids "$instance_id" \
      --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' \
      --output text 2>/dev/null || echo "None None")
    if [[ -n "$pub" && "$pub" != "None" && -n "$priv" && "$priv" != "None" ]]; then
      echo "$pub $priv"
      return 0
    fi
    sleep 5
  done
  return 1
}

BENCH_ID="$(launch_one bench)"
SCYLLA_ID="$(launch_one scylla)"
echo "launched bench=$BENCH_ID scylla=$SCYLLA_ID"

read -r BENCH_PUB BENCH_PRIV < <(wait_ips "$BENCH_ID")
read -r SCYLLA_PUB SCYLLA_PRIV < <(wait_ips "$SCYLLA_ID")
echo "bench $BENCH_PUB ($BENCH_PRIV) scylla $SCYLLA_PUB ($SCYLLA_PRIV)"

manifest_write "$MANIFEST_NAME" "$(python3 - <<PY
import json, datetime
print(json.dumps({
  "topology": "boson-scylla-1",
  "campaign": "$CAMPAIGN",
  "region": "$BOSON_NATIVE_AWS_REGION",
  "instance_type": "$BOSON_NATIVE_AWS_INSTANCE_TYPE",
  "created_at": datetime.datetime.utcnow().isoformat() + "Z",
  "instances": [
    {"role": "bench", "instance_id": "$BENCH_ID", "public_ip": "$BENCH_PUB", "private_ip": "$BENCH_PRIV"},
    {"role": "scylla", "instance_id": "$SCYLLA_ID", "public_ip": "$SCYLLA_PUB", "private_ip": "$SCYLLA_PRIV"},
  ],
}, indent=2))
PY
)"

echo "manifest=$(manifest_path "$MANIFEST_NAME")"
