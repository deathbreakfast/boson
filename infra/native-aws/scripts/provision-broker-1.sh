#!/usr/bin/env bash
# Launch 1× bench + 1× broker (redis or nats).
# Bench/broker instance types: BOSON_BENCH_INSTANCE_TYPE / BOSON_BROKER_INSTANCE_TYPE.
# Usage: BOSON_BROKER=redis|nats ./provision-broker-1.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/hardware.sh"

BENCH_TYPE="${BOSON_BENCH_INSTANCE_TYPE:-$BOSON_NATIVE_AWS_INSTANCE_TYPE}"
BROKER_TYPE="${BOSON_BROKER_INSTANCE_TYPE:-$BOSON_NATIVE_AWS_INSTANCE_TYPE}"
BENCH_HARDWARE="$(boson_hardware_tag_from_instance_type "$BENCH_TYPE")"

BROKER="${BOSON_BROKER:?set BOSON_BROKER to redis or nats}"
case "$BROKER" in
  redis) BROKER_PORT=6379 ;;
  nats) BROKER_PORT=4222 ;;
  *) echo "BOSON_BROKER must be redis or nats" >&2; exit 1 ;;
esac

MANIFEST_NAME="${BOSON_NATIVE_MANIFEST:-boson-${BROKER}-1}"
CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-tier3-${BROKER}-$(date -u +%Y%m%d)}"

if [[ ! -f "$BOSON_NATIVE_AWS_KEY_PATH" ]]; then
  echo "SSH key not found: $BOSON_NATIVE_AWS_KEY_PATH" >&2
  exit 1
fi

SG_ID="$(aws ec2 describe-security-groups \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --filters "Name=group-name,Values=$BOSON_NATIVE_AWS_SG_NAME" \
  --query 'SecurityGroups[0].GroupId' --output text)"

aws ec2 authorize-security-group-ingress \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --group-id "$SG_ID" \
  --protocol tcp --port "$BROKER_PORT" --source-group "$SG_ID" 2>/dev/null || true

AMI_ID="$(aws ec2 describe-images \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --owners amazon \
  --filters $BOSON_NATIVE_AMI_X86_FILTER \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
  --output text)"

launch_one() {
  local role="$1"
  local itype="$2"
  local name="boson-${role}-${CAMPAIGN}"
  aws ec2 run-instances \
    --region "$BOSON_NATIVE_AWS_REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$itype" \
    --key-name "$BOSON_NATIVE_AWS_KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":${BOSON_NATIVE_AWS_EBS_GB},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${name}},{Key=Project,Value=${BOSON_NATIVE_AWS_PROJECT_TAG}},{Key=Role,Value=${role}},{Key=Campaign,Value=${CAMPAIGN}},{Key=Broker,Value=${BROKER}}]" \
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

BENCH_ID="$(launch_one bench "$BENCH_TYPE")"
BROKER_ID="$(launch_one "$BROKER" "$BROKER_TYPE")"
echo "launched bench=$BENCH_ID ($BENCH_TYPE) ${BROKER}=$BROKER_ID ($BROKER_TYPE) hardware=$BENCH_HARDWARE"

read -r BENCH_PUB BENCH_PRIV < <(wait_ips "$BENCH_ID")
read -r BROKER_PUB BROKER_PRIV < <(wait_ips "$BROKER_ID")
echo "bench $BENCH_PUB ($BENCH_PRIV) ${BROKER} $BROKER_PUB ($BROKER_PRIV)"

manifest_write "$MANIFEST_NAME" "$(python3 - <<PY
import json, datetime
print(json.dumps({
  "topology": "boson-${BROKER}-1",
  "broker": "${BROKER}",
  "campaign": "${CAMPAIGN}",
  "region": "${BOSON_NATIVE_AWS_REGION}",
  "instance_type": "${BENCH_TYPE}",
  "bench_instance_type": "${BENCH_TYPE}",
  "broker_instance_type": "${BROKER_TYPE}",
  "hardware": "${BENCH_HARDWARE}",
  "created_at": datetime.datetime.utcnow().isoformat() + "Z",
  "instances": [
    {"role": "bench", "instance_id": "${BENCH_ID}", "instance_type": "${BENCH_TYPE}", "public_ip": "${BENCH_PUB}", "private_ip": "${BENCH_PRIV}"},
    {"role": "${BROKER}", "instance_id": "${BROKER_ID}", "instance_type": "${BROKER_TYPE}", "public_ip": "${BROKER_PUB}", "private_ip": "${BROKER_PRIV}"},
  ],
}, indent=2))
PY
)"

echo "manifest=$(manifest_path "$MANIFEST_NAME")"
