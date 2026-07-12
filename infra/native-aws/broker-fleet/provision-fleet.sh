#!/usr/bin/env bash
# Launch 1× bench + N× standalone brokers (NATS or Redis).
# Usage: BOSON_FLEET_SIZE=4 BOSON_BROKER=nats|redis ./provision-fleet.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/hardware.sh"

FLEET_SIZE="${BOSON_FLEET_SIZE:?set BOSON_FLEET_SIZE (e.g. 1, 2, 4)}"
BROKER="${BOSON_BROKER:-nats}"
case "$BROKER" in
  nats|redis) ;;
  *) echo "BOSON_BROKER must be nats or redis" >&2; exit 1 ;;
esac
BENCH_TYPE="${BOSON_BENCH_INSTANCE_TYPE:-c6i.large}"
BROKER_TYPE="${BOSON_BROKER_INSTANCE_TYPE:-t3.medium}"
BENCH_HARDWARE="$(boson_hardware_tag_from_instance_type "$BENCH_TYPE")"
MANIFEST_NAME="${BOSON_NATIVE_MANIFEST:-boson-${BROKER}-fleet-${FLEET_SIZE}}"
CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-fleet-${FLEET_SIZE}-$(date -u +%Y%m%d)}"

if [[ ! -f "$BOSON_NATIVE_AWS_KEY_PATH" ]]; then
  echo "SSH key not found: $BOSON_NATIVE_AWS_KEY_PATH" >&2
  exit 1
fi

SG_ID="$(aws ec2 describe-security-groups \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --filters "Name=group-name,Values=$BOSON_NATIVE_AWS_SG_NAME" \
  --query 'SecurityGroups[0].GroupId' --output text)"

if [[ "$BROKER" == "nats" ]]; then
  aws ec2 authorize-security-group-ingress \
    --region "$BOSON_NATIVE_AWS_REGION" \
    --group-id "$SG_ID" \
    --protocol tcp --port 4222 --source-group "$SG_ID" 2>/dev/null || true
  aws ec2 authorize-security-group-ingress \
    --region "$BOSON_NATIVE_AWS_REGION" \
    --group-id "$SG_ID" \
    --protocol tcp --port 6222 --source-group "$SG_ID" 2>/dev/null || true
else
  aws ec2 authorize-security-group-ingress \
    --region "$BOSON_NATIVE_AWS_REGION" \
    --group-id "$SG_ID" \
    --protocol tcp --port 6379 --source-group "$SG_ID" 2>/dev/null || true
fi

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
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${name}},{Key=Project,Value=${BOSON_NATIVE_AWS_PROJECT_TAG}},{Key=Role,Value=${role}},{Key=Campaign,Value=${CAMPAIGN}},{Key=FleetSize,Value=${FLEET_SIZE}}]" \
    --query 'Instances[0].InstanceId' --output text
}

wait_ips() {
  local instance_id="$1"
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
echo "launched bench=$BENCH_ID ($BENCH_TYPE)"
BROKER_IDS=()
BROKER_ROLES=()
for i in $(seq 0 $((FLEET_SIZE - 1))); do
  role="${BROKER}-${i}"
  id="$(launch_one "$role" "$BROKER_TYPE")"
  BROKER_IDS+=("$id")
  BROKER_ROLES+=("$role")
  echo "launched $role=$id ($BROKER_TYPE)"
done

read -r BENCH_PUB BENCH_PRIV < <(wait_ips "$BENCH_ID")
INSTANCES_JSON="[{\"role\": \"bench\", \"instance_id\": \"${BENCH_ID}\", \"instance_type\": \"${BENCH_TYPE}\", \"public_ip\": \"${BENCH_PUB}\", \"private_ip\": \"${BENCH_PRIV}\"}"
for idx in "${!BROKER_IDS[@]}"; do
  read -r pub priv < <(wait_ips "${BROKER_IDS[$idx]}")
  role="${BROKER_ROLES[$idx]}"
  INSTANCES_JSON+=", {\"role\": \"${role}\", \"instance_id\": \"${BROKER_IDS[$idx]}\", \"instance_type\": \"${BROKER_TYPE}\", \"public_ip\": \"${pub}\", \"private_ip\": \"${priv}\"}"
done
INSTANCES_JSON+="]"

manifest_write "$MANIFEST_NAME" "$(python3 - <<PY
import json, datetime
print(json.dumps({
  "topology": "boson-${BROKER}-fleet-${FLEET_SIZE}",
  "broker": "${BROKER}",
  "fleet_size": ${FLEET_SIZE},
  "campaign": "${CAMPAIGN}",
  "region": "${BOSON_NATIVE_AWS_REGION}",
  "bench_instance_type": "${BENCH_TYPE}",
  "broker_instance_type": "${BROKER_TYPE}",
  "hardware": "${BENCH_HARDWARE}",
  "created_at": datetime.datetime.utcnow().isoformat() + "Z",
  "instances": ${INSTANCES_JSON},
}, indent=2))
PY
)"

echo "manifest=$(manifest_path "$MANIFEST_NAME") broker=$BROKER fleet_size=$FLEET_SIZE hardware=$BENCH_HARDWARE"
