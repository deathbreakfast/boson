#!/usr/bin/env bash
# Launch bc_max bench hosts + N standalone brokers for multi-bench campaigns.
# Usage: BENCH_COUNT=4 BOSON_FLEET_SIZE=4 BOSON_BROKER=nats|redis ./provision-multibench-fleet.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export BOSON_NATIVE_AWS_ROOT="$ROOT"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/manifest.sh"
# shellcheck disable=SC1091
source "$ROOT/lib/hardware.sh"

BENCH_COUNT="${BENCH_COUNT:-4}"
FLEET_SIZE="${BOSON_FLEET_SIZE:-4}"
BROKER="${BOSON_BROKER:-nats}"
case "$BROKER" in
  nats|redis) ;;
  *) echo "BOSON_BROKER must be nats or redis" >&2; exit 1 ;;
esac
BENCH_TYPE="${BOSON_BENCH_INSTANCE_TYPE:-c6i.large}"
BROKER_TYPE="${BOSON_BROKER_INSTANCE_TYPE:-t3.medium}"
BENCH_HARDWARE="$(boson_hardware_tag_from_instance_type "$BENCH_TYPE")"
MANIFEST_NAME="${BOSON_NATIVE_MANIFEST:-boson-${BROKER}-multibench-${FLEET_SIZE}}"
CAMPAIGN="${BOSON_NATIVE_CAMPAIGN:-boson-multibench-${FLEET_SIZE}-$(date -u +%Y%m%d)}"

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
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${name}},{Key=Project,Value=${BOSON_NATIVE_AWS_PROJECT_TAG}},{Key=Role,Value=${role}},{Key=Campaign,Value=${CAMPAIGN}},{Key=FleetSize,Value=${FLEET_SIZE}},{Key=BenchCount,Value=${BENCH_COUNT}}]" \
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

INSTANCES_JSON="["
FIRST=1

for i in $(seq 0 $((BENCH_COUNT - 1))); do
  role="bench"
  if [[ "$BENCH_COUNT" -gt 1 ]]; then
    role="bench-${i}"
  fi
  id="$(launch_one "$role" "$BENCH_TYPE")"
  echo "launched $role=$id ($BENCH_TYPE)"
  read -r pub priv < <(wait_ips "$id")
  if [[ "$FIRST" -eq 1 ]]; then
    INSTANCES_JSON+="{\"role\": \"${role}\", \"instance_id\": \"${id}\", \"instance_type\": \"${BENCH_TYPE}\", \"public_ip\": \"${pub}\", \"private_ip\": \"${priv}\"}"
    FIRST=0
  else
    INSTANCES_JSON+=", {\"role\": \"${role}\", \"instance_id\": \"${id}\", \"instance_type\": \"${BENCH_TYPE}\", \"public_ip\": \"${pub}\", \"private_ip\": \"${priv}\"}"
  fi
done

for i in $(seq 0 $((FLEET_SIZE - 1))); do
  role="${BROKER}-${i}"
  id="$(launch_one "$role" "$BROKER_TYPE")"
  echo "launched $role=$id ($BROKER_TYPE)"
  read -r pub priv < <(wait_ips "$id")
  INSTANCES_JSON+=", {\"role\": \"${role}\", \"instance_id\": \"${id}\", \"instance_type\": \"${BROKER_TYPE}\", \"public_ip\": \"${pub}\", \"private_ip\": \"${priv}\"}"
done
INSTANCES_JSON+="]"

manifest_write "$MANIFEST_NAME" "$(python3 - <<PY
import json, datetime
print(json.dumps({
  "topology": "boson-${BROKER}-multibench-${FLEET_SIZE}",
  "broker": "${BROKER}",
  "fleet_size": ${FLEET_SIZE},
  "bench_count": ${BENCH_COUNT},
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

echo "manifest=$(manifest_path "$MANIFEST_NAME") broker=$BROKER bench_count=$BENCH_COUNT fleet_size=$FLEET_SIZE hardware=$BENCH_HARDWARE"
