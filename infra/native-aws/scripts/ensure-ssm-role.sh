#!/usr/bin/env bash
# Instance access setup.
# IAM principal lacks iam:* and ssm:* — campaigns use SSH with continuum-bench key.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"

if [[ ! -f "$BOSON_NATIVE_AWS_KEY_PATH" ]]; then
  echo "SSH key not found: $BOSON_NATIVE_AWS_KEY_PATH" >&2
  exit 1
fi

# Ensure boson-bench-sg exists with SSH from current public IP.
SG_ID="$(aws ec2 describe-security-groups \
  --region "$BOSON_NATIVE_AWS_REGION" \
  --filters "Name=group-name,Values=$BOSON_NATIVE_AWS_SG_NAME" \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"

if [[ -z "$SG_ID" || "$SG_ID" == "None" ]]; then
  SG_ID="$(aws ec2 create-security-group \
    --region "$BOSON_NATIVE_AWS_REGION" \
    --group-name "$BOSON_NATIVE_AWS_SG_NAME" \
    --description "boson native-aws bench" \
    --query GroupId --output text)"
  echo "Created security group $SG_ID"
fi

MY_IP="$(curl -sf https://checkip.amazonaws.com || true)"
if [[ -n "$MY_IP" ]]; then
  aws ec2 authorize-security-group-ingress \
    --region "$BOSON_NATIVE_AWS_REGION" \
    --group-id "$SG_ID" \
    --protocol tcp --port 22 --cidr "${MY_IP}/32" 2>/dev/null || true
fi

echo "SSM/IAM unavailable; using SSH key=$BOSON_NATIVE_AWS_KEY_NAME path=$BOSON_NATIVE_AWS_KEY_PATH sg=$SG_ID"
