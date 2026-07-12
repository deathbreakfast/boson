#!/usr/bin/env bash
# Artifact storage setup.
# IAM principal lacks s3:CreateBucket / s3:ListAllMyBuckets — use local artifact dir.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/config/defaults.env"
# shellcheck disable=SC1091
source "$ROOT/lib/artifact.sh"

DIR="$(artifact_local_dir)"
mkdir -p "$DIR"
echo "S3 unavailable for this IAM user; using local artifact dir: $DIR"
echo "BOSON_NATIVE_ARTIFACT_DIR=$DIR"
