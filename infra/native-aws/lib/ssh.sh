#!/usr/bin/env bash
# SSH/SCP helpers for native-aws EC2 workers.
set -euo pipefail

ssh_opts=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=15
  -o BatchMode=yes
  -o ServerAliveInterval=30
)

scp_opts=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=15
  -o BatchMode=yes
)

ssh_cmd() {
  local host="$1"
  shift
  # -n: do not read stdin (safe inside while-read loops).
  ssh -n "${ssh_opts[@]}" -i "$BOSON_NATIVE_AWS_KEY_PATH" "ec2-user@${host}" "$@"
}

# Like ssh_cmd but allows stdin (heredoc scripts).
ssh_cmd_stdin() {
  local host="$1"
  shift
  ssh "${ssh_opts[@]}" -i "$BOSON_NATIVE_AWS_KEY_PATH" "ec2-user@${host}" "$@"
}

scp_to() {
  local host="$1"
  local src="$2"
  local dst="$3"
  scp "${scp_opts[@]}" -i "$BOSON_NATIVE_AWS_KEY_PATH" "$src" "ec2-user@${host}:${dst}"
}

scp_from() {
  local host="$1"
  local src="$2"
  local dst="$3"
  scp "${scp_opts[@]}" -i "$BOSON_NATIVE_AWS_KEY_PATH" "ec2-user@${host}:${src}" "$dst"
}

ssh_wait_ready() {
  local host="$1"
  local tries="${2:-60}"
  local i
  for ((i = 1; i <= tries; i++)); do
    if ssh_cmd "$host" "echo ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  echo "SSH not ready on $host after ${tries} attempts" >&2
  return 1
}
