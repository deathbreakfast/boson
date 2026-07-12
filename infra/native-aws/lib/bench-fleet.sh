#!/usr/bin/env bash
# Resolve bench host IP by 1-based index from manifest roles bench / bench-0..bench-N.
set -euo pipefail

bench_ip_for_role() {
  local manifest_json="$1"
  local role="$2"
  echo "$manifest_json" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(next(i['public_ip'] for i in m['instances'] if i['role'] == sys.argv[1]))
" "$role"
}

resolve_bench_ip() {
  local manifest_json="$1"
  local idx="$2"
  local role="bench-$((idx - 1))"
  if bench_ip_for_role "$manifest_json" "$role" 2>/dev/null; then
    return 0
  fi
  bench_ip_for_role "$manifest_json" "bench"
}

bench_hosts_from_manifest() {
  local manifest_json="$1"
  echo "$manifest_json" | python3 -c "
import json, sys
m = json.load(sys.stdin)
roles = sorted(
    (i['role'], i['public_ip']) for i in m['instances']
    if i['role'] == 'bench' or i['role'].startswith('bench-')
)
for role, ip in roles:
    print(f'{role}\t{ip}')
"
}
