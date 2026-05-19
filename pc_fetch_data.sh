#!/bin/bash
#
# pc_fetch_data.sh
# Version: 1.2
# Date: 2026-05-14

set -e
#set -x

BASE_URL="${PC_BASE_URL:-http://localhost:8080}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POOLS_YAML="${POOLS_FILE:-$SCRIPT_DIR/worker_health/pool_classifier_web/pools.yaml}"

shuffle() {
  python3 -c "import random,sys; lines=sys.stdin.read().splitlines(); random.shuffle(lines); print('\n'.join(lines))"
}

classify_pools() {
  local pool
  for pool in "$@"; do
    echo "==> classifying $pool"
    body=$(curl -s -X POST -w "\n%{http_code}" "$BASE_URL/classify/$pool")
    http_code=$(echo "$body" | tail -1)
    body=$(echo "$body" | sed '$d')
    echo "HTTP $http_code"
    echo "$body" | jq . 2>/dev/null || echo "$body"
  done
}

# VM pools are excluded: they're not long-lived and have a large volume of jobs/workers that overwhelms the tool.
all_pools=()
while IFS= read -r pool; do
  all_pools+=("$pool")
done < <(yq '.pools[] | .provisioner + "/" + .worker_type' "$POOLS_YAML" | grep -viE '\bvms?\b')

autophone=()
rest=()
for pool in "${all_pools[@]}"; do
  if [[ "$pool" == proj-autophone/* ]]; then
    autophone+=("$pool")
  else
    rest+=("$pool")
  fi
done

# Phase 1: proj-autophone
phase1=()
while IFS= read -r pool; do
  phase1+=("$pool")
done < <(printf '%s\n' "${autophone[@]}" | shuffle)

echo "==> Phase 1: proj-autophone (${#phase1[@]} pools)"
classify_pools "${phase1[@]}"

# Phase 2: everything else
phase2=()
while IFS= read -r pool; do
  phase2+=("$pool")
done < <(printf '%s\n' "${rest[@]}" | shuffle)

echo "==> Phase 2: remaining pools (${#phase2[@]} pools)"
classify_pools "${phase2[@]}"
