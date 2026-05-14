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

pools=()
while IFS= read -r pool; do
  pools+=("$pool")
done < <(yq '.pools[] | .provisioner + "/" + .worker_type' "$POOLS_YAML")

for pool in "${pools[@]}"; do
  echo "==> classifying $pool"
  body=$(curl -s -X POST -w "\n%{http_code}" "$BASE_URL/classify/$pool")
  http_code=$(echo "$body" | tail -1)
  body=$(echo "$body" | sed '$d')
  echo "HTTP $http_code"
  echo "$body" | jq . 2>/dev/null || echo "$body"
done
