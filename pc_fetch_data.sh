#!/bin/bash
#
# pc_fetch_data.sh
# Version: 1.1
# Date: 2026-05-11

set -e
#set -x

BASE_URL="${PC_BASE_URL:-http://localhost:8080}"

pools=(
  proj-autophone/gecko-t-bitbar-gw-perf-a55
  proj-autophone/gecko-t-bitbar-gw-perf-p6
  proj-autophone/gecko-t-bitbar-gw-perf-s24
  proj-autophone/gecko-t-bitbar-gw-unit-p5
  proj-autophone/gecko-t-lambda-alpha-a55
  proj-autophone/gecko-t-lambda-perf-a55
  releng-hardware/gecko-t-linux-talos-1804
  releng-hardware/gecko-t-linux-talos-2404
)

for pool in "${pools[@]}"; do
  echo "==> classifying $pool"
  body=$(curl -s -X POST -w "\n%{http_code}" "$BASE_URL/classify/$pool")
  http_code=$(echo "$body" | tail -1)
  body=$(echo "$body" | sed '$d')
  echo "HTTP $http_code"
  echo "$body" | jq . 2>/dev/null || echo "$body"
done
