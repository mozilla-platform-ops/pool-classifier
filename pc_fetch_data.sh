#!/bin/bash
#
# pc_fetch_data.sh
# Trigger the same aggregate scan path used by the production scheduler.

set -euo pipefail

BASE_URL="${PC_BASE_URL:-http://localhost:8080}"

echo "==> classifying all enabled pools"
body=$(curl --silent --show-error -X POST --write-out "\n%{http_code}" "$BASE_URL/classify-all")
http_code=$(printf '%s\n' "$body" | tail -1)
body=$(printf '%s\n' "$body" | sed '$d')

echo "HTTP $http_code"
printf '%s\n' "$body" | jq . 2>/dev/null || printf '%s\n' "$body"

test "$http_code" -ge 200 && test "$http_code" -lt 300
