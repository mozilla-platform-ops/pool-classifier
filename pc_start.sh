#!/bin/bash
#
# pc_start.sh — start the pool_classifier Flask app
# Set POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS=1 to render current page
# code instead of stored dashboard snapshots (useful for local UI work).
# HTML pages show a port-derived color and badge in this local launcher. Debug
# instances say "DEBUG PORT"; stable instances show only the port. Override
# them with PC_INSTANCE_LABEL and PC_INSTANCE_COLOR (#RRGGBB) when desired.
#

set -e

export TC_TOKEN_FILE="${TC_TOKEN_FILE:-$HOME/.tc_token}"
export DATABASE_URL="${DATABASE_URL:-postgresql://pc:pc@127.0.0.1:5433/pool_classifier}"  # pragma: allowlist secret
export PC_INSTANCE_IDENTITY="${PC_INSTANCE_IDENTITY:-1}"
# pc_start.sh is the local-only launcher; allow its local admin page by default.
# Set ADMIN_IAP_BYPASS=0 to exercise the production authorization path.
export ADMIN_IAP_BYPASS="${ADMIN_IAP_BYPASS:-1}"

PORT="${PC_PORT:-8080}"

cd "$(dirname "${BASH_SOURCE[0]}")"

exec uv run --frozen flask --app worker_health.pool_classifier_web.app:create_app run -p "$PORT" "$@"
