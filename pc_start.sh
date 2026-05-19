#!/bin/bash
#
# pc_start.sh — start the pool_classifier Flask app
#

set -e

export TC_TOKEN_FILE="${TC_TOKEN_FILE:-$HOME/.tc_token}"
export DATABASE_URL="${DATABASE_URL:-postgresql://pc:pc@127.0.0.1:5433/pool_classifier}"  # pragma: allowlist secret

PORT="${PC_PORT:-8080}"

cd "$(dirname "${BASH_SOURCE[0]}")"

exec pipenv run flask --app worker_health.pool_classifier_web.app:create_app run -p "$PORT" "$@"
