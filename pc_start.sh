#!/bin/bash
#
# pc_start.sh — start the stable scanner and debug UI together.
# Set PC_PORT to launch only one instance. The paired debug UI renders live
# page code with dashboard snapshots disabled.
# HTML pages show a port-derived color and badge in this local launcher. Port
# 18081 is the local UI-debug instance; 18080 is the stable scanner target.
# Debug instances say "DEBUG PORT"; stable instances show only the port.
# Override the badge with PC_INSTANCE_LABEL and PC_INSTANCE_COLOR (#RRGGBB).
#

set -e

export TC_TOKEN_FILE="${TC_TOKEN_FILE:-$HOME/.tc_token}"
export DATABASE_URL="${DATABASE_URL:-postgresql://pc:pc@127.0.0.1:5433/pool_classifier}"  # pragma: allowlist secret
export PC_INSTANCE_IDENTITY="${PC_INSTANCE_IDENTITY:-1}"
# pc_start.sh is the local-only launcher; allow its local admin page by default.
# Set ADMIN_IAP_BYPASS=0 to exercise the production authorization path.
export ADMIN_IAP_BYPASS="${ADMIN_IAP_BYPASS:-1}"

cd "$(dirname "${BASH_SOURCE[0]}")"

start_instance() {
  local port="$1"
  shift
  local debug_args=()
  # Keep debug mode for callers that explicitly select the previous debug port.
  if [ "$port" = "18081" ] || [ "$port" = "8081" ]; then
    debug_args=(--debug)
  fi
  exec uv run --frozen flask --app worker_health.pool_classifier_web.app:create_app run -p "$port" "${debug_args[@]}" "$@"
}

if [ -n "${PC_PORT:-}" ]; then
  start_instance "$PC_PORT" "$@"
fi

# Give each server its own process group so shutdown also reaches the debug reloader.
set -m
server_pids=()
stop_instances() {
  trap '' INT TERM HUP
  local pid
  for pid in "${server_pids[@]}"; do
    kill -TERM -- "-$pid" 2>/dev/null || true
  done
  for pid in "${server_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap stop_instances EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

start_instance 18080 "$@" &
server_pids+=("$!")
POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS=1 start_instance 18081 "$@" &
server_pids+=("$!")
stable_pid="${server_pids[0]}"
debug_pid="${server_pids[1]}"

# macOS's Bash 3.2 does not have wait -n.
while kill -0 "$stable_pid" 2>/dev/null && kill -0 "$debug_pid" 2>/dev/null; do
  sleep 1
done
if kill -0 "$stable_pid" 2>/dev/null; then
  wait "$debug_pid"
else
  wait "$stable_pid"
fi
