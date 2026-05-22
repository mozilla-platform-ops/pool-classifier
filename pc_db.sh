#!/bin/bash
#
# pc_db.sh — manage the local pool_classifier Postgres container
#
# Usage:
#   pc_db.sh up        # start postgres (detached)
#   pc_db.sh down      # stop postgres
#   pc_db.sh migrate   # apply SQL migrations
#   pc_db.sh init      # up + migrate (one-step setup)
#   pc_db.sh status    # docker compose ps
#   pc_db.sh logs      # follow postgres logs
#   pc_db.sh psql      # interactive psql shell
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/worker_health/pool_classifier_web/docker-compose.yml"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

dc() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

cmd="${1:-}"
case "$cmd" in
  up)
    dc up -d postgres
    ;;
  down)
    dc down
    ;;
  migrate)
    dc run --rm migrate
    ;;
  init)
    dc up -d postgres
    # Wait for the healthcheck to pass before running migrations.
    for _ in {1..30}; do
      status=$(docker inspect --format '{{.State.Health.Status}}' pool_classifier_web-postgres-1 2>/dev/null || echo "starting")
      [[ "$status" == "healthy" ]] && break
      sleep 1
    done
    dc run --rm migrate
    ;;
  status|ps)
    dc ps
    ;;
  logs)
    dc logs -f postgres
    ;;
  psql)
    docker exec -it pool_classifier_web-postgres-1 psql -U pc -d pool_classifier
    ;;
  ""|-h|--help|help)
    sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "unknown command: $cmd" >&2
    sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
    ;;
esac
