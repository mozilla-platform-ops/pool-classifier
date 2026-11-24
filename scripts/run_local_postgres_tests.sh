#!/usr/bin/env bash
# Run the complete test suite, including tests that require local Postgres.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

cd "${repo_root}"
if [[ -n "${PC_TEST_DATABASE_URL:-}" ]]; then
    DATABASE_URL="$PC_TEST_DATABASE_URL" uv run --frozen --group dev python -m worker_health.pool_classifier_web.scripts.migrate
else
    ./pc_db.sh init
fi

export PC_TEST_DATABASE_URL="${PC_TEST_DATABASE_URL:-postgresql://pc:pc@127.0.0.1:5433/pool_classifier}"  # pragma: allowlist secret

exec uv run --frozen --group dev pytest tests/ -x -q "$@"
