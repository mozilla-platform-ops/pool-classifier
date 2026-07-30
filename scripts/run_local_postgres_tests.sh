#!/usr/bin/env bash
# Run the complete test suite, including tests that require local Postgres.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

cd "${repo_root}"
./pc_db.sh init

export PC_TEST_DATABASE_URL="${PC_TEST_DATABASE_URL:-postgresql://pc:pc@127.0.0.1:5433/pool_classifier}"  # pragma: allowlist secret

exec pipenv run pytest tests/ -x -q "$@"
