# Local development

Pool Classifier periodically polls Taskcluster worker pools, classifies recent
task results from their logs, and serves a Flask dashboard. Failure patterns
are defined in
[`patterns.yaml`](../../worker_health/pool_classifier_web/patterns.yaml).

## Setup

Install the development dependencies:

```sh
uv sync --group dev
```

Create `~/.tc_token` with a Taskcluster credential:

```json
{
  "clientId": "mozilla-auth0/ad|Mozilla-LDAP|example/pool-classifier",
  "accessToken": "REDACTED"
}
```

Start local Postgres and apply migrations:

```sh
./pc_db.sh init
./pc_db.sh status
./pc_db.sh psql
```

The helper wraps the repository's Docker Compose configuration. Postgres listens
on `127.0.0.1:5433` and its data is retained in `./pgdata/`; `docker compose
down -v` does not remove that bind mount.

Start the app:

```sh
./pc_start.sh                  # serves on :8080
PC_PORT=8090 ./pc_start.sh     # override port
```

Useful local URLs:

- Dashboard: <http://localhost:8080/>
- Admin dashboard: <http://localhost:8080/admin>
- Example pool: <http://localhost:8080/pools/proj-autophone/gecko-t-lambda-perf-a55>
- Liveness check: <http://localhost:8080/healthz>
- [Public API](../reference/public-api.md)

## Classify pools

```sh
# One pool
curl -s -X POST localhost:8080/classify/proj-autophone/gecko-t-lambda-perf-a55 | jq .

# Every enabled non-VM pool, through the aggregate production path
bash pc_fetch_data.sh

# Include -vms worker types too
INCLUDE_VMS_POOLS=1 bash pc_fetch_data.sh
```

OIDC validation is disabled locally when `CLASSIFY_OIDC_AUDIENCE` is unset. In
production, classify requests require a Cloud Scheduler-signed JWT.

## Test

```sh
# Unit and web tests that do not need Postgres
uv run --frozen --group dev pytest tests/ --ignore=tests/test_runner.py -x -q

# Required full suite, including Postgres-backed tests
scripts/run_local_postgres_tests.sh
```

## Configuration

`pools.yaml` is the pool registry. Defaults apply to all pools, provisioner
defaults override them, and individual pool values take precedence. Disabled
pools remain visible but are not classified. Enabled worker types ending in
`-vms` are excluded from automatic classification unless `INCLUDE_VMS_POOLS=1`.

`patterns.yaml` defines failure categories. Patterns are ordered by severity
(`critical`, `high`, then `low`) and file order; the first match wins.

## Preview a task classification

After editing the normal `patterns.yaml`, compare its proposed result for one
Taskcluster task with the checked-in rules. This fetches one terminal run and
its log, but does not write to the database or modify the worktree:

```sh
uv run pool_classifier.py --preview-task <task-id>
```

The default baseline is `HEAD`; the proposed rules are the working-tree copy
of `worker_health/pool_classifier_web/patterns.yaml`. Use `--preview-run` to
choose a run rather than the newest terminal one, or `--base-ref origin/main`
to compare with another committed baseline.

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | Postgres DSN | required |
| `TC_TOKEN_FILE` | Taskcluster credential file | `~/.tc_token` |
| `TC_TOKEN_JSON` | Inline Taskcluster credential | unset |
| `TC_ROOT_URL` | Taskcluster root URL | `firefox-ci-tc` |
| `POOLS_FILE` / `PATTERNS_FILE` | Registry overrides | package-relative |
| `INCLUDE_VMS_POOLS` | Include `-vms` pools in automatic classification | unset |
| `CLASSIFY_OIDC_AUDIENCE` | Require OIDC on classify endpoints | unset |
| `CLASSIFY_OIDC_SA_EMAIL` | Expected caller email claim | unset |
| `ADMIN_IAP_BYPASS` | Local-only admin IAP bypass | `1` from `pc_start.sh` |
| `LOG_JSON` | Emit structured logs | unset |

## Local tools

Capture a reusable page-performance baseline with the local service running:

```sh
uv run --frozen scripts/benchmark_web_pages.py --runs 3 \
  --output /private/tmp/pool-classifier-before.json
```

Use `--base-url` and `--pool provisioner/worker-type` to select another listener
or representative pool. Dashboard snapshots are built after successful scans;
fixed dashboard views can use the last complete snapshot while live API ranges
continue to be queried directly.
