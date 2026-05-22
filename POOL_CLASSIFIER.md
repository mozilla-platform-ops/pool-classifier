# Pool Classifier

Multi-pool Taskcluster worker health classifier with a Flask dashboard. Each pool gets periodically polled, recent task results are pulled from TC, and log output is matched against [`patterns.yaml`](worker_health/pool_classifier_web/patterns.yaml) to bucket failures by category and severity.

For the Cloud Run migration plan and history, see [`PC_CLOUD_RUN_MIGRATION.md`](PC_CLOUD_RUN_MIGRATION.md).

## Layout

```
worker_health/
  pool_classifier.py                  # core classifier (TC polling + matching)
  pool_classifier_web/
    app.py                            # Flask app factory
    auth.py                           # OIDC validation for /classify/*
    registry.py                       # pools.yaml loader
    pools.yaml                        # pool registry
    patterns_registry.py              # patterns.yaml loader
    patterns.yaml                     # classification rules
    storage.py                        # SqliteStorage + PostgresStorage
    templates/index.html              # multi-pool dashboard
    scripts/migrate.py                # SQL migration runner
    migrations/001_init.sql           # Postgres schema
    docker-compose.yml                # local postgres + migrate
    terraform/                        # GCP infra (Phase 4)
  tests/
    test_web_app.py                   # Flask integration tests
    test_postgres_storage.py          # storage parity tests
pc_start.sh                           # convenience: run the flask app
pc_fetch_data.sh                      # convenience: POST /classify/* for all pools
```

## Local dev

### One-time setup

```sh
pipenv install --dev
```

Make sure you have a Taskcluster token at `~/.tc_token` (JSON with `clientId` and `accessToken`).

### Start Postgres + apply migrations

```sh
# Start postgres (port 5433 on localhost, data persisted to ./pgdata)
docker compose -f worker_health/pool_classifier_web/docker-compose.yml up -d postgres

# Apply migrations
docker compose -f worker_health/pool_classifier_web/docker-compose.yml run --rm migrate
```

To stop:

```sh
docker compose -f worker_health/pool_classifier_web/docker-compose.yml down
```

`./pgdata/` is a bind mount, so `docker compose down -v` will NOT destroy it. Delete the directory by hand if you want a fresh DB.

### Start the app

```sh
./pc_start.sh                  # serves on :8080
PC_PORT=8090 ./pc_start.sh     # override port
```

What this sets:
- `DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier` <!-- pragma: allowlist secret -->
- `TC_TOKEN_FILE=$HOME/.tc_token`

Then:
- Dashboard: <http://localhost:8080/>
- Per-pool: <http://localhost:8080/pools/proj-autophone/gecko-t-lambda-perf-a55>
- Health check: <http://localhost:8080/healthz>

### Trigger classify cycles

`/classify/*` runs the polling+classification loop for one pool and writes results.

```sh
# Single pool
curl -s -X POST localhost:8080/classify/proj-autophone/gecko-t-lambda-perf-a55 | jq .

# Every enabled pool (autophone first, then releng-hardware, shuffled within each phase)
bash pc_fetch_data.sh
```

OIDC validation is off locally (decorator no-ops when `CLASSIFY_OIDC_AUDIENCE` is unset). In production it requires a Cloud Scheduler-signed JWT.

### Tests

```sh
# Unit tests (no Postgres needed)
pipenv run pytest tests/ --ignore=tests/test_runner.py -x -q

# Postgres + web tests (need the docker-compose stack up)
export PC_TEST_DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier  # pragma: allowlist secret
pipenv run pytest tests/test_postgres_storage.py tests/test_web_app.py -v
```

## Configuration

### Pools (`pool_classifier_web/pools.yaml`)

Registry of pools the dashboard knows about. Disabled pools stay listed (greyed-out on the index) but are not classified. Each pool can override its cron schedule.

### Patterns (`pool_classifier_web/patterns.yaml`)

Failure classification rules. Each pattern has:
- `name` — category key written to `task_results.category`
- `regex` — Python regex matched against the task log
- `severity` — `critical` | `high` | `low` (drives heatmap colors)
- `tags` — informational only
- `description` — shown in the UI
- `enabled` — set `false` to mute without deleting

Match order: patterns are sorted critical → high → low, file order within a tier; first match wins. So a critical pattern always beats a high one, even if it appears later in the file.

### Environment variables

| var                       | purpose                                    | default                    |
|---------------------------|--------------------------------------------|----------------------------|
| `DATABASE_URL`            | Postgres DSN                                | required                   |
| `TC_TOKEN_FILE`           | Path to `{"clientId","accessToken"}` JSON   | `~/.tc_token`              |
| `TC_TOKEN_JSON`           | Inline TC token JSON (overrides file)       | unset                      |
| `TC_ROOT_URL`             | Taskcluster root URL                        | firefox-ci-tc              |
| `POOLS_FILE`              | Override path to `pools.yaml`               | package-relative           |
| `PATTERNS_FILE`           | Override path to `patterns.yaml`            | package-relative           |
| `CLASSIFY_OIDC_AUDIENCE`  | If set, require OIDC bearer on `/classify/*` | unset (off, local dev)     |
| `CLASSIFY_OIDC_SA_EMAIL`  | Expected `email` claim in the OIDC token    | unset (any caller passes)  |
| `LOG_JSON`                | Set `true` for structured logs              | unset                      |
