# Pool Classifier

Pool Classifier is a Cloud Run service and Flask dashboard for monitoring
Taskcluster worker pools. It periodically classifies recent task results,
matches task logs against failure patterns, and surfaces pool health, alerting
workers, success rates, and unclassified failures.

For the operator runbook, see [PC_CLOUD_OVERVIEW.md](PC_CLOUD_OVERVIEW.md).
For local development details, see [POOL_CLASSIFIER.md](POOL_CLASSIFIER.md).
For migration history, see
[PC_CLOUD_RUN_MIGRATION.md](PC_CLOUD_RUN_MIGRATION.md).

## Repository Layout

```text
worker_health/
  pool_classifier.py                  # core Taskcluster polling + classification
  pool_classifier_web/
    app.py                            # Flask app factory and routes
    auth.py                           # OIDC validation for /classify/*
    pools.yaml                        # pool registry
    patterns.yaml                     # failure classification rules
    storage.py                        # SQLite/Postgres storage implementations
    migrations/                       # Postgres schema migrations
    terraform/                        # Cloud Run, LB, SQL, Scheduler infra
tests/                                # pytest suite
Dockerfile                            # Cloud Run image
cloudbuild.yaml                       # build, push, deploy
docker-entrypoint.sh                  # migrations + gunicorn startup
pc_db.sh                              # local Postgres helper
pc_start.sh                           # local Flask helper
pc_fetch_data.sh                      # trigger classify for all enabled pools
```

The Python package is still named `worker_health` for compatibility after the
repo extraction. Any `worker_health` references in package paths are vestigial
and can be cleaned up once the standalone service has stabilized. Renaming it to
`pool_classifier` is tracked as follow-up work.

## Local Development

Install dependencies:

```sh
pipenv install --dev
```

Make sure a Taskcluster token exists at `~/.tc_token`:

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
```

Start the app:

```sh
./pc_start.sh
```

Useful local URLs:

- Dashboard: <http://localhost:8080/>
- Example pool:
  <http://localhost:8080/pools/proj-autophone/gecko-t-lambda-perf-a55>
- Health check: <http://localhost:8080/healthz>
- Utilization API: see [docs/utilization-api.md](docs/utilization-api.md)

The dashboard and pool pages identify pools using `listed` availability. For
those wake-on-dispatch pools, listed and non-quarantined workers count as
eligible capacity, but Taskcluster listing does not confirm device liveness.

Trigger classify cycles:

```sh
# Single pool
curl -s -X POST localhost:8080/classify/proj-autophone/gecko-t-lambda-perf-a55 | jq .

# Every enabled pool
bash pc_fetch_data.sh
```

Query duration-weighted utilization:

```sh
curl -sG localhost:8080/api/v1/pools/proj-autophone/gecko-t-lambda-perf-a55/utilization \
  --data-urlencode 'start=2026-07-21T10:00:00Z' \
  --data-urlencode 'end=2026-07-21T12:00:00Z' \
  --data-urlencode 'bucket_seconds=3600' | jq .
```

## Tests

```sh
# Unit and web tests that do not require local Postgres
pipenv run pytest tests/ --ignore=tests/test_runner.py -x -q

# Postgres-backed tests
./pc_db.sh init
export PC_TEST_DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier  # pragma: allowlist secret
pipenv run pytest tests/test_postgres_storage.py tests/test_web_app.py -v
```

## Deploy

Code deploys are built from the repository root:

```sh
gcloud builds submit --config cloudbuild.yaml \
  --substitutions=_TAG=$(git rev-parse --short HEAD) \
  --project=relops-pool-classifier .
```

Infrastructure changes live under
`worker_health/pool_classifier_web/terraform/`:

```sh
cd worker_health/pool_classifier_web/terraform
terraform plan
terraform apply
```
