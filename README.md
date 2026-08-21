# Pool Classifier

[![CI](https://github.com/mozilla-platform-ops/pool-classifier/actions/workflows/ci.yml/badge.svg)](https://github.com/mozilla-platform-ops/pool-classifier/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mozilla-platform-ops/pool-classifier/graph/badge.svg)](https://codecov.io/gh/mozilla-platform-ops/pool-classifier)
[![Python 3.14+](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![Code style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Pool Classifier is a Cloud Run service and Flask dashboard for monitoring
Taskcluster worker pools. It periodically classifies recent task results,
matches task logs against failure patterns, and surfaces pool health, alerting
workers, success rates, and unclassified failures.

The Python package remains named `worker_health` for compatibility after the
repository extraction. Those package-path references are vestigial.

## Quick start

```sh
uv sync --group dev
./pc_db.sh init
./pc_start.sh
```

The local dashboard is available at <http://localhost:8080/>. A Taskcluster
token is required at `~/.tc_token`; see the development guide for its format
and the complete local workflow.

To launch the production-style Gunicorn server directly, use:

```sh
uv run pool-classifier
PORT=8090 uv run pool-classifier
```

It binds to `0.0.0.0:8080` by default. `PORT`, `POOL_CLASSIFIER_HOST`,
`GUNICORN_WORKERS`, `GUNICORN_THREADS`, and `GUNICORN_TIMEOUT` configure the
same defaults as the Cloud Run container; command-line options take precedence.

### Preview a task classification

Use the read-only `task-classifier` command to compare the current working-tree
patterns with patterns at a Git ref for one Taskcluster task:

```sh
uv run task-classifier TASK_ID
uv run task-classifier TASK_ID --run 0 --base-ref origin/main
```

`TASK_ID` is required. `--run` selects a terminal run (the newest terminal run
is the default); `--base-ref` selects the baseline pattern revision (default:
`HEAD`). `--provisioner`, `--worker-type`, and `--poll-interval` use the same
defaults as the polling classifier, and `--no-color` disables terminal colors.
The command only fetches task data and prints a comparison; it does not update
classification storage or patterns.

To inspect current detail-page code without waiting for a classifier to replace
a cached dashboard snapshot, start the port-8081 debug listener with snapshots
disabled. `pc_start.sh` enables Flask debug mode automatically on port 8081;
the default port 8080 remains the stable target for `pc_fetch_data.sh`:

```sh
PC_PORT=8081 POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS=1 ./pc_start.sh
```

## Documentation

### Development

- [Local setup, configuration, tests, and local tools](docs/development/local-setup.md)

Run the PostgreSQL-backed suite with a terminal coverage report locally:

```sh
scripts/run_local_postgres_tests.sh --cov=worker_health --cov-report=term-missing
```

CI enforces 65% overall coverage, publishes `coverage.xml` as a workflow
artifact, and reports coverage to Codecov.

### Operations

- [Cloud Run runbook](docs/operations/cloud-run.md)
- [Release and ad-hoc deployment procedure](docs/operations/deployments.md)
- [Production database maintenance](docs/operations/database-maintenance.md)
- [Backfill observed start-lag metadata](docs/operations/backfill-observed-start-lag.md)
- [Backfill recent job-source metadata](docs/operations/backfill-observed-start-lag.md#job-sources)

### API reference

- [Public API](docs/reference/public-api.md)
- [Utilization API](docs/reference/utilization-api.md)
- [Observed scheduled-to-start lag API](docs/reference/observed-start-lag-api.md)

### Design and history

- [Utilization and queue lag for pool sizing](docs/design/utilization-and-queue-lag.md)
- [Fitness and Pool Classifier comparison](docs/design/fitness-comparison.md)
- [Cloud Run migration history](docs/history/cloud-run-migration.md)
- [Dashboard query refactor](docs/history/dashboard-query-refactor.md)
- [2026-07-30 database migration-lock incident](docs/history/incidents/2026-07-30-database-migration-lock.md)

## Repository layout

```text
worker_health/
  pool_classifier.py                  # Taskcluster polling and classification
  pool_classifier_web/                # Flask app, storage, migrations, and Terraform
tests/                                # pytest suite
scripts/                              # test, build, benchmark, and maintenance helpers
```
