# Pool Classifier

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

To inspect current detail-page code without waiting for a classifier to replace
a cached dashboard snapshot, start a separate local listener with snapshots
disabled:

```sh
PC_PORT=8081 POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS=1 ./pc_start.sh
```

## Documentation

### Development

- [Local setup, configuration, tests, and local tools](docs/development/local-setup.md)

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
