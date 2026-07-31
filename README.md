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
cloudbuild.yaml                       # build and push release images
docker-entrypoint.sh                  # gunicorn startup
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
uv sync --group dev
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
- Public API: see [docs/public-api.md](docs/public-api.md)
- Utilization API: see [docs/utilization-api.md](docs/utilization-api.md)

The dashboard and pool pages identify pools using `listed` availability. For
those wake-on-dispatch pools, listed and non-quarantined workers count as
eligible capacity, but Taskcluster listing does not confirm device liveness.

Trigger classify cycles:

```sh
# Single pool
curl -s -X POST localhost:8080/classify/proj-autophone/gecko-t-lambda-perf-a55 | jq .

# Every enabled non-VM pool (`-vms` worker types are skipped by default)
bash pc_fetch_data.sh

# Intentionally include VM pools too
INCLUDE_VMS_POOLS=1 bash pc_fetch_data.sh
```

Query duration-weighted utilization:

```sh
curl -sG localhost:8080/api/v1/pools/proj-autophone/gecko-t-lambda-perf-a55/utilization \
  --data-urlencode 'start=2026-07-21T10:00:00Z' \
  --data-urlencode 'end=2026-07-21T12:00:00Z' \
  --data-urlencode 'bucket_seconds=3600' | jq .
```

### Local page benchmarks

With the local service running, capture a reusable baseline for the overview
and a representative detail page. The first run reflects the current cache
state; later runs capture warm behavior. Save the JSON before a change and
compare its per-request `median_ms` and `p95_ms` summaries afterward.

```sh
uv run --frozen scripts/benchmark_web_pages.py --runs 3 \
  --output /private/tmp/pool-classifier-before.json
```

Use `--base-url` and `--pool provisioner/worker-type` to target a different
local listener or representative pool.

## Tests

```sh
# Unit and web tests that do not require local Postgres
uv run --frozen --group dev pytest tests/ --ignore=tests/test_runner.py -x -q

# Full suite, including Postgres-backed tests
scripts/run_local_postgres_tests.sh
```

## Build and deploy

Build the release image from the repository root. This command does not change
production traffic:

```sh
gcloud builds submit --config cloudbuild.yaml \
  --substitutions=_TAG=vVERSION,COMMIT_SHA=$(git rev-parse "vVERSION^{commit}") \
  --project=relops-pool-classifier .
```

After Terraform has created the two jobs, run the manual production gate with
the same immutable release image. A failed command stops here; do not deploy or
promote traffic after a failure.

```sh
IMAGE=us-west1-docker.pkg.dev/relops-pool-classifier/pool-classifier/app:vVERSION

# Schema migrations run before any candidate web revision exists.
gcloud run jobs update pool-classifier-migrate \
  --image="$IMAGE" --region=us-west1 --project=relops-pool-classifier
gcloud run jobs execute pool-classifier-migrate \
  --wait --region=us-west1 --project=relops-pool-classifier

# Start a candidate only after the migration job succeeds; retain current traffic.
gcloud run deploy pool-classifier --image="$IMAGE" --no-traffic \
  --region=us-west1 --project=relops-pool-classifier

# Promote only after the candidate reports Ready=True and log inspection passes.
# With --no-traffic, Cloud Run can label the candidate revision "Retired" and
# keep latestReadyRevisionName pointing at the traffic-serving revision. This
# is expected while the candidate has 0% traffic: inspect that candidate's own
# Ready condition and image digest, rather than treating "Retired" alone as a
# failed deployment. It becomes active when traffic is promoted.
gcloud run services update-traffic pool-classifier --to-latest \
  --region=us-west1 --project=relops-pool-classifier
```

## Production database maintenance

Startup migrations are restricted to fast, bounded schema changes. Historical
data updates and heavyweight index work must use an explicitly reviewed,
separately triggered maintenance path. In particular, migration 007 adds its
timestamp columns without backfilling historical rows; the observation-timestamp
backfill is a separate operational task.

Use the maintenance job only for explicitly reviewed index work and operational
backfills; it is not the migration job. Terraform creates this durable runner
once. Each execution selects an allowlisted operation from the release image,
so normal migrations and later maintenance operations do not require Terraform
changes. For example, after the migration job has recorded migration 007, point
the maintenance job at the release image and execute the concurrent-index
operation manually:

```sh
IMAGE=us-west1-docker.pkg.dev/relops-pool-classifier/pool-classifier/app:vVERSION
gcloud run jobs update pool-classifier-db-maintenance \
  --image="$IMAGE" --region=us-west1 --project=relops-pool-classifier
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,create-unresolved-task-run-index" \
  --wait --region=us-west1 --project=relops-pool-classifier
```

The `create-unresolved-task-run-index` operation runs `CREATE INDEX CONCURRENTLY` for
`idx_task_results_unresolved` outside a transaction and fails if migration 007
is absent or the resulting index is invalid. The job has no default operation:
executing it without `--args` fails visibly. Inspect its structured Cloud
Logging output before treating the release as complete. This remains a manual
release step until the dedicated migration-deployment work is implemented.

### Migration 007 timestamp backfill

After migration 007 is recorded, use the same maintenance job to backfill
legacy `task_results` rows. This operation has no state file or durable cursor:
within one execution it advances an in-memory primary-key cursor, while every
selected batch still requires `observed_at` or `last_checked_at` to be NULL.
It updates only those NULL values from `classified_at` and commits before
continuing. A stopped execution starts from the beginning, skips rows already
filled by previous executions, and is therefore safe to rerun.

First inspect the exact remaining count:

```sh
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,backfill-m007-task-timestamps,--count-only" \
  --wait --region=us-west1 --project=relops-pool-classifier
```

Then run paced batches. The job logs JSON start, progress, retry, and completion
events; the final completion event reports the verified remaining count. A
failed or timed-out execution can be rerun with the same command.

```sh
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,backfill-m007-task-timestamps,--batch-size,1000,--batch-delay-seconds,0.2,--retries,3" \
  --wait --region=us-west1 --project=relops-pool-classifier
```

Use `--dry-run` to report the first batch's size without updating rows, or
`--max-batches=N` for a deliberately bounded maintenance execution. Finish by
rerunning `--count-only` and confirming it reports zero rows remaining.

### Datastore summary

The `datastore-summary` operation is read-only and emits one structured JSON
record for comparing a local database with production. It includes PostgreSQL
and autovacuum settings, core-table live/dead-row estimates and sizes,
`task_results` index statistics, timestamp-backfill completeness, and grouped
connection activity. It never emits connection strings or query text; each
query has a 10-second statement timeout.

Locally, run it against the same database used by the web app:

```sh
DATABASE_URL=postgresql://pc:pc@localhost:5433/pool_classifier \
  uv run -m worker_health.pool_classifier_web.scripts.db_maintenance \
  --operation datastore-summary
```

For production, first update the maintenance job to the desired release image,
then execute the same allowlisted operation and inspect its JSON log record:

```sh
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,datastore-summary" \
  --wait --region=us-west1 --project=relops-pool-classifier
```

### Utilization task-run query plan

Use the `utilization-task-run-query-plan` operation to inspect the exact
`task_results` overlap query used by utilization endpoints before choosing an
index. It accepts only a timezone-qualified window of seven days or less. By
default it asks PostgreSQL for a non-executing JSON plan. Pass `--analyze` only
when collecting measured read/buffer evidence; that executes the read-only
query under a 30-second statement timeout (configurable up to 120 seconds).

```sh
DATABASE_URL=postgresql://pc:pc@localhost:5433/pool_classifier \
  uv run -m worker_health.pool_classifier_web.scripts.db_maintenance \
  --operation utilization-task-run-query-plan --pool-id releng-hardware/gecko-t-osx-1500-m4 \
  --start 2026-07-31T00:00:00Z --end 2026-07-31T01:00:00Z --analyze
```

On production, point the maintenance job to the release image, then run the
same operation with an observed 1h or 24h window. Preserve the JSON output
from both environments when selecting or rejecting an index.

Infrastructure changes live under
`worker_health/pool_classifier_web/terraform/`:

```sh
cd worker_health/pool_classifier_web/terraform
terraform plan
terraform apply
```
