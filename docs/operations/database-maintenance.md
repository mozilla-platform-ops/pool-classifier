# Production database maintenance

Startup migrations are limited to fast, bounded schema changes. Historical
updates and heavyweight index work must use the separately triggered,
reviewed maintenance path—not the migration job.

Point the maintenance job at the reviewed image, execute one allowlisted
operation, and inspect its structured Cloud Logging output before treating the
operation as complete:

```sh
IMAGE=us-west1-docker.pkg.dev/relops-pool-classifier/pool-classifier/app:vVERSION
gcloud run jobs update pool-classifier-db-maintenance \
  --image="$IMAGE" --region=us-west1 --project=relops-pool-classifier
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,create-unresolved-task-run-index" \
  --wait --region=us-west1 --project=relops-pool-classifier
```

The job has no default operation: execution without `--args` fails visibly.

## Migration policy and associated backfills

Migrations are applied automatically at application startup and are limited to
fast schema changes. Check the `schema_migrations` table or deployment logs to
confirm a migration is recorded before running any associated maintenance job.
Never add a historical table update to a startup migration: provide a bounded,
restart-safe maintenance operation instead.

| Migration / data | Required follow-up | Where to run it |
| --- | --- | --- |
| 007 `observed_task_runs` | Backfill legacy `observed_at` and `last_checked_at` values when historical timestamp coverage is needed. | [Timestamp backfill](#timestamp-backfill) |
| 006 `observed_start_lag` | Optional enrichment of recent Queue `scheduled` metadata for dashboard start-lag data. | [Observed start-lag backfill](backfill-observed-start-lag.md) |
| 010 `task_sources` | Optional enrichment of recent task-source labels for the job-source chart; the migration itself is schema-only. | [Job-source backfill](#job-source-backfill) |

## Timestamp backfill

After migration 007 is recorded, backfill legacy `task_results` timestamp rows
in paced batches. The operation commits each batch; a stopped execution safely
restarts and skips rows already populated.

```sh
# Inspect remaining rows first.
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,backfill-m007-task-timestamps,--count-only" \
  --wait --region=us-west1 --project=relops-pool-classifier

# Run a bounded, paced backfill.
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,backfill-m007-task-timestamps,--batch-size,1000,--batch-delay-seconds,0.2,--retries,3" \
  --wait --region=us-west1 --project=relops-pool-classifier
```

Use `--dry-run` to inspect the first batch or `--max-batches=N` to bound an
execution. Finish with `--count-only` and confirm zero rows remain.

## Job-source backfill

Migration 010 creates `task_sources`; it deliberately does not fetch or infer
historical data. To enrich already-stored task runs, use the reviewed
`backfill-job-sources` operation. It defaults to the preceding 14 days and
requires an explicit `--lookback-days` increase for a wider historical window.
Its default batch uses 12 concurrent Taskcluster fetches, paced to 8 requests
per second across the batch; both limits can be overridden with `--concurrency`
and `--requests-per-second`.
It stores only the compact derived source/method, never the Taskcluster task
definition, and successful batches are idempotent.

```sh
# Inspect the bounded scope without contacting Taskcluster or changing data.
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,backfill-job-sources,--count-only" \
  --wait --region=us-west1 --project=relops-pool-classifier

# Backfill the default 14-day window.
gcloud run jobs execute pool-classifier-db-maintenance \
  --args="-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,backfill-job-sources,--lookback-days,14" \
  --wait --region=us-west1 --project=relops-pool-classifier
```

The operation reports selected, fetched, classified, unknown, and error task
counts for every pool. Re-run after an error: unsuccessful fetches remain
unrecorded and therefore eligible for the next bounded execution. Deterministic
Taskcluster 400/404 responses are recorded as `unknown` rather than retried,
so legacy rows with invalid or expired task IDs do not block a pool.

A single Ctrl-C lets the active batch commit, then stops cleanly with exit
status 130 and a `db_maintenance_stopped` log event. Use a second Ctrl-C only
to abort immediately.

## Datastore summary

`datastore-summary` is read-only. It reports PostgreSQL settings, table sizes,
index statistics, timestamp-backfill completeness, and grouped connection
activity without logging connection strings or query text.

```sh
DATABASE_URL=postgresql://pc:pc@localhost:5433/pool_classifier \
  uv run -m worker_health.pool_classifier_web.scripts.db_maintenance \
  --operation datastore-summary
```

For production, update and execute `pool-classifier-db-diagnose` with the
reviewed image, then inspect its JSON log record.

## Utilization query-plan capture

Use `utilization-task-run-query-plan` for the exact overlap query before adding
an index. It accepts a timezone-qualified window no longer than seven days.
Without `--analyze` it obtains a non-executing JSON plan; `--analyze` runs the
read-only query under a statement timeout.

```sh
DATABASE_URL=postgresql://pc:pc@localhost:5433/pool_classifier \
  uv run -m worker_health.pool_classifier_web.scripts.db_maintenance \
  --operation utilization-task-run-query-plan \
  --pool-id releng-hardware/gecko-t-osx-1500-m4 \
  --start 2026-07-31T00:00:00Z --end 2026-07-31T01:00:00Z --analyze
```

The measured index for recent windows is the partial covering
`(pool_id, run_resolved)` index. Create it only through the reviewed
`create-utilization-task-run-index` maintenance operation, then capture a new
plan to verify the access path.
