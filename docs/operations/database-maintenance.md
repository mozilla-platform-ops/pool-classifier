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
