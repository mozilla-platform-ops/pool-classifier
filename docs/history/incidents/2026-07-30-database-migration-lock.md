# Incident: Cloud Run rollout and database migration lock

Date: 2026-07-30 PDT / 2026-07-31 UTC

## Summary

The `v1.1.3`/`v1.1.4` rollout caused the Pool Classifier dashboard to appear
stuck after IAP authentication. Cloud Run itself remained available, but a
startup database migration held locks on `task_results` while backfilling
historical rows. Dashboard queries queued behind that lock and the browser
spun until requests were cancelled.

Production was restored to the known-good image from commit `2acc749` using the
`pool-classifier-rollback` revision. The service is currently routed 100% to
that revision.

## Timeline

- `v1.1.3` was tagged and published. Cloud Build built and pushed the image,
  but the Cloud Run revision failed its startup probe.
- Startup logs showed concurrent migration attempts and `DuplicateColumn`
  errors while applying migration `002`/`006`.
- `v1.1.4` added a PostgreSQL advisory lock to serialize migrations. This fixed
  the race, but kept all migrations in one transaction.
- Migration `007_observed_task_runs` then ran:

  ```sql
  UPDATE task_results
  SET observed_at = classified_at,
      last_checked_at = classified_at
  WHERE observed_at IS NULL OR last_checked_at IS NULL;
  ```

- Because the transaction also contained `ALTER TABLE`, the migration held a
  relation lock on `task_results` for the duration of the historical update.
- The dashboard's global aggregate queries blocked behind that lock. Cloud Run
  showed failed/unready revisions, while the load balancer logged requests
  that waited and were eventually disconnected.
- A temporary in-VPC Cloud Run diagnostic job found the migration backend
  (`pid 2666391`) running for roughly 27 minutes, along with many blocked
  dashboard queries. The 27 minutes was backend elapsed time, not a measurement
  of uninterrupted row-processing time; lock waits and storage I/O may have
  contributed.
- `pg_terminate_backend(2666391)` returned `true`, rolling back the incomplete
  migration and releasing the lock.
- A fresh rollback revision based on `2acc749` was created and given 100% of
  traffic. Cloud Run and the application returned to healthy status.
- A later diagnostic count found `1,066,825` rows in `task_results`. Because
  migration `007` adds both timestamp columns immediately before the update,
  approximately all 1.07 million existing rows would have required backfilling.
- The failed transaction rolled back completely. Production therefore does not
  currently have the `observed_at` or `last_checked_at` columns.

## Root cause

The rollout exposed two distinct migration failures:

1. In `v1.1.3`, multiple Cloud Run instances attempted startup migrations
   concurrently. Their schema changes raced and produced `DuplicateColumn`
   errors.
2. In `v1.1.4`, the PostgreSQL advisory lock serialized those migration
   attempts, fixing the race. However, all schema changes and the unbounded
   historical backfill still ran in one transaction. This made startup time
   proportional to the production data size and held a relation lock on
   `task_results` while the backfill processed approximately 1.07 million
   rows. Cloud Run startup probes and normal dashboard reads could not complete
   while the migration was running.

The advisory lock addressed concurrent migration races, but did not make a
long-running backfill appropriate for startup.

## Remediation

- Keep production traffic on the known-good `pool-classifier-rollback`
  revision until a corrected release is verified.
- Remove the historical `UPDATE` from startup migrations. Migrations should
  perform only bounded, fast schema work.
- Run the historical backfill separately as a resumable, batched operational
  job with progress and error reporting. Trigger and monitor it independently
  of the web service rollout.
- Do not hold a transaction-scoped schema lock while processing historical
  data.
- Set conservative `lock_timeout` and `statement_timeout` values for deploy
  migrations so a blocked or unexpectedly expensive migration fails visibly
  instead of making the application unavailable indefinitely.
- Treat startup migrations as schema-only. Require explicit review and a
  separate execution path for historical data changes and heavyweight index
  creation. In particular, use a one-shot maintenance job and `CREATE INDEX
  CONCURRENTLY` rather than creating a large index in a web-service startup
  transaction.

## Observability improvement

PostgreSQL currently reports blank `application_name` values for application
connections, so `pg_stat_activity` cannot identify which Cloud Run instance is
responsible for a slow query.

Set `application_name` on every PostgreSQL connection to include the revision
and instance identity, for example:

```text
pool-classifier/<K_REVISION>/<HOSTNAME>
```

`K_REVISION` identifies the Cloud Run revision and `HOSTNAME` identifies the
instance. With this in place, lock and query diagnostics can map each backend
PID directly to a revision/instance and distinguish migration traffic from
dashboard traffic.

## Follow-up beads

- `pool-classifier-fuo` (P0 bug): Make migration 007 safe and restore the
  production rollout.
- `pool-classifier-zyw` (P1 task): Add a resumable batched backfill for task
  observation timestamps. This is blocked by `pool-classifier-fuo`.
- `pool-classifier-pyo` (P1 task): Separate production schema migrations from
  web-service startup.
- `pool-classifier-my1` (P2 task): Identify PostgreSQL connections by revision,
  instance, and workload.

Migration-policy documentation and regression safeguards are included in
`pool-classifier-fuo` and `pool-classifier-pyo`.

## Current state

- Cloud Run service: `Ready=True`
- Traffic: 100% to `pool-classifier-rollback`
- Rollback image source: commit `2acc749`
- `v1.1.4` image was built, but its deployment was not completed successfully
- Migration `007` was rolled back; production does not have its new timestamp
  columns
- The `pool-classifier-db-diagnose` Cloud Run job remains available for
  read-only production diagnostics
