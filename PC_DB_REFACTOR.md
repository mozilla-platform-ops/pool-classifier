# Pool Classifier — Dashboard Query Refactor

Status: **implemented** — `pool_summaries_global()` in `storage.py`, index loop
rewritten in `app.py`, parity tests in `tests/test_postgres_storage.py`
(`test_pool_summaries_global_*`). Needs an image rebuild to deploy.

Goal: collapse the dashboard index page's per-pool query fan-out into a couple
of `GROUP BY pool_id` queries on a single connection. Wins on both **latency**
(cold-load slowness) and **DB connections** (the index is a major driver of the
connection exhaustion documented in `PC_CLOUD_RUN_MIGRATION.md`).

## Problem

The index handler (`app.py`, `index()` ~lines 92–142) loops over every enabled
pool and runs **7 queries per pool**:

| Storage call | Query (all scoped `WHERE pool_id = ?`) |
|---|---|
| `count_alerting(thr)` | `COUNT(*) FROM workers WHERE consecutive_failures >= ?` |
| `count_workers()` | `COUNT(*) FROM workers` |
| `oldest_classified_at()` | `MIN(classified_at) FROM task_results` |
| `count_recent_errors(1h)` | `COUNT(*) FROM task_results WHERE run_state IN ('failed','exception') AND COALESCE(run_resolved, classified_at) >= ?` |
| `count_recent_successes(1h)` | `COUNT(*) FROM task_results WHERE run_state='completed' AND COALESCE(run_resolved, classified_at) >= ?` |
| `count_recent_errors(24h)` | same, 24h window |
| `count_recent_successes(24h)` | same, 24h window |

With ~38 enabled pools that's **~266 sequential round-trips per page load**.

Worse, it's not just query count: each `_get_classifier(prov, wt)` on first touch
**opens a new persistent DB connection** and runs `_init_db()` (`init_schema` +
`get_seen_tasks`). So a cold index render **opens ~38 connections** and runs 38×
`get_seen_tasks`. That per-pool connection fan-out is a primary cause of the
`db-g1-small` connection exhaustion (`remaining connection slots are reserved
...`) — so this refactor also relieves connection pressure, not just latency.

(Source query bodies: `PostgresStorage` in `pool_classifier_web/storage.py` —
`count_alerting` ~526, `oldest_classified_at` ~709, `count_workers` ~758,
`count_recent_errors` ~763, `count_recent_successes` ~772.)

## Proposed fix — 2 grouped queries, 1 connection

Every summary query is a simple aggregate over `workers` or `task_results`
scoped by `pool_id`. Drop the per-pool filter, `GROUP BY pool_id`, and fold the
windowed counts with Postgres `FILTER`:

```sql
-- Query 1: workers → workers + alerting, all pools
SELECT pool_id,
       COUNT(*)                                              AS workers,
       COUNT(*) FILTER (WHERE consecutive_failures >= :thr)  AS alerting
FROM workers GROUP BY pool_id;

-- Query 2: task_results → oldest + 1h/24h error+success counts, all pools, one scan
SELECT pool_id,
       MIN(classified_at) AS oldest,
       COUNT(*) FILTER (WHERE run_state IN ('failed','exception') AND COALESCE(run_resolved, classified_at) >= :s1h)  AS err_1h,
       COUNT(*) FILTER (WHERE run_state = 'completed'            AND COALESCE(run_resolved, classified_at) >= :s1h)    AS ok_1h,
       COUNT(*) FILTER (WHERE run_state IN ('failed','exception') AND COALESCE(run_resolved, classified_at) >= :s24h) AS err_24h,
       COUNT(*) FILTER (WHERE run_state = 'completed'            AND COALESCE(run_resolved, classified_at) >= :s24h)   AS ok_24h
FROM task_results GROUP BY pool_id;
```

The handler looks up each pool from two dicts keyed by `pool_id`, and computes
`errors/host` and success-rate in Python exactly as today.

**~266 queries on ~38 connections → 2 queries on 1 connection.**

## Implementation shape

- **Precedent:** `count_category_hits_global(dsn, since)` (storage.py, used by
  `/patterns`) is already this pattern — a module-level function that opens one
  connection and runs a global `GROUP BY` query. Add a sibling, e.g.
  `pool_summaries_global(dsn, threshold, since_1h, since_24h) -> {pool_id: {...}}`.
- **Rewrite the index loop** to call it once and look each pool up; keep the
  `errors/host` + success-rate math in the handler.
- **No template change** — `index.html` keeps consuming the same `rows` shape.
- Per-pool page (`/pools/<prov>/<wt>`) keeps using a classifier (one
  connection) — unaffected.

## Correctness notes

- DB `pool_id` is `"{provisioner}/{worker_type}"` (see `_get_classifier` in
  app.py). Map each registry pool to that key for lookup.
- Pools with zero rows won't appear in the `GROUP BY` result → default to
  `0`/`None` (mirrors current behavior for empty pools).
- Disabled pools render as blank rows with no query (unchanged).

## Validation plan

- Add a parity test (against the local compose Postgres, gated on
  `PC_TEST_DATABASE_URL`) asserting the batched dict matches the old per-pool
  methods (`count_alerting`/`count_workers`/`oldest_classified_at`/
  `count_recent_errors`/`count_recent_successes`) for several seeded pools,
  including an empty pool.
- Confirm the index renders identically before redeploy.

## Follow-on (optional)

- Once the index no longer fans out connections, the `db-g1-small` connection
  tuning (`max_connections=100`, `GUNICORN_WORKERS=1`, `max_instances=2`) has
  more headroom and could be relaxed — but keep as safety margin unless needed.
- The persistent-connection-per-pool model in `_get_classifier`'s cache is still
  a latent fit issue for serverless + tiny DB; a connection pool or per-request
  connections would be the deeper fix if connection pressure recurs.
