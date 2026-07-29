# Public API

Pool Classifier provides versioned, read-only JSON endpoints under `/api/v1`.
Every successful response includes `api_version: 1`. Unknown pools return:

```json
{"error": {"code": "not_found", "message": "pool not found"}}
```

## Discovery

`GET /api/v1` lists the available v1 endpoint paths. `GET /api/v1/pools`
returns every configured pool, including disabled pools. Each item has its
stable ID, provisioner, worker type, inferred OS, enabled state and reason,
schedule, and availability mode.

## Pool summary

`GET /api/v1/pools/{provisioner}/{worker_type}/summary` returns a dashboard
health snapshot. Its `pool` object is the same configuration shape used by the
pool-discovery endpoint. `metrics` includes total worker, alerting-worker, and
terminal-run counts plus success/error totals and trailing 1-hour and 24-hour
success rates. Rates are `null` when their window has no terminal runs.

`coverage.task_runs` and `coverage.worker_availability` independently report
their earliest successful collection time (`started_at`) and newest successful
collection boundary (`through`). `freshness.collected_at` is the newest
successful observation from either source. `freshness.stale` becomes `true`
when that observation is over one hour old; it is `null` until collection
begins. A configured pool with no collected data returns HTTP 200 with zero
metrics and null coverage/freshness fields.

`availability_mode` describes the capacity semantics: `recent_contact` counts
workers recently active in Taskcluster, while `listed` counts non-quarantined
workers returned by a complete Taskcluster listing. See
[`utilization-api.md`](utilization-api.md) for the full availability and
collection-coverage definitions.

## Existing endpoints

The discovery document also lists the utilization and observed-start-lag
endpoints. Their request parameters and response schemas remain documented in
[`utilization-api.md`](utilization-api.md) and
[`observed-start-lag-api.md`](observed-start-lag-api.md).
