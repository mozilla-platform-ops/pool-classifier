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

The discovery document also lists the utilization, observed-start-lag, and capacity-scenarios
endpoints. Their request parameters and response schemas remain documented in
[`utilization-api.md`](utilization-api.md),
[`observed-start-lag-api.md`](observed-start-lag-api.md), and
[`capacity-scenarios-api.md`](capacity-scenarios-api.md).

## Failures

`GET /api/v1/pools/{provisioner}/{worker_type}/failures` requires timezone-aware
ISO 8601 `start` and `end` parameters. The interval is start-inclusive and
end-exclusive, and may be at most 90 days. It returns terminal non-successful
runs (`failed`, `exception`, and `expired`) grouped by classification category.
Failures with no category are reported as `unclassified`. An optional exact
`category` filter narrows the returned group.

## Workers

`GET /api/v1/pools/{provisioner}/{worker_type}/workers` returns worker IDs,
activity timestamps, lifetime success/failure counters, consecutive failures,
current availability and quarantine state, and the top failure category in the
selected time window. The default window is the trailing 24 hours; callers can
supply the same bounded `start` and `end` interval as the failures endpoint.

Optional filters are `quarantined=true|false`, `alerting=true|false`, and an
exact failure `category`. Results are ordered by alerting workers first and
then worker ID. They use opaque cursor pagination: `limit` defaults to 50 and
is at most 200; pass `pagination.next_cursor` as `cursor` to obtain the next
page. Empty pages return HTTP 200 with an empty `workers` list.

## Patterns

`GET /api/v1/patterns` returns configured classification-pattern metadata in
registry order: name, severity, tags, description, and enabled state. It
includes disabled patterns so consumers can inspect the complete configured
registry; it intentionally does not return pattern regular expressions.
