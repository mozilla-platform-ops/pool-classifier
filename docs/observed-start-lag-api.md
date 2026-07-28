# Observed scheduled-to-start lag API

`GET /api/v1/pools/{provisioner}/{worker_type}/observed-start-lag` accepts ISO
8601 `start` and `end` query parameters and an optional positive `slo_seconds`
parameter. If omitted, the SLO is configurable with
`OBSERVED_START_LAG_SLO_SECONDS` (default: 300 seconds).

The result reports a bounded sample count, nearest-rank p50/p95, and the share
of samples that started within the selected SLO. A sample is included only when
the raw Queue `runs[].scheduled` and `runs[].started` timestamps are both
present, ordered, and `scheduled` falls within the requested window.

This is **observed scheduled-to-start lag**, not a total queued-task count,
drop/expiry rate, or pool-health verdict. It includes only task runs that
started and were later observed terminal by per-worker polling; jobs that never
ran are invisible. `scheduled` is retained as the raw Queue scheduled timestamp
and is not relabeled as a general task-readiness timestamp.
