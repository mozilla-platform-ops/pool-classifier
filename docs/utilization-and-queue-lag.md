# Utilization and Queue Lag for Pool Sizing

Pool sizing should not be decided from one number. Queue lag and utilization
answer different questions, and the difference matters particularly for device
pools with wake-on-dispatch workers.

## The two signals

**Queue lag** is the time from when a task is eligible for a pool to when a
worker starts it. It measures the experience of task submitters and is the
best leading signal for whether capacity is keeping up with demand.

**Utilization** is busy worker time divided by available worker time. It helps
explain sustained saturation and identify obvious unused capacity. It is not a
direct measure of whether work waited.

For queue lag to be useful, its start must be defined carefully. It should
begin when work is genuinely eligible for the pool, not merely when a task is
created or while it is blocked by dependencies, priority, routing, or another
capability requirement. Report percentiles (especially p50 and p95) and the
share of tasks meeting a queue-lag SLO; avoid using only an average.

## How to interpret them together

| Queue lag | Utilization | Likely interpretation |
| --- | --- | --- |
| High | High | Strong capacity-shortage evidence. Add capacity or reduce demand. |
| High | Low | Investigate routing, capability mismatch, cold starts, quarantines, scheduler behavior, or unavailable devices. The whole pool may not be undersized. |
| Low | Moderate | Usually healthy headroom. |
| Low | Low | Possible excess capacity, but validate the denominator and workload mix before resizing. |

A low lag with high utilization can still be acceptable when the queue drains
quickly, though it leaves less resilience for bursts and failures. Conversely,
high lag with low aggregate utilization often means that the constrained unit is
a subset of the pool, not total worker count.

## What the current utilization API measures

The current API is duration-weighted and never averages bucket percentages:

```text
utilization_pct = busy_worker_seconds / available_worker_seconds * 100
```

It only publishes an authoritative percentage for a window with complete
task-run and availability coverage. This is intentional: partial collection
must not be presented as a sizing result.

There is an important freshness limitation. The current numerator is built
from terminal task runs with both `run_started` and `run_resolved` recorded.
An in-progress task is not counted until it finishes. As a result, the metric
can understate near-real-time occupancy, especially for long-running tests.
It should therefore be described as **observed terminal-run occupancy**, not
as a complete live measure of every device's current activity.

For `listed` availability pools, the denominator includes every non-quarantined
worker returned by Taskcluster. Listing establishes eligibility, not physical
device liveness: a dormant or unhealthy device may remain listed. This can also
make utilization look lower than usable capacity would imply.

## Presentation guidance

Do not use utilization alone as a public headline or a verdict that a pool is
over-provisioned. Present it with:

- queue-lag percentiles and an explicit SLO;
- coverage and data freshness;
- the availability mode and the listed-mode liveness limitation where relevant;
- throughput or completed-job counts for workload context; and
- a concise scope label, such as “observed terminal-run occupancy.”

This framing makes the metric useful without implying that an idle denominator
is necessarily healthy, available hardware.

## Future improvement

A truer live occupancy metric would persist active run starts as soon as they
are observed, count them through the shared coverage boundary, and close them
when terminal status arrives. That would make utilization and queue lag more
directly comparable while retaining the coverage guarantees of the current
API.
