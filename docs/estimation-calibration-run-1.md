# Estimation calibration run 1: move 10 Talos hosts from 1804 to 2404

## Purpose

Validate the capacity-scenarios model against a controlled, steady-state host
transfer. This run measures two independent effects:

- Removing 10 healthy hosts from `gecko-t-linux-talos-1804`.
- Adding 10 healthy hosts to `gecko-t-linux-talos-2404` after imaging and
  registration complete.

The imaging interval is intentionally not part of either estimate: the model
represents healthy, ready capacity and does not model reimage or readiness
time.

## Frozen pre-change prediction

Window: `2026-08-10T18:34:28Z` to `2026-08-17T18:34:28Z`.

| Pool | Host delta | Modeled p95 lag | Observed baseline p95 | 1-hour target | Coverage |
| --- | ---: | ---: | ---: | --- | --- |
| `gecko-t-linux-talos-1804` | -10 | 0s | 0.182s | pass | task runs 100.0%; availability 88.1% |
| `gecko-t-linux-talos-2404` | +10 | 1h 47m 10s | 2h 20m 49s | fail | task runs 88.1%; availability 87.8% |

Both primary estimates use the configured 120-second turnaround assumption.
2404's observed busy-worker-cycle median is about 125 seconds and produces a
very similar +10 prediction (1h 47m 38s). 1804 has no eligible busy-worker
cycles, so it has no observed turnaround alternative.

The model is uncalibrated. It excludes tasks that never started, and the
availability coverage is incomplete. Treat these predictions as a recorded
hypothesis, not a guarantee.

## Host inventory

- Host IDs: <!-- fill before quarantine -->
- Operator: <!-- fill -->
- Change reference: <!-- fill -->

## Procedure

1. Before changing capacity, save the source and target summary,
   observed-start-lag, and capacity-scenarios responses. Record the exact
   query windows used.
2. Drain active work if the quarantine mechanism does not already prevent new
   work while allowing active work to finish.
3. Quarantine all ten selected hosts in 1804. Record both the operator action
   time and the time Pool Classifier first observes all ten as quarantined.
   This is the 1804 capacity-removal event.
4. Keep the hosts quarantined/offline while they are reimaged. Do not include
   this interval when evaluating either steady-state estimate.
5. Register the hosts in 2404. Record each ready time and the time Pool
   Classifier first observes all ten as healthy, available 2404 workers. This
   is the 2404 capacity-addition event.
6. Monitor observed p95 lag and worker availability after each event. Use an
   early 24-hour safety check, then a seven-day primary comparison once the
   relevant pool has been stable for the whole window.
7. Compare observed p95 with the frozen prediction, record the error, and
   record whether the one-hour SLO classification was correct.

## Event log

| Event | UTC time | Notes |
| --- | --- | --- |
| Pre-change API snapshots captured | <!-- fill --> | |
| All 10 hosts quarantined in 1804 | <!-- fill --> | |
| Classifier observed 10 quarantined in 1804 | <!-- fill --> | |
| Imaging started | <!-- fill --> | |
| Imaging completed | <!-- fill --> | |
| All 10 hosts ready in 2404 | <!-- fill --> | |
| Classifier observed 10 available in 2404 | <!-- fill --> | |

## Results

### 24-hour safety check

| Pool | Window | Observed p95 | Within 1 hour? | Coverage | Notes |
| --- | --- | ---: | --- | --- | --- |
| 1804 | <!-- fill --> | | | | |
| 2404 | <!-- fill --> | | | | |

### Seven-day calibration comparison

| Pool | Forecast p95 | Observed p95 | Error (observed - forecast) | Correct 1-hour classification? | Task / availability coverage | Notes |
| --- | ---: | ---: | ---: | --- | --- | --- |
| 1804 (-10) | 0s | <!-- fill --> | <!-- fill --> | <!-- fill --> | <!-- fill --> | |
| 2404 (+10) | 1h 47m 10s | <!-- fill --> | <!-- fill --> | <!-- fill --> | <!-- fill --> | |

## Outcome

<!-- Summarize whether this run supports calibrating the model, what error was
observed, and whether a further controlled delta is needed. -->
