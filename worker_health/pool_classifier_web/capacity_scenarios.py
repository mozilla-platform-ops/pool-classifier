"""Counterfactual capacity scenarios based on retained observed run data.

This is deliberately a bounded FIFO replay, not a Taskcluster queue simulator.
It only knows about runs that started and later became terminal, and models
healthy-worker capacity changed by a signed host delta.
"""

from __future__ import annotations

import heapq
from collections import deque
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Iterable


MODEL_VERSION = "fifo-observed-runs-v2"
MIN_BUSY_TURNAROUND_SAMPLES = 30
SCOPE = (
    "FIFO counterfactual replay of observed terminal task runs. It excludes "
    "tasks that never started and does not model Taskcluster routing, worker "
    "capabilities, retries, or wake-up time."
)


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    values.sort()
    return values[max(0, ceil(len(values) * fraction) - 1)]


def busy_turnaround_summary(runs: Iterable[dict], range_start: str, range_end: str) -> dict:
    """Summarize host turnaround only where the next observed run was already waiting."""
    start, end = _parse(range_start), _parse(range_end)
    by_worker: dict[str, list[tuple[datetime, datetime, datetime]]] = {}
    for row in runs:
        try:
            worker_id = str(row["worker_id"])
            scheduled, started, resolved = (_parse(row[key]) for key in ("scheduled", "started", "resolved"))
        except (AttributeError, KeyError, TypeError, ValueError):
            continue
        if start <= scheduled < end and scheduled <= started <= resolved:
            by_worker.setdefault(worker_id, []).append((scheduled, started, resolved))
    gaps = []
    for worker_runs in by_worker.values():
        worker_runs.sort(key=lambda item: item[1])
        for (_scheduled, _started, resolved), (next_scheduled, next_started, _next_resolved) in zip(worker_runs, worker_runs[1:]):
            if next_scheduled <= resolved <= next_started:
                gaps.append((next_started - resolved).total_seconds())
    return {
        "source": "busy_worker_cycles",
        "sample_count": len(gaps),
        "p50_seconds": _percentile(gaps, 0.50),
        "p90_seconds": _percentile(gaps, 0.90),
        "p95_seconds": _percentile(gaps, 0.95),
        "available": len(gaps) >= MIN_BUSY_TURNAROUND_SAMPLES,
        "minimum_samples": MIN_BUSY_TURNAROUND_SAMPLES,
    }


def _availability_by_time(
    transitions: Iterable[dict], start: datetime, end: datetime,
) -> list[tuple[datetime, int]]:
    """Return effective capacity changes using the same late-correction rules as utilization."""
    records = []
    for sequence, row in enumerate(transitions):
        try:
            effective_at = _parse(row["effective_at"])
            observed_at = _parse(row["observed_at"])
        except (KeyError, TypeError, ValueError):
            continue
        records.append((observed_at, int(row.get("id") or sequence), effective_at, row["worker_id"], bool(row["available"])))
    records.sort()
    boundaries = {start, end}
    boundaries.update(effective_at for _observed, _id, effective_at, _worker, _available in records if start < effective_at < end)
    result = []
    for boundary in sorted(boundaries):
        states: dict[str, bool] = {}
        for _observed, _id, effective_at, worker_id, available in records:
            if effective_at <= boundary:
                states[worker_id] = available
        capacity = sum(states.values())
        if not result or result[-1][1] != capacity:
            result.append((boundary, capacity))
    return result


def _scenario(
    arrivals: list[tuple[datetime, float]], capacity_changes: list[tuple[datetime, int]], host_delta: int,
    slo_seconds: int, turnaround_seconds: int,
) -> dict:
    queued: deque[tuple[datetime, float]] = deque()
    completions: list[datetime] = []
    lags: list[float] = []
    current_capacity = 0
    capacity_index = 0
    arrival_index = 0
    max_queue_depth = 0

    while arrival_index < len(arrivals) or queued or completions or capacity_index < len(capacity_changes):
        next_arrival = arrivals[arrival_index][0] if arrival_index < len(arrivals) else None
        next_capacity = capacity_changes[capacity_index][0] if capacity_index < len(capacity_changes) else None
        next_completion = completions[0] if completions else None
        times = [value for value in (next_arrival, next_capacity, next_completion) if value is not None]
        if not times:
            break
        now = min(times)
        while completions and completions[0] <= now:
            heapq.heappop(completions)
        while capacity_index < len(capacity_changes) and capacity_changes[capacity_index][0] <= now:
            current_capacity = max(0, capacity_changes[capacity_index][1] + host_delta)
            capacity_index += 1
        while arrival_index < len(arrivals) and arrivals[arrival_index][0] <= now:
            queued.append(arrivals[arrival_index])
            arrival_index += 1
        while queued and len(completions) < current_capacity:
            scheduled, duration = queued.popleft()
            lags.append((now - scheduled).total_seconds())
            heapq.heappush(completions, now + timedelta(seconds=duration + turnaround_seconds))
        max_queue_depth = max(max_queue_depth, len(queued))
        if queued and not completions and capacity_index >= len(capacity_changes):
            break

    p50, p95 = _percentile(lags, 0.50), _percentile(lags, 0.95)
    return {
        "host_delta": host_delta,
        "modeled_task_count": len(lags),
        "unstarted_task_count": len(arrivals) - len(lags),
        "modeled_p50_seconds": p50,
        "modeled_p95_seconds": p95,
        "started_within_target_pct": round(100 * sum(lag <= slo_seconds for lag in lags) / len(lags), 1) if lags else None,
        "max_queue_depth": max_queue_depth,
        "meets_target": len(lags) == len(arrivals) and p95 is not None and p95 <= slo_seconds,
    }


def _capacity_threshold(host_delta_min: int, host_delta_max: int, run_scenario) -> dict:
    """Find the smallest capacity delta that meets the target in the requested range."""
    lower_scenario = run_scenario(host_delta_min)
    if lower_scenario["meets_target"]:
        return {
            "status": "at_or_below_search_limit",
            "minimum_host_delta_meeting_target": None,
            "maximum_removable_hosts_meeting_target": None,
            "maximum_removable_hosts_lower_bound": max(0, -host_delta_min),
        }
    upper_scenario = run_scenario(host_delta_max)
    if not upper_scenario["meets_target"]:
        return {
            "status": "no_passing_delta",
            "minimum_host_delta_meeting_target": None,
            "maximum_removable_hosts_meeting_target": None,
            "maximum_removable_hosts_lower_bound": None,
        }

    lower, upper = host_delta_min + 1, host_delta_max
    while lower < upper:
        middle = (lower + upper) // 2
        if run_scenario(middle)["meets_target"]:
            upper = middle
        else:
            lower = middle + 1
    return {
        "status": "exact",
        "minimum_host_delta_meeting_target": lower,
        "maximum_removable_hosts_meeting_target": max(0, -lower),
        "maximum_removable_hosts_lower_bound": None,
    }


def calculate_capacity_scenarios(
    pool_id: str, range_start: str, range_end: str, target_p95_seconds: int,
    host_delta_min: int, host_delta_max: int, turnaround_seconds: int, runs: Iterable[dict], availability_transitions: Iterable[dict],
) -> dict:
    start, end = _parse(range_start), _parse(range_end)
    if end <= start:
        raise ValueError("end must be after start")
    if target_p95_seconds <= 0:
        raise ValueError("target_p95_seconds must be greater than zero")
    if turnaround_seconds < 0:
        raise ValueError("turnaround_seconds must be non-negative")
    if host_delta_min > 0 or host_delta_max < 0:
        raise ValueError("host delta range must include zero")
    if host_delta_min > host_delta_max:
        raise ValueError("host_delta_min must not exceed host_delta_max")

    arrivals = []
    observed_lags = []
    excluded_runs = 0
    for row in runs:
        try:
            scheduled, started, resolved = (_parse(row[key]) for key in ("scheduled", "started", "resolved"))
        except (AttributeError, KeyError, TypeError, ValueError):
            excluded_runs += 1
            continue
        if not start <= scheduled < end or started < scheduled or resolved < started:
            excluded_runs += 1
            continue
        arrivals.append((scheduled, (resolved - started).total_seconds()))
        observed_lags.append((started - scheduled).total_seconds())
    arrivals.sort()
    capacity_changes = _availability_by_time(availability_transitions, start, end)
    def run_scenario(host_delta: int) -> dict:
        return _scenario(arrivals, capacity_changes, host_delta, target_p95_seconds, turnaround_seconds)

    threshold = _capacity_threshold(host_delta_min, host_delta_max, run_scenario)
    display_deltas = {host_delta_min, 0, host_delta_max}
    if threshold["status"] == "exact":
        threshold_delta = threshold["minimum_host_delta_meeting_target"]
        display_deltas.update({threshold_delta - 1, threshold_delta, threshold_delta + 1})
    scenarios = [run_scenario(delta) for delta in sorted(
        delta for delta in display_deltas if host_delta_min <= delta <= host_delta_max
    )]
    modeled_baseline = next(scenario for scenario in scenarios if scenario["host_delta"] == 0)
    observed_p95 = _percentile(observed_lags, 0.95)
    modeled_p95 = modeled_baseline["modeled_p95_seconds"]
    return {
        "metric": "modeled_capacity_scenario",
        "model": {
            "version": MODEL_VERSION,
            "status": "uncalibrated",
            "scope": SCOPE,
            "capacity_basis": "observed healthy workers plus signed host delta",
            "assumptions": {"turnaround_seconds": turnaround_seconds, "effective_capacity_floor": 0},
        },
        "pool_id": pool_id,
        "start_at": start.isoformat(),
        "end_at": end.isoformat(),
        "target_p95_seconds": target_p95_seconds,
        "host_delta_range": {"min": host_delta_min, "max": host_delta_max},
        "observed_run_count": len(arrivals),
        "excluded_run_count": excluded_runs,
        "observed_baseline": {
            "p50_seconds": _percentile(observed_lags, 0.50),
            "p95_seconds": observed_p95,
            "started_within_target_pct": round(
                100 * sum(lag <= target_p95_seconds for lag in observed_lags) / len(observed_lags), 1,
            ) if observed_lags else None,
        },
        "calibration": {
            "status": "uncalibrated",
            "modeled_zero_added_hosts_p95_seconds": modeled_p95,
            "observed_p95_seconds": observed_p95,
            "p95_difference_seconds": round(modeled_p95 - observed_p95, 3) if modeled_p95 is not None and observed_p95 is not None else None,
        },
        "scenarios": scenarios,
        "capacity_threshold": threshold,
    }


def calculate_turnaround_sensitivity(
    pool_id: str, range_start: str, range_end: str, target_p95_seconds: int,
    host_delta_min: int, host_delta_max: int, runs: list[dict], availability_transitions: list[dict],
) -> dict:
    """Return comparable fixed and per-pool turnaround scenario results."""
    summary = busy_turnaround_summary(runs, range_start, range_end)
    variants = []
    for identifier, label, seconds, source in (
        ("fixed_120_seconds", "Fixed 2-minute turnaround", 120, "configured_constant"),
        ("busy_cycle_median", "Per-pool busy-cycle median", summary["p50_seconds"], summary["source"]),
    ):
        if seconds is None or (identifier == "busy_cycle_median" and not summary["available"]):
            continue
        result = calculate_capacity_scenarios(
            pool_id, range_start, range_end, target_p95_seconds, host_delta_min, host_delta_max, int(seconds), runs, availability_transitions,
        )
        variants.append({
            "id": identifier,
            "label": label,
            "turnaround": {"seconds": int(seconds), "source": source},
            "calibration": result["calibration"],
            "scenarios": result["scenarios"],
            "capacity_threshold": result["capacity_threshold"],
        })
    return {"busy_turnaround": summary, "variants": variants}
