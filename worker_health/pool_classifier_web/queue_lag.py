"""Observed scheduled-to-start lag calculations.

These samples come only from task runs that started and were subsequently
observed terminal by the per-worker collector.  They are deliberately not a
measurement of the complete Taskcluster queue population.
"""

from __future__ import annotations

from datetime import datetime, timezone
from math import ceil
from typing import Iterable


SCOPE = (
    "Only task runs that started and were later observed terminal by the "
    "per-worker collector; tasks that never started are not represented."
)


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _percentile_nearest_rank(samples: list[float], percentile: float) -> float:
    """Return the nearest-rank percentile for already sorted samples."""
    return samples[max(0, ceil(percentile * len(samples)) - 1)]


def calculate_observed_start_lag(
    pool_id: str,
    range_start: str,
    range_end: str,
    slo_seconds: int,
    runs: Iterable[dict[str, str | None]],
) -> dict:
    """Summarize valid scheduled-to-start samples whose schedule is in range."""
    start = _parse(range_start)
    end = _parse(range_end)
    if end <= start:
        raise ValueError("end must be after start")
    if slo_seconds <= 0:
        raise ValueError("slo_seconds must be greater than zero")

    samples = []
    for run in runs:
        scheduled_value, started_value = run.get("scheduled"), run.get("started")
        if not scheduled_value or not started_value:
            continue
        try:
            scheduled, started = _parse(scheduled_value), _parse(started_value)
        except ValueError:
            continue
        if not start <= scheduled < end or started < scheduled:
            continue
        samples.append((started - scheduled).total_seconds())

    samples.sort()
    sample_count = len(samples)
    within_slo_count = sum(sample <= slo_seconds for sample in samples)
    return {
        "metric": "observed_scheduled_to_start_lag",
        "scope": SCOPE,
        "pool_id": pool_id,
        "start_at": start.isoformat(),
        "end_at": end.isoformat(),
        "sample_count": sample_count,
        "p50_seconds": _percentile_nearest_rank(samples, 0.50) if samples else None,
        "p95_seconds": _percentile_nearest_rank(samples, 0.95) if samples else None,
        "slo_seconds": slo_seconds,
        "started_within_slo_count": within_slo_count,
        "started_within_slo_pct": round(100 * within_slo_count / sample_count, 1) if sample_count else None,
    }
