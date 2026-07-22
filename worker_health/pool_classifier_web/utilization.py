from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _merge(intervals: Iterable[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    merged: List[Tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _intersection(
    left: Iterable[Tuple[datetime, datetime]],
    right: Iterable[Tuple[datetime, datetime]],
) -> List[Tuple[datetime, datetime]]:
    left_intervals = _merge(left)
    right_intervals = _merge(right)
    result = []
    left_index = right_index = 0
    while left_index < len(left_intervals) and right_index < len(right_intervals):
        left_start, left_end = left_intervals[left_index]
        right_start, right_end = right_intervals[right_index]
        start = max(left_start, right_start)
        end = min(left_end, right_end)
        if end > start:
            result.append((start, end))
        if left_end <= right_end:
            left_index += 1
        else:
            right_index += 1
    return result


def _clip_seconds(
    intervals: Iterable[Tuple[datetime, datetime]],
    start: datetime,
    end: datetime,
) -> float:
    seconds = 0.0
    for interval_start, interval_end in intervals:
        clipped_start = max(start, interval_start)
        clipped_end = min(end, interval_end)
        if clipped_end > clipped_start:
            seconds += (clipped_end - clipped_start).total_seconds()
    return seconds


def _is_complete(
    intervals: Iterable[Tuple[datetime, datetime]],
    start: datetime,
    end: datetime,
) -> bool:
    clipped = _merge(
        (max(start, interval_start), min(end, interval_end))
        for interval_start, interval_end in intervals
        if min(end, interval_end) > max(start, interval_start)
    )
    return len(clipped) == 1 and clipped[0] == (start, end)


def _task_intervals(task_runs: List[dict]) -> List[Tuple[datetime, datetime]]:
    intervals = []
    for run in task_runs:
        start = _parse(run["start_at"])
        end = _parse(run["end_at"])
        if end > start:
            intervals.append((start, end))
    return intervals


def _availability_intervals_by_worker(
    transitions: List[dict],
    range_start: datetime,
    range_end: datetime,
) -> Dict[str, List[Tuple[datetime, datetime]]]:
    by_worker: Dict[str, List[dict]] = {}
    for transition in transitions:
        by_worker.setdefault(transition["worker_id"], []).append(transition)

    available_by_worker = {}
    for worker_id, worker_transitions in by_worker.items():
        events = sorted(
            worker_transitions,
            key=lambda event: (_parse(event["observed_at"]), event.get("id", 0)),
        )
        parsed_events = [(_parse(event["effective_at"]), bool(event["available"])) for event in events]
        boundaries = {range_start, range_end}
        boundaries.update(
            max(range_start, min(range_end, effective_at))
            for effective_at, _available in parsed_events
        )
        ordered = sorted(boundaries)
        intervals = []
        for segment_start, segment_end in zip(ordered, ordered[1:]):
            available: Optional[bool] = None
            for effective_at, event_available in parsed_events:
                if effective_at <= segment_start:
                    available = event_available
            if available and segment_end > segment_start:
                intervals.append((segment_start, segment_end))
        available_by_worker[worker_id] = _merge(intervals)
    return available_by_worker


def _sum_worker_seconds(
    intervals_by_worker: Dict[str, List[Tuple[datetime, datetime]]],
    start: datetime,
    end: datetime,
) -> float:
    return sum(_clip_seconds(intervals, start, end) for intervals in intervals_by_worker.values())


def calculate_utilization(
    pool_id: str,
    range_start: str,
    range_end: str,
    bucket_seconds: int,
    task_runs: List[dict],
    availability_transitions: List[dict],
    task_coverage_intervals: List[dict],
    availability_coverage_intervals: List[dict],
) -> dict:
    start = _parse(range_start)
    end = _parse(range_end)
    if end <= start:
        raise ValueError("range_end must be after range_start")
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be greater than zero")

    task_coverage = [(_parse(row["start_at"]), _parse(row["end_at"])) for row in task_coverage_intervals]
    availability_coverage = [
        (_parse(row["start_at"]), _parse(row["end_at"]))
        for row in availability_coverage_intervals
    ]
    combined_coverage = _intersection(task_coverage, availability_coverage)
    duration_seconds = (end - start).total_seconds()
    coverage_seconds = _clip_seconds(combined_coverage, start, end)
    collection_starts = [
        intervals[0][0]
        for intervals in (task_coverage, availability_coverage)
        if intervals
    ]
    collection_started = max(collection_starts).isoformat() if len(collection_starts) == 2 else None

    task_intervals = _task_intervals(task_runs)
    availability_intervals = _availability_intervals_by_worker(
        availability_transitions,
        start,
        end,
    )

    buckets = []
    bucket_start = start
    while bucket_start < end:
        remaining_seconds = (end - bucket_start).total_seconds()
        bucket_end = bucket_start + timedelta(seconds=min(bucket_seconds, remaining_seconds))
        bucket_duration = (bucket_end - bucket_start).total_seconds()
        bucket_coverage_seconds = _clip_seconds(combined_coverage, bucket_start, bucket_end)
        complete = _is_complete(combined_coverage, bucket_start, bucket_end)
        bucket = {
            "start_at": bucket_start.isoformat(),
            "end_at": bucket_end.isoformat(),
            "coverage_pct": bucket_coverage_seconds / bucket_duration * 100,
            "complete": complete,
            "status": "incomplete",
            "busy_worker_hours": None,
            "available_worker_hours": None,
            "worker_equivalents": None,
            "utilization_pct": None,
        }
        if complete:
            busy_seconds = _clip_seconds(task_intervals, bucket_start, bucket_end)
            available_seconds = _sum_worker_seconds(
                availability_intervals,
                bucket_start,
                bucket_end,
            )
            bucket.update(
                {
                    "status": "available" if available_seconds > 0 else "unavailable",
                    "busy_worker_hours": busy_seconds / 3600,
                    "available_worker_hours": available_seconds / 3600,
                    "worker_equivalents": busy_seconds / bucket_duration,
                    "utilization_pct": (
                        busy_seconds / available_seconds * 100
                        if available_seconds > 0
                        else None
                    ),
                },
            )
        buckets.append(bucket)
        bucket_start = bucket_end

    return {
        "pool_id": pool_id,
        "start_at": start.isoformat(),
        "end_at": end.isoformat(),
        "bucket_seconds": bucket_seconds,
        "collection_started": collection_started,
        "coverage_pct": coverage_seconds / duration_seconds * 100,
        "complete": _is_complete(combined_coverage, start, end),
        "buckets": buckets,
    }
