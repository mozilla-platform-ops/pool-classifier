from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker_health.pool_classifier_web.storage import SqliteStorage


def _iso(base, minutes):
    return (base + timedelta(minutes=minutes)).isoformat()


def test_startup_accumulates_and_uninterrupted_polls_coalesce(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    start = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)

    storage.record_collection_coverage("task_runs", _iso(start, 0), True, 900)
    storage.record_collection_coverage("task_runs", _iso(start, 10), True, 900)
    storage.record_collection_coverage("task_runs", _iso(start, 20), True, 900)
    storage.commit()

    coverage = storage.get_collection_coverage("task_runs", _iso(start, 0), _iso(start, 20))
    assert coverage["collection_started"] == _iso(start, 0)
    assert coverage["intervals"] == [{"start_at": _iso(start, 0), "end_at": _iso(start, 20)}]
    assert coverage["coverage_seconds"] == 1200
    assert coverage["coverage_pct"] == 100
    assert coverage["complete"] is True


def test_failed_poll_creates_gap_and_resumed_collection(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    start = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)

    for minutes, success in ((0, True), (10, True), (20, False), (30, True), (40, True)):
        storage.record_collection_coverage("task_runs", _iso(start, minutes), success, 900)
    storage.commit()

    coverage = storage.get_collection_coverage("task_runs", _iso(start, 0), _iso(start, 40))
    assert coverage["intervals"] == [
        {"start_at": _iso(start, 0), "end_at": _iso(start, 10)},
        {"start_at": _iso(start, 30), "end_at": _iso(start, 40)},
    ]
    assert coverage["coverage_seconds"] == 1200
    assert coverage["coverage_pct"] == 50
    assert coverage["complete"] is False


def test_elapsed_outage_starts_new_interval_without_failure_observation(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    start = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)

    storage.record_collection_coverage("worker_availability", _iso(start, 0), True, 900)
    storage.record_collection_coverage("worker_availability", _iso(start, 16), True, 900)
    storage.commit()

    coverage = storage.get_collection_coverage("worker_availability")
    assert coverage["intervals"] == [
        {"start_at": _iso(start, 0), "end_at": _iso(start, 0)},
        {"start_at": _iso(start, 16), "end_at": _iso(start, 16)},
    ]
    assert coverage["coverage_pct"] is None
    assert coverage["complete"] is None


def test_sources_are_independent_and_inputs_are_validated(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    start = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)
    storage.record_collection_coverage("task_runs", _iso(start, 0), True, 900)
    storage.record_collection_coverage("worker_availability", _iso(start, 5), True, 900)
    storage.commit()

    assert storage.get_collection_coverage("task_runs")["collection_started"] == _iso(start, 0)
    assert storage.get_collection_coverage("worker_availability")["collection_started"] == _iso(start, 5)
    with pytest.raises(ValueError, match="unknown collection source"):
        storage.get_collection_coverage("unknown")
    with pytest.raises(ValueError, match="provided together"):
        storage.get_collection_coverage("task_runs", range_start=_iso(start, 0))
    with pytest.raises(ValueError, match="after range_start"):
        storage.get_collection_coverage("task_runs", _iso(start, 0), _iso(start, 0))
