from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker_health.pool_classifier_web.storage import SqliteStorage


START = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)


def _iso(hours=0, minutes=0):
    return (START + timedelta(hours=hours, minutes=minutes)).isoformat()


def _storage(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    return storage


def _complete_coverage(storage, end_hours=2):
    for source in ("task_runs", "worker_availability"):
        storage.record_collection_coverage(source, _iso(), True, end_hours * 3600)
        storage.record_collection_coverage(source, _iso(hours=end_hours), True, end_hours * 3600)


def _transition(storage, worker_id, available, reason, effective_at, observed_at, quarantined=False):
    storage.record_worker_availability_transition(
        worker_id,
        "group-1",
        available,
        quarantined,
        effective_at,
        None,
        reason,
        effective_at,
        observed_at,
    )


def _task(storage, task_id, worker_id, started, resolved):
    storage.record_task_result(
        task_id,
        worker_id,
        0,
        "completed",
        None,
        None,
        started,
        resolved,
        resolved,
    )


def test_bucketed_utilization_clips_and_sums_overlapping_task_intervals(tmp_path):
    storage = _storage(tmp_path)
    _complete_coverage(storage)
    _transition(storage, "w1", True, "online", _iso(), _iso())
    _transition(storage, "w1", False, "quarantine", _iso(minutes=30), _iso(minutes=30), True)
    _transition(storage, "w1", True, "unquarantine", _iso(hours=1), _iso(hours=1))
    _transition(storage, "w2", True, "online", _iso(), _iso())

    _task(storage, "before", "w1", _iso(hours=-1), _iso())
    _task(storage, "w1-main", "w1", _iso(minutes=-30), _iso(minutes=45))
    _task(storage, "w1-overlap", "w1", _iso(minutes=15), _iso(minutes=30))
    _task(storage, "w2-cross", "w2", _iso(minutes=30), _iso(hours=1, minutes=30))
    _task(storage, "w1-second", "w1", _iso(hours=1), _iso(hours=2, minutes=30))
    _task(storage, "after", "w2", _iso(hours=2), _iso(hours=3))
    storage.commit()

    result = storage.get_utilization(_iso(), _iso(hours=2), 3600)

    assert result["complete"] is True
    assert result["coverage_pct"] == 100
    assert result["collection_started"] == _iso()
    first, second = result["buckets"]
    assert first["status"] == second["status"] == "available"
    assert first["busy_worker_hours"] == pytest.approx(1.5)
    assert first["available_worker_hours"] == pytest.approx(1.5)
    assert first["worker_equivalents"] == pytest.approx(1.5)
    assert first["utilization_pct"] == pytest.approx(100)
    assert second["busy_worker_hours"] == pytest.approx(1.5)
    assert second["available_worker_hours"] == pytest.approx(2.0)
    assert second["worker_equivalents"] == pytest.approx(1.5)
    assert second["utilization_pct"] == pytest.approx(75.0)


def test_return_transition_and_partial_final_bucket(tmp_path):
    storage = _storage(tmp_path)
    _complete_coverage(storage, end_hours=1)
    _transition(storage, "w1", True, "online", _iso(), _iso())
    _transition(storage, "w1", False, "contact_timeout", _iso(minutes=15), _iso(minutes=20))
    _transition(storage, "w1", True, "return", _iso(minutes=30), _iso(minutes=35))
    _task(storage, "return-task", "w1", _iso(minutes=30), _iso(hours=1))
    storage.commit()

    result = storage.get_utilization(_iso(), _iso(hours=1), 1800)
    first, second = result["buckets"]
    assert first["available_worker_hours"] == pytest.approx(0.25)
    assert first["busy_worker_hours"] == 0
    assert second["available_worker_hours"] == pytest.approx(0.5)
    assert second["worker_equivalents"] == pytest.approx(1.0)
    assert second["utilization_pct"] == pytest.approx(100)

    partial = storage.get_utilization(_iso(), _iso(hours=1), 2400)["buckets"]
    assert partial[-1]["start_at"] == _iso(minutes=40)
    assert partial[-1]["end_at"] == _iso(hours=1)
    assert partial[-1]["worker_equivalents"] == pytest.approx(1.0)


def test_empty_bucket_and_zero_availability(tmp_path):
    available_storage = _storage(tmp_path / "available")
    _complete_coverage(available_storage, end_hours=1)
    _transition(available_storage, "w1", True, "online", _iso(), _iso())
    available_storage.commit()
    empty = available_storage.get_utilization(_iso(), _iso(hours=1), 3600)["buckets"][0]
    assert empty["status"] == "available"
    assert empty["busy_worker_hours"] == 0
    assert empty["worker_equivalents"] == 0
    assert empty["utilization_pct"] == 0

    unavailable_storage = _storage(tmp_path / "unavailable")
    _complete_coverage(unavailable_storage, end_hours=1)
    _transition(unavailable_storage, "w1", False, "contact_timeout", _iso(), _iso())
    unavailable_storage.commit()
    unavailable = unavailable_storage.get_utilization(_iso(), _iso(hours=1), 3600)["buckets"][0]
    assert unavailable["status"] == "unavailable"
    assert unavailable["available_worker_hours"] == 0
    assert unavailable["utilization_pct"] is None


def test_late_return_contact_corrects_an_inferred_timeout(tmp_path):
    storage = _storage(tmp_path)
    _complete_coverage(storage)
    _transition(storage, "w1", True, "online", _iso(), _iso())
    _transition(storage, "w1", False, "contact_timeout", _iso(hours=1), _iso(hours=1, minutes=10))
    _transition(storage, "w1", True, "return", _iso(minutes=50), _iso(hours=1, minutes=15))
    storage.commit()

    buckets = storage.get_utilization(_iso(), _iso(hours=2), 3600)["buckets"]
    assert buckets[0]["available_worker_hours"] == pytest.approx(1)
    assert buckets[1]["available_worker_hours"] == pytest.approx(1)


def test_incomplete_coverage_suppresses_metrics(tmp_path):
    storage = _storage(tmp_path)
    for source in ("task_runs", "worker_availability"):
        storage.record_collection_coverage(source, _iso(), True, 3600)
        storage.record_collection_coverage(source, _iso(minutes=30), True, 3600)
    _transition(storage, "w1", True, "online", _iso(), _iso())
    _task(storage, "task", "w1", _iso(), _iso(hours=1))
    storage.commit()

    result = storage.get_utilization(_iso(), _iso(hours=1), 3600)
    bucket = result["buckets"][0]
    assert result["coverage_pct"] == 50
    assert result["complete"] is False
    assert bucket["status"] == "incomplete"
    assert bucket["busy_worker_hours"] is None
    assert bucket["available_worker_hours"] is None
    assert bucket["worker_equivalents"] is None
    assert bucket["utilization_pct"] is None


def test_utilization_validates_range_and_bucket(tmp_path):
    storage = _storage(tmp_path)
    with pytest.raises(ValueError, match="after range_start"):
        storage.get_utilization(_iso(), _iso(), 3600)
    with pytest.raises(ValueError, match="greater than zero"):
        storage.get_utilization(_iso(), _iso(hours=1), 0)
