from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker_health.pool_classifier_web.storage import SqliteStorage
from worker_health.pool_classifier import PoolClassifier


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


def test_task_run_overlap_extends_coverage_without_a_timer_gap(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    start = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)

    storage.record_collection_coverage("task_runs", _iso(start, 0), True, None)
    storage.record_collection_coverage("task_runs", _iso(start, 120), True, None)
    storage.commit()

    assert storage.get_collection_coverage("task_runs")["intervals"] == [
        {"start_at": _iso(start, 0), "end_at": _iso(start, 120)},
    ]


def test_classifier_keeps_overlap_coverage_through_transient_status_failure(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    monkeypatch.setattr(
        classifier,
        "_get_recent_tasks",
        lambda _group, _worker: [{"taskId": "task-1", "runId": 0}],
    )
    monkeypatch.setattr(
        classifier, "_get_task_status",
        lambda _task: (_ for _ in ()).throw(RuntimeError("Queue busy")),
    )

    classifier.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])
    classifier.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])

    coverage = storage.get_collection_coverage("task_runs")
    assert len(coverage["intervals"]) == 1
    assert storage.list_unresolved_task_runs(10)[0]["task_id"] == "task-1"


def _coverage_classifier(tmp_path, monkeypatch, windows):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)

    def recent_tasks(_group, _worker):
        result = next(windows)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(classifier, "_get_recent_tasks", recent_tasks)
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task: {"status": {"runs": []}})
    return classifier, storage


def test_classifier_bridges_transient_get_worker_failure_when_next_window_overlaps(tmp_path, monkeypatch):
    task = [{"taskId": "task-1", "runId": 0}]
    classifier, storage = _coverage_classifier(tmp_path, monkeypatch, iter([task, RuntimeError("Queue busy"), task]))
    workers = [{"workerId": "worker-1", "workerGroup": "group-1"}]

    for _ in range(3):
        classifier.classify_cycle(workers=workers)

    assert len(storage.get_collection_coverage("task_runs")["intervals"]) == 1


def test_classifier_bridges_repeated_get_worker_failures_when_next_window_overlaps(tmp_path, monkeypatch):
    task = [{"taskId": "task-1", "runId": 0}]
    classifier, storage = _coverage_classifier(
        tmp_path, monkeypatch, iter([task, RuntimeError("Queue busy"), RuntimeError("Queue busy"), task]),
    )
    workers = [{"workerId": "worker-1", "workerGroup": "group-1"}]

    for _ in range(4):
        classifier.classify_cycle(workers=workers)

    assert len(storage.get_collection_coverage("task_runs")["intervals"]) == 1


def test_classifier_preserves_gap_after_get_worker_failure_when_next_window_does_not_overlap(tmp_path, monkeypatch):
    first_task = [{"taskId": "task-1", "runId": 0}]
    second_task = [{"taskId": "task-2", "runId": 0}]
    classifier, storage = _coverage_classifier(
        tmp_path, monkeypatch, iter([first_task, RuntimeError("Queue busy"), second_task, second_task]),
    )
    workers = [{"workerId": "worker-1", "workerGroup": "group-1"}]

    for _ in range(4):
        classifier.classify_cycle(workers=workers)

    assert len(storage.get_collection_coverage("task_runs")["intervals"]) == 2


def test_classifier_does_not_mask_a_proven_gap_with_another_workers_fetch_failure(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    windows = {
        "worker-fetch-failure": iter([
            [{"taskId": "task-a", "runId": 0}], RuntimeError("Queue busy"),
        ]),
        "worker-gap": iter([
            [{"taskId": "task-b", "runId": 0}], [{"taskId": "task-c", "runId": 0}],
        ]),
    }

    def recent_tasks(_group, worker_id):
        result = next(windows[worker_id])
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(classifier, "_get_recent_tasks", recent_tasks)
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task: {"status": {"runs": []}})
    workers = [
        {"workerId": "worker-fetch-failure", "workerGroup": "group-1"},
        {"workerId": "worker-gap", "workerGroup": "group-1"},
    ]

    classifier.classify_cycle(workers=workers)
    classifier.classify_cycle(workers=workers)

    coverage = storage.get_collection_coverage("task_runs")
    assert len(coverage["intervals"]) == 1
    state = storage.db.execute(
        "SELECT last_success FROM collection_coverage_state WHERE source = 'task_runs'",
    ).fetchone()
    assert state["last_success"] == 0


def test_classifier_starts_new_coverage_interval_for_unbridged_window(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    windows = iter([
        [{"taskId": "task-1", "runId": 0}],
        [{"taskId": "task-2", "runId": 0}],
        [{"taskId": "task-2", "runId": 0}],
    ])
    monkeypatch.setattr(classifier, "_get_recent_tasks", lambda _group, _worker: next(windows))
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task: {"status": {"runs": []}})

    classifier.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])
    classifier.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])
    classifier.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])

    assert len(storage.get_collection_coverage("task_runs")["intervals"]) == 2
    events = storage.list_task_run_coverage_events("2020-01-01T00:00:00+00:00", "2030-01-01T00:00:00+00:00")
    assert events[0]["reason"] == "recent_tasks_no_overlap"
    assert events[0]["worker_id"] == "worker-1"
    assert events[0]["previous_recent_tasks"] == [["task-1", 0]]
    assert events[0]["current_recent_tasks"] == [["task-2", 0]]


@pytest.mark.parametrize("current_window", [
    [{"taskId": "task-1", "runId": 0}],
    [{"taskId": "task-1", "runId": 0}, {"taskId": "task-2", "runId": 0}],
])
def test_classifier_keeps_coverage_when_an_idle_worker_receives_work(tmp_path, monkeypatch, current_window):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    windows = iter([[], current_window, current_window])
    monkeypatch.setattr(classifier, "_get_recent_tasks", lambda _group, _worker: next(windows))
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task: {"status": {"runs": []}})
    worker = [{"workerId": "worker-1", "workerGroup": "group-1"}]

    classifier.classify_cycle(workers=worker)
    classifier.classify_cycle(workers=worker)
    classifier.classify_cycle(workers=worker)

    assert len(storage.get_collection_coverage("task_runs")["intervals"]) == 1
    assert storage.list_task_run_coverage_events(
        "2020-01-01T00:00:00+00:00", "2030-01-01T00:00:00+00:00",
    ) == []


def test_threaded_polling_persists_a_baseline_task_window(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_retry_unresolved_task_runs", lambda: ({}, True))
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    monkeypatch.setattr(
        classifier, "_get_recent_tasks", lambda _group, _worker: [{"taskId": "task-1", "runId": 0}],
    )
    monkeypatch.setattr(
        classifier,
        "_get_task_status",
        lambda _task: {"status": {"runs": [{
            "runId": 0, "workerId": "worker-1", "state": "completed",
            "started": "2026-07-21T10:00:00+00:00", "resolved": "2026-07-21T10:01:00+00:00",
        }]}},
    )

    classifier.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])

    assert storage.db.execute("SELECT run_state FROM task_results").fetchone()[0] == "completed"
    assert len(storage.get_collection_coverage("task_runs")["intervals"]) == 1


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
