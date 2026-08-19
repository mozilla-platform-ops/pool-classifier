from __future__ import annotations

import sqlite3
import threading
import gzip
import io
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import json
import logging

from worker_health.pool_classifier import PhaseMemorySampler, PoolClassifier
from worker_health.pool_classifier_web.storage import PostgresStorage, SqliteStorage


def test_compressed_log_fetch_keeps_true_plaintext_tail(monkeypatch, tmp_path):
    payload = b"start\n" + (b"verbose test output\n" * 10_000) + b"WARNING - Got 21 unexpected statuses\n"
    compressed = gzip.compress(payload)

    class Raw(io.BytesIO):
        pass

    class Response:
        status_code = 200
        headers = {"x-goog-stored-content-length": str(len(compressed))}

        def __init__(self):
            self.raw = Raw(compressed)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr("worker_health.pool_classifier.requests.get", lambda *_args, **_kwargs: Response())
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=object(), use_color=False)

    log_text, status = classifier._fetch_log_tail("task-id", 0)

    assert status == "ok"
    assert "WARNING - Got 21 unexpected statuses" in log_text
    assert len(log_text.encode()) <= 20480 + 51200


def test_sqlite_records_resolved_time_and_distinct_retries(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()

    for run_id, state, started, resolved in (
        (0, "failed", "2026-07-14T10:00:00+00:00", "2026-07-14T10:05:00+00:00"),
        (1, "completed", "2026-07-14T10:10:00+00:00", "2026-07-14T10:15:00+00:00"),
        (2, "exception", None, None),
    ):
        storage.record_task_result(
            "task-1",
            "worker-1",
            run_id,
            state,
            None,
            None,
            started,
            resolved,
            "2026-07-14T10:20:00+00:00",
        )
    storage.commit()

    rows = storage.db.execute(
        "SELECT run_id, run_state, run_resolved FROM task_results ORDER BY run_id",
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (0, "failed", "2026-07-14T10:05:00+00:00"),
        (1, "completed", "2026-07-14T10:15:00+00:00"),
        (2, "exception", None),
    ]
    assert storage.get_seen_task_runs() == {
        "worker-1": {("task-1", 0), ("task-1", 1), ("task-1", 2)},
    }


def test_phase_memory_sampler_reports_local_peak_and_deltas():
    readings = iter([100, 140, 125])
    sampler = PhaseMemorySampler(lambda: next(readings), interval_seconds=60)

    with sampler:
        sampler._sample()

    assert sampler.metrics() == {
        "rss_start_bytes": 100,
        "rss_end_bytes": 125,
        "rss_max_bytes": 140,
        "rss_delta_bytes": 25,
        "rss_peak_delta_bytes": 40,
        "rss_sample_count": 3,
    }


def test_completed_task_is_logged_once_without_a_category(tmp_path, monkeypatch, caplog):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_record_job_source", lambda *_args: None)
    task = (
        "task-1", 0, "completed", "2026-08-18T10:00:00+00:00",
        "2026-08-18T10:01:00+00:00", None,
    )

    with caplog.at_level(logging.INFO, logger="worker_health.pool_classifier"):
        classifier._process_results("worker-1", [task])

    assert caplog.text.count("completed task=task-1 run=0") == 1
    assert "→ None" not in caplog.text


def test_sqlite_persists_observed_runs_and_guards_terminal_transitions(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    observed = "2026-07-14T10:00:00+00:00"
    checked = "2026-07-14T10:02:00+00:00"
    classified = "2026-07-14T10:05:00+00:00"

    storage.record_observed_task_run("task-1", "worker-1", 0, observed)
    assert storage.record_task_run_check("task-1", 0, checked) is True
    assert storage.list_unresolved_task_runs(10) == [{
        "task_id": "task-1", "worker_id": "worker-1", "run_id": 0,
        "run_state": "observed", "observed_at": observed, "last_checked_at": checked,
    }]

    storage.record_task_result(
        "task-1", "worker-1", 0, "failed", "infra", "worker-shutdown",
        "2026-07-14T10:01:00+00:00", "2026-07-14T10:04:00+00:00", classified,
    )
    # A late observation must never erase or downgrade a terminal outcome.
    storage.record_observed_task_run("task-1", "other-worker", 0, "2026-07-14T10:06:00+00:00")
    storage.commit()

    row = storage.db.execute(
        "SELECT worker_id, run_state, category, reason_resolved, observed_at, last_checked_at"
        " FROM task_results WHERE task_id = 'task-1'",
    ).fetchone()
    assert tuple(row) == (
        "worker-1", "failed", "infra", "worker-shutdown", observed, classified,
    )
    assert storage.list_unresolved_task_runs(10) == []


def test_prepared_observed_batch_is_durable_deduplicated_and_keeps_routing(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    storage.record_observed_task_run(
        "from-restart", "old-worker", 0, "2026-07-14T10:00:00+00:00",
    )
    storage.commit()

    by_task, continuity, window_observed = classifier._prepare_observed_task_run_batch([
        ("worker-a", "group-a", [{"taskId": "shared", "runId": 0}]),
        ("worker-b", "group-b", [{"taskId": "shared", "runId": 1}]),
    ])

    assert window_observed is True
    assert continuity == {"worker-a": None, "worker-b": None}
    assert {
        task_id: {(item["worker_id"], item["worker_group"], item["run_id"])
        for item in references}
        for task_id, references in by_task.items()
    } == {
        "from-restart": {("old-worker", None, 0)},
        "shared": {("worker-a", "group-a", 0), ("worker-b", "group-b", 1)},
    }
    rows = storage.list_unresolved_task_runs(10)
    assert {row["task_id"] for row in rows} == {"from-restart", "shared"}
    assert all(row["last_checked_at"] > "2026-07-14T10:00:00+00:00" for row in rows)


def test_prepared_task_statuses_fetch_once_per_task_and_do_not_raise(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    barrier = threading.Barrier(3)
    calls = []

    def status(task_id):
        calls.append(task_id)
        barrier.wait(timeout=1)
        if task_id == "expired":
            return None
        if task_id == "transient":
            raise RuntimeError("Queue busy")
        return {"status": {"runs": []}}

    monkeypatch.setattr(classifier, "_get_task_status", status)
    results = classifier._fetch_prepared_task_statuses({
        "ok": [{"task_id": "ok"}, {"task_id": "ok"}],
        "expired": [{"task_id": "expired"}],
        "transient": [{"task_id": "transient"}],
    })

    assert set(calls) == {"ok", "expired", "transient"}
    assert results["ok"] == ("ok", {"status": {"runs": []}})
    assert results["expired"] == ("expired", None)
    assert results["transient"] == ("error", None)


def test_apply_prepared_statuses_expires_retries_and_routes_terminals(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    observed = "2026-07-14T10:00:00+00:00"
    for task_id, worker_id in (("expired", "worker-a"), ("retry", "worker-b"), ("done", "worker-c")):
        storage.record_observed_task_run(task_id, worker_id, 0, observed)
    storage.commit()
    references = {
        "expired": [{"task_id": "expired", "worker_id": "worker-a", "worker_group": "group-a", "run_id": 0}],
        "retry": [{"task_id": "retry", "worker_id": "worker-b", "worker_group": "group-b", "run_id": 0}],
        "done": [{"task_id": "done", "worker_id": "worker-c", "worker_group": "group-c", "run_id": 0}],
    }

    terminals, complete = classifier._apply_prepared_task_statuses(references, {
        "expired": ("expired", None),
        "retry": ("error", None),
        "done": ("ok", {"status": {"runs": [{
            "runId": 0, "workerId": "worker-c", "state": "completed",
            "started": "2026-07-14T10:01:00+00:00", "resolved": "2026-07-14T10:02:00+00:00",
        }]}}),
    })

    assert complete is False
    assert terminals["worker-c"][0][:3] == ("done", 0, "completed")
    assert classifier.seen_task_runs == {
        "worker-a": {("expired", 0)},
        "worker-c": {("done", 0)},
    }
    states = dict(storage.db.execute("SELECT task_id, run_state FROM task_results"))
    assert states == {"expired": "expired", "retry": "observed", "done": "observed"}


def test_classify_cycle_deduplicates_status_io_and_serializes_storage(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    main_thread = threading.get_ident()
    storage_threads = []
    original_record = storage.record_observed_task_run
    original_check = storage.record_task_run_check

    def record(*args):
        storage_threads.append(threading.get_ident())
        return original_record(*args)

    def check(*args):
        storage_threads.append(threading.get_ident())
        return original_check(*args)

    monkeypatch.setattr(storage, "record_observed_task_run", record)
    monkeypatch.setattr(storage, "record_task_run_check", check)
    monkeypatch.setattr(
        classifier, "_get_recent_tasks",
        lambda _group, worker_id: [{"taskId": "shared", "runId": 0 if worker_id == "worker-a" else 1}],
    )
    status_threads = []

    def status(task_id):
        status_threads.append(threading.get_ident())
        assert task_id == "shared"
        return {"status": {"runs": [
            {"runId": 0, "workerId": "worker-a", "state": "failed"},
            {"runId": 1, "workerId": "worker-b", "state": "completed"},
        ]}}

    monkeypatch.setattr(classifier, "_get_task_status", status)
    monkeypatch.setattr(classifier, "_fetch_log_tail", lambda *_args: ("", "empty"))
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    summary = classifier.classify_cycle(workers=[
        {"workerId": "worker-a", "workerGroup": "group-a"},
        {"workerId": "worker-b", "workerGroup": "group-b"},
    ])

    assert summary["new_terminal"] == 2
    assert summary["category_counts"] == {"unclassified": 1}
    for phase in ("worker_poll", "task_preparation", "task_status", "terminal_task_processing"):
        metrics = summary["memory_phases"][phase]
        assert set(metrics) >= {
            "rss_start_bytes", "rss_end_bytes", "rss_max_bytes", "rss_delta_bytes",
            "rss_peak_delta_bytes", "rss_sample_count",
        }
        if metrics["rss_sample_count"]:
            assert metrics["rss_sample_count"] >= 2
            assert metrics["rss_max_bytes"] >= metrics["rss_start_bytes"]
            assert metrics["rss_end_bytes"] is not None
        else:
            assert metrics["rss_start_bytes"] is metrics["rss_end_bytes"] is metrics["rss_max_bytes"] is None
        assert "peak_rss_bytes" not in metrics
    assert len(status_threads) == 1
    assert status_threads[0] != main_thread
    assert storage_threads and set(storage_threads) == {main_thread}


def test_sqlite_expired_observed_runs_are_not_reopened(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    storage.record_observed_task_run("expired", "worker-1", 0, "2026-07-14T10:00:00+00:00")
    assert storage.expire_task_run("expired", 0, "2026-07-14T10:02:00+00:00") is True
    storage.record_observed_task_run("expired", "worker-1", 0, "2026-07-14T10:03:00+00:00")
    storage.commit()

    row = storage.db.execute(
        "SELECT run_state, observed_at, last_checked_at FROM task_results WHERE task_id = 'expired'",
    ).fetchone()
    assert tuple(row) == ("expired", "2026-07-14T10:00:00+00:00", "2026-07-14T10:02:00+00:00")


def test_recent_outcome_counts_use_task_resolution_time(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    now = datetime.now(timezone.utc)

    storage.record_task_result(
        "old-completed", "worker-1", 0, "completed", None, None,
        (now - timedelta(hours=3)).isoformat(), (now - timedelta(hours=2)).isoformat(), now.isoformat(),
    )
    storage.record_task_result(
        "recent-failed", "worker-1", 0, "failed", None, None,
        (now - timedelta(hours=1)).isoformat(), (now - timedelta(minutes=30)).isoformat(),
        (now - timedelta(hours=2)).isoformat(),
    )
    # Rows stored before run_resolved existed retain the historical discovery-time fallback.
    storage.record_task_result(
        "legacy-completed", "worker-1", 0, "completed", None, None,
        None, None, (now - timedelta(minutes=20)).isoformat(),
    )
    storage.commit()

    since = (now - timedelta(hours=1)).isoformat()
    assert storage.count_recent_errors(since) == 1
    assert storage.count_recent_successes(since) == 1


def test_sqlite_migrates_legacy_task_results(tmp_path):
    db_path = tmp_path / "pool_classifier.db"
    with sqlite3.connect(db_path) as db:
        db.executescript(
            """
            CREATE TABLE task_results (
                task_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                run_id INTEGER,
                run_state TEXT NOT NULL,
                category TEXT,
                reason_resolved TEXT,
                run_started TEXT,
                classified_at TEXT NOT NULL,
                PRIMARY KEY (task_id, worker_id)
            );
            INSERT INTO task_results VALUES
                ('task-1', 'worker-1', 0, 'completed', NULL, NULL,
                 '2026-07-14T10:00:00+00:00', '2026-07-14T10:05:00+00:00');
            """
        )

    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    storage.close()
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()

    row = storage.db.execute(
        "SELECT task_id, run_id, run_resolved FROM task_results",
    ).fetchone()
    assert tuple(row) == ("task-1", 0, None)

    storage.record_task_result(
        "task-1",
        "worker-1",
        1,
        "completed",
        None,
        None,
        "2026-07-14T11:00:00+00:00",
        None,
        "2026-07-14T11:05:00+00:00",
    )
    storage.commit()
    assert storage.db.execute("SELECT COUNT(*) FROM task_results").fetchone()[0] == 2


def test_terminal_collection_returns_all_unseen_runs_with_intervals(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner",
        "worker-type",
        results_dir=tmp_path,
        storage=storage,
        use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(
        classifier,
        "_get_recent_tasks",
        lambda _group, _worker: [{"taskId": "task-1", "runId": 0}, {"taskId": "task-1", "runId": 1}],
    )
    monkeypatch.setattr(
        classifier,
        "_get_task_status",
        lambda _task: {
            "status": {
                "runs": [
                    {
                        "runId": 0,
                        "workerId": "worker-1",
                        "state": "failed",
                        "scheduled": "2026-07-14T09:55:00+00:00",
                        "started": "2026-07-14T10:00:00+00:00",
                        "resolved": "2026-07-14T10:05:00+00:00",
                        "reasonCreated": "scheduled",
                        "reasonResolved": "failed",
                    },
                    {
                        "runId": 1,
                        "workerId": "worker-1",
                        "state": "completed",
                        "scheduled": "2026-07-14T10:08:00+00:00",
                        "started": "2026-07-14T10:10:00+00:00",
                        "resolved": "2026-07-14T10:20:00+00:00",
                        "reasonCreated": "retry",
                        "reasonResolved": "completed",
                    },
                ],
            },
        },
    )

    runs, complete = classifier._new_terminal_tasks("worker-1", "group-1")

    assert complete is True
    assert runs == [
        (
            "task-1",
            0,
            "failed",
            "2026-07-14T10:00:00+00:00",
            "2026-07-14T10:05:00+00:00",
            "failed",
            "2026-07-14T09:55:00+00:00",
            "scheduled",
        ),
        (
            "task-1",
            1,
            "completed",
            "2026-07-14T10:10:00+00:00",
            "2026-07-14T10:20:00+00:00",
            "completed",
            "2026-07-14T10:08:00+00:00",
            "retry",
        ),
    ]
    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], True)


def test_task_window_continuity_uses_stored_runs_without_snapshot_state(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task: {"status": {"runs": []}})

    monkeypatch.setattr(
        classifier, "_get_recent_tasks", lambda _group, _worker: [{"taskId": "first", "runId": 0}],
    )
    _tasks, complete, continuity, window_observed = classifier._new_terminal_tasks_with_continuity("worker-1", "group-1")
    assert (complete, continuity, window_observed) == (False, None, True)  # first nonempty window is a baseline

    monkeypatch.setattr(
        classifier, "_get_recent_tasks", lambda _group, _worker: [{"taskId": "first", "runId": 0}, {"taskId": "next", "runId": 0}],
    )
    _tasks, complete, continuity, window_observed = classifier._new_terminal_tasks_with_continuity("worker-1", "group-1")
    assert (complete, continuity, window_observed) == (False, True, True)

    monkeypatch.setattr(
        classifier, "_get_recent_tasks", lambda _group, _worker: [{"taskId": "unbridged", "runId": 0}],
    )
    _tasks, complete, continuity, window_observed = classifier._new_terminal_tasks_with_continuity("worker-1", "group-1")
    assert (complete, continuity, window_observed) == (False, False, True)

    monkeypatch.setattr(classifier, "_get_recent_tasks", lambda _group, _worker: [])
    _tasks, complete, continuity, window_observed = classifier._new_terminal_tasks_with_continuity("worker-1", "group-1")
    assert (complete, continuity, window_observed) == (True, None, False)


def test_direct_task_collection_treats_idle_to_active_as_continuous(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task: {"status": {"runs": []}})
    windows = iter([
        [],
        [{"taskId": "first", "runId": 0}],
        [{"taskId": "first", "runId": 0}, {"taskId": "next", "runId": 0}],
    ])
    monkeypatch.setattr(classifier, "_get_recent_tasks", lambda _group, _worker: next(windows))

    _tasks, complete, continuity, window_observed = classifier._new_terminal_tasks_with_continuity("worker-1", "group-1")
    assert (complete, continuity, window_observed) == (True, None, False)
    _tasks, complete, continuity, window_observed = classifier._new_terminal_tasks_with_continuity("worker-1", "group-1")
    assert (complete, continuity, window_observed) == (False, True, True)
    _tasks, complete, continuity, window_observed = classifier._new_terminal_tasks_with_continuity("worker-1", "group-1")
    assert (complete, continuity, window_observed) == (False, True, True)
    assert storage.list_task_run_coverage_events(
        "2020-01-01T00:00:00+00:00", "2030-01-01T00:00:00+00:00",
    ) == []


def test_start_lag_backfill_enriches_existing_runs_once_per_task(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    for run_id in (0, 1):
        storage.record_task_result(
            "task-1", "worker-1", run_id, "completed", None, "completed",
            "2026-07-14T10:00:00+00:00", "2026-07-14T10:05:00+00:00", "2026-07-14T10:05:00+00:00",
        )
    storage.commit()
    calls = []

    def status(task_id):
        calls.append(task_id)
        return {"status": {"runs": [
            {"runId": 0, "scheduled": "2026-07-14T09:55:00+00:00", "reasonCreated": "scheduled"},
            {"runId": 1, "scheduled": "2026-07-14T10:06:00+00:00", "reasonCreated": "retry"},
        ]}}

    monkeypatch.setattr(classifier, "_get_task_status", status)
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    result = classifier.backfill_start_lag(batch_size=10, concurrency=1, retries=0)

    assert result == {
        "selected_runs": 2, "selected_tasks": 1, "enriched_runs": 2,
        "expired_tasks": 0, "unmatched_runs": 0, "transient_failures": 0,
    }
    assert calls == ["task-1"]
    rows = storage.db.execute(
        "SELECT run_id, run_scheduled, reason_created FROM task_results ORDER BY run_id",
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (0, "2026-07-14T09:55:00+00:00", "scheduled"),
        (1, "2026-07-14T10:06:00+00:00", "retry"),
    ]
    assert classifier.backfill_start_lag(batch_size=10, concurrency=1, retries=0)["selected_runs"] == 0


def test_start_lag_backfill_excludes_unresolved_observed_runs(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    storage.record_observed_task_run("still-running", "worker-1", 0, "2026-07-14T10:00:00+00:00")
    storage.commit()
    status_calls = []
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    monkeypatch.setattr(
        classifier,
        "_get_task_status",
        lambda task_id: status_calls.append(task_id),
    )

    result = classifier.backfill_start_lag(batch_size=10, concurrency=1, retries=0)

    assert result["selected_runs"] == 0
    assert storage.count_task_runs_missing_schedule() == {"runs": 0, "tasks": 0}
    assert status_calls == []


def test_start_lag_backfill_respects_not_before(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    for task_id, started_at in (
        ("old-task", "2026-07-01T10:00:00+00:00"),
        ("recent-task", "2026-07-14T10:00:00+00:00"),
    ):
        storage.record_task_result(
            task_id, "worker-1", 0, "completed", None, "completed",
            started_at, "2026-07-14T10:05:00+00:00", "2026-07-14T10:05:00+00:00",
        )
    storage.commit()
    calls = []
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    monkeypatch.setattr(
        classifier,
        "_get_task_status",
        lambda task_id: calls.append(task_id) or {"status": {"runs": [{"runId": 0, "scheduled": "2026-07-14T09:55:00+00:00"}]}},
    )

    result = classifier.backfill_start_lag(
        batch_size=10, concurrency=1, retries=0, not_before="2026-07-10T00:00:00+00:00",
    )

    assert result["enriched_runs"] == 1
    assert calls == ["recent-task"]
    assert storage.count_task_runs_missing_schedule() == {"runs": 1, "tasks": 1}


def test_start_lag_backfill_finishes_and_persists_a_stop_requested_batch(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    storage.record_task_result(
        "task-1", "worker-1", 0, "completed", None, "completed",
        "2026-07-14T10:00:00+00:00", "2026-07-14T10:05:00+00:00", "2026-07-14T10:05:00+00:00",
    )
    storage.commit()
    state_file = tmp_path / ".backfill-state.json"
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task_id: {
        "status": {"runs": [{"runId": 0, "scheduled": "2026-07-14T09:55:00+00:00"}]},
    })

    result = classifier.backfill_start_lag(
        batch_size=1, concurrency=1, retries=0, state_file=state_file, should_stop=lambda: True,
    )

    assert result["enriched_runs"] == 1
    assert result["stop_requested"] is True
    assert storage.db.execute("SELECT run_scheduled FROM task_results").fetchone()[0] == "2026-07-14T09:55:00+00:00"
    assert state_file.exists()


def test_start_lag_backfill_reports_expired_status_without_modifying_run(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    storage.record_task_result(
        "expired", "worker-1", 0, "completed", None, "completed",
        "2026-07-14T10:00:00+00:00", "2026-07-14T10:05:00+00:00", "2026-07-14T10:05:00+00:00",
    )
    storage.commit()
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task_id: None)
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)

    result = classifier.backfill_start_lag(batch_size=10, concurrency=1, retries=0)

    assert result["expired_tasks"] == 1
    assert storage.db.execute("SELECT run_scheduled FROM task_results").fetchone()[0] is None


def test_start_lag_backfill_persists_and_pages_past_unavailable_runs(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    for task_id, resolved_at in (
        ("expired", "2026-07-14T10:10:00+00:00"),
        ("available", "2026-07-14T10:05:00+00:00"),
    ):
        storage.record_task_result(
            task_id, "worker-1", 0, "completed", None, "completed",
            "2026-07-14T10:00:00+00:00", resolved_at, resolved_at,
        )
    storage.commit()
    state_file = tmp_path / ".backfill-state.json"
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    monkeypatch.setattr(classifier, "_get_task_status", lambda task_id: None if task_id == "expired" else {
        "status": {"runs": [{"runId": 0, "scheduled": "2026-07-14T09:55:00+00:00"}]},
    })

    first = classifier.backfill_start_lag(batch_size=1, concurrency=1, retries=0, state_file=state_file)
    second = classifier.backfill_start_lag(batch_size=1, concurrency=1, retries=0, state_file=state_file)

    assert first["expired_tasks"] == 1
    assert second["enriched_runs"] == 1
    assert json.loads(state_file.read_text())["pools"]["provisioner/worker-type"]["expired_task_ids"] == ["expired"]


def test_start_lag_backfill_logs_backlog_before_selecting_batch(tmp_path, monkeypatch, caplog):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    for task_id, run_id in (("task-1", 0), ("task-1", 1), ("task-2", 0)):
        storage.record_task_result(
            task_id, "worker-1", run_id, "completed", None, "completed",
            "2026-07-14T10:00:00+00:00", "2026-07-14T10:05:00+00:00", "2026-07-14T10:05:00+00:00",
        )
    storage.commit()
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task_id: None)

    classifier.backfill_start_lag(batch_size=1, concurrency=1, retries=0)

    assert "Start-lag backfill backlog: 3 runs across 2 tasks" in caplog.text


def test_start_lag_dashboard_links_trend_and_heatmap_hover(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()

    html = classifier._write_html({})

    assert "const lagKey" in html
    assert "data-lag-key" in html
    assert "bindLagHover();" in html
    assert ".lag-hm-cell.lag-linked-hover" in html
    assert "box-shadow:inset 0 0 0 2px #fff" in html
    assert '<h2 id="s-start-lag">Start Lag</h2>' in html
    assert "Observed scheduled-to-start time for terminal task runs." in html
    assert '<a href="#s-start-lag">Start Lag</a>' in html
    assert '<a href="#s-heatmap">Worker Activity</a>' in html
    assert "table:not(.not-sortable) th" in html

    heatmap_html = classifier._write_html(
        {},
        heatmap={"worker-1": {0: {"s": 1, "critical": 0, "high": 0, "low": 0}}},
    )
    assert '<table class="hm-grid not-sortable">' in heatmap_html
    assert "Workers are ordered by recent failure severity (critical counts twice), then hostname." in heatmap_html


def test_activity_heatmap_renders_unclassified_with_distinct_color(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)

    html = classifier._write_html(
        {"worker-1": {"failures_by_category": {"unclassified": 1}}},
        quarantined=set(),
        heatmap={"worker-1": {0: {"s": 0, "critical": 0, "high": 0, "low": 0, "unclassified": 1, "cats": {"unclassified": 1}}}},
    )

    assert ".hm-sev-unclassified { background: #5b245f; }" in html
    assert "#hm-tip .tip-unclassified { color: #c86ccd; }" in html
    assert '"unclassified": "unclassified"' in html
    assert 'class="hm-cell hm-sev-unclassified"' in html
    assert 'background:#5b245f"></span>unclassified' in html

    mixed_html = classifier._write_html(
        {},
        heatmap={"worker-1": {0: {"s": 2, "critical": 0, "high": 0, "low": 1, "unclassified": 1, "cats": {}}}},
    )
    assert 'class="hm-cell hm-sev-low"' in mixed_html

    offenders_html = classifier._write_html({"worker-1": {"failures_by_category": {"unclassified": 1}}}, quarantined=set())
    assert '<h2 id="s-offenders">Top Offenders</h2>' in offenders_html
    assert "Workers with the most failures in the last day, grouped by category." in offenders_html
    assert 'href="/pools/provisioner/worker-type/unclassified">unclassified</a>' in offenders_html


def test_postgres_heatmap_does_not_mark_completed_rows_as_unclassified():
    rows = [
        {"worker_id": "worker-1", "hour_ago": 0, "run_state": "failed", "category": "unclassified", "cnt": 1},
        {"worker_id": "worker-2", "hour_ago": 0, "run_state": "completed", "category": "unclassified", "cnt": 4},
    ]

    class Cursor:
        def execute(self, *_args):
            pass

        def fetchall(self):
            return rows

    @contextmanager
    def cursor():
        yield Cursor()

    storage = object.__new__(PostgresStorage)
    storage.pool_id = "pool-id"
    storage._cursor = cursor

    heatmap = storage.query_heatmap("2026-08-18T00:00:00+00:00")

    assert heatmap["worker-1"][0] == {
        "s": 0, "critical": 0, "high": 0, "low": 0, "unclassified": 1,
        "cats": {"unclassified": 1},
    }
    assert heatmap["worker-2"][0]["s"] == 4
    assert heatmap["worker-2"][0]["unclassified"] == 0

def test_utilization_timeline_explains_incomplete_coverage_with_break_diagnostics(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()

    html = classifier._write_html({})

    assert 'const COVERAGE_BREAKS_URL = "/api/v1/pools/provisioner/worker-type/coverage-breaks";' in html
    assert "coverageEventsForBucket" in html
    assert "Recent task windows did not overlap" in html
    assert "group.length >= 5" in html
    assert "Possible general task collection interruption" in html
    assert "collectionInterruption(bucketEvents)" in html
    assert "util-hour-collection-interruption" in html
    assert "${group.length} workers" in html
    assert "windows: ${previous} → ${current}; overlap: ${overlap}" in html
    assert "No retained coverage-break event explains this gap." in html
    assert "Coverage: ${bucket.coverage_pct.toFixed(1)}%" in html


def test_all_workers_summary_describes_tracked_workers_and_all_quarantines(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()

    html = classifier._write_html({"worker-1": {}}, quarantined={"untracked-worker": None})

    assert '<h2 id="s-all">All Workers</h2>' in html
    assert "1 tracked workers &middot; 1 currently quarantined." in html
    assert "Tracked workers have recorded task history; this is not a liveness or readiness check." in html
    assert "workers available" not in html


def test_quarantine_section_uses_a_plain_heading_with_supporting_count(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()

    html = classifier._write_html({}, quarantine_details={"worker-1": {}, "worker-2": {}})

    assert '<h2 id="s-quarantined">Quarantined Workers</h2>' in html
    assert "2 workers currently quarantined." in html
    assert "&#x1F512; Quarantined Workers" not in html


def test_pool_detail_sections_put_pool_health_before_host_debugging(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()

    html = classifier._write_html(
        {
            "worker-1": {
                "successes": 1,
                "failures": 2,
                "failures_by_category": {"test": 2},
                "consecutive_failures": 2,
                "last_failure": "2026-08-07T12:00:00+00:00",
                "last_failure_category": "test",
            },
        },
        quarantined={"worker-1": "2026-08-08T12:00:00+00:00"},
        heatmap={"worker-1": {0: {"s": 1, "critical": 0, "high": 0, "low": 0}}},
        quarantine_details={"worker-1": {}},
    )

    headings = [
        'id="summary-heading"',
        'id="s-job-sources"',
        'id="s-start-lag"',
        'id="s-utilization"',
        'id="s-device-turnaround"',
        'id="s-attention"',
        'id="s-quarantined"',
        'id="s-categories"',
        'id="s-heatmap"',
        'id="s-offenders"',
        'id="s-all"',
    ]
    positions = [html.index(heading) for heading in headings]
    assert positions == sorted(positions)
    assert '.pool-highlights-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr));' in html
    assert '.pool-highlights-grid { grid-template-columns:1fr; }' in html
    assert '<section aria-labelledby="s-device-turnaround">' in html
    assert '<section aria-labelledby="s-attention">' in html


def test_pool_detail_renders_scan_time_busy_device_turnaround(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    monkeypatch.setattr(
        storage,
        "get_busy_turnaround",
        lambda _start, _end: {
            "sample_count": 45,
            "p50_seconds": 130,
            "p95_seconds": 340,
            "available": True,
            "minimum_samples": 30,
        },
    )

    html = classifier.render_html()

    assert 'href="#s-device-turnaround">Device Turnaround</a>' in html
    assert '<h2 id="s-device-turnaround">Device Turnaround</h2>' in html
    assert "2m 10s median" in html
    assert "p95: 5m 40s" in html
    assert "Observed handoffs: 45" in html
    assert "all between-task overhead" in html


def test_terminal_collection_reports_incomplete_worker_poll(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner",
        "worker-type",
        results_dir=tmp_path,
        storage=storage,
        use_color=False,
    )
    classifier._init_db()

    def fail_recent_tasks(_group, _worker):
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(classifier, "_get_recent_tasks", fail_recent_tasks)
    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], False)


def test_observed_run_retries_after_restart_and_reaches_terminal_path(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(
        classifier, "_get_recent_tasks", lambda _group, _worker: [{"taskId": "task-1", "runId": 0}],
    )
    monkeypatch.setattr(classifier, "_get_task_status", lambda _task: (_ for _ in ()).throw(RuntimeError("Queue busy")))

    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], False)
    assert storage.list_unresolved_task_runs(10)[0]["task_id"] == "task-1"

    restarted = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path,
        storage=SqliteStorage("provisioner/worker-type", tmp_path), use_color=False,
    )
    restarted._init_db()
    monkeypatch.setattr(
        restarted,
        "_get_task_status",
        lambda _task: {"status": {"runs": [{
            "runId": 0, "workerId": "worker-1", "state": "completed",
            "started": "2026-07-14T10:00:00+00:00", "resolved": "2026-07-14T10:05:00+00:00",
        }]}},
    )

    recovered, complete = restarted._retry_unresolved_task_runs()
    assert complete is True
    restarted._process_results("worker-1", recovered["worker-1"], worker_group="group-1")
    row = restarted.storage.db.execute(
        "SELECT run_state, observed_at, last_checked_at FROM task_results WHERE task_id = 'task-1'",
    ).fetchone()
    assert row["run_state"] == "completed"
    assert row["observed_at"] < row["last_checked_at"]


def test_classify_cycle_recovers_observed_run_after_restart(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    monkeypatch.setattr(
        classifier, "_get_recent_tasks", lambda _group, _worker: [{"taskId": "task-1", "runId": 0}],
    )
    monkeypatch.setattr(
        classifier, "_get_task_status",
        lambda _task: (_ for _ in ()).throw(RuntimeError("Queue busy")),
    )
    classifier.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])
    assert storage.list_unresolved_task_runs(10)[0]["run_state"] == "observed"

    restarted = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path,
        storage=SqliteStorage("provisioner/worker-type", tmp_path), use_color=False,
    )
    restarted._init_db()
    monkeypatch.setattr(restarted, "_update_reports", lambda: None)
    monkeypatch.setattr(restarted, "_get_recent_tasks", lambda _group, _worker: [])
    monkeypatch.setattr(
        restarted, "_get_task_status",
        lambda _task: {"status": {"runs": [{
            "runId": 0, "workerId": "worker-1", "state": "completed",
            "started": "2026-07-14T10:00:00+00:00", "resolved": "2026-07-14T10:05:00+00:00",
        }]}},
    )

    restarted.classify_cycle(workers=[{"workerId": "worker-1", "workerGroup": "group-1"}])
    assert restarted.storage.db.execute(
        "SELECT run_state FROM task_results WHERE task_id = 'task-1'",
    ).fetchone()[0] == "completed"


def test_terminal_collection_persists_queue_404_as_expired(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner", "worker-type", results_dir=tmp_path, storage=storage, use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(
        classifier, "_get_recent_tasks", lambda _group, _worker: [{"taskId": "expired-task", "runId": 0}],
    )
    status_calls = []
    monkeypatch.setattr(classifier, "_get_task_status", lambda task_id: status_calls.append(task_id) and None)

    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], True)
    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], True)
    assert status_calls == ["expired-task"]
    assert storage.db.execute(
        "SELECT run_state FROM task_results WHERE task_id = 'expired-task'",
    ).fetchone()[0] == "expired"


def test_terminal_collection_skips_expired_task_status_references(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner",
        "worker-type",
        results_dir=tmp_path,
        storage=storage,
        use_color=False,
    )
    classifier._init_db()
    monkeypatch.setattr(
        classifier,
        "_get_recent_tasks",
        lambda _group, _worker: [{"taskId": "expired-task", "runId": 0}],
    )
    status_calls = []

    def expired_status(task_id):
        status_calls.append(task_id)
        return None  # Queue status endpoint returned a definitive 404.

    monkeypatch.setattr(classifier, "_get_task_status", expired_status)

    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], True)
    # The tombstone stays in the in-process seen set, avoiding repeated 404s.
    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], True)
    assert status_calls == ["expired-task"]
