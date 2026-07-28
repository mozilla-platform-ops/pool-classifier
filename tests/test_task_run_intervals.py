from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import json
import logging

from worker_health.pool_classifier import PoolClassifier
from worker_health.pool_classifier_web.storage import SqliteStorage


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
            {"runId": 0, "workerId": "worker-a", "state": "completed"},
            {"runId": 1, "workerId": "worker-b", "state": "completed"},
        ]}}

    monkeypatch.setattr(classifier, "_get_task_status", status)
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)
    summary = classifier.classify_cycle(workers=[
        {"workerId": "worker-a", "workerGroup": "group-a"},
        {"workerId": "worker-b", "workerGroup": "group-b"},
    ])

    assert summary["new_terminal"] == 2
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

    offenders_html = classifier._write_html({"worker-1": {"failures_by_category": {"test": 1}}}, quarantined=set())
    assert '<h2 id="s-offenders">Top Offenders</h2>' in offenders_html
    assert "Workers with the most failures in the last day, grouped by category." in offenders_html


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
