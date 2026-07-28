from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import json

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
