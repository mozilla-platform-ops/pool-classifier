from __future__ import annotations

import sqlite3

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
                        "started": "2026-07-14T10:00:00+00:00",
                        "resolved": "2026-07-14T10:05:00+00:00",
                        "reasonResolved": "failed",
                    },
                    {
                        "runId": 1,
                        "workerId": "worker-1",
                        "state": "completed",
                        "started": "2026-07-14T10:10:00+00:00",
                        "resolved": "2026-07-14T10:20:00+00:00",
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
        ),
        (
            "task-1",
            1,
            "completed",
            "2026-07-14T10:10:00+00:00",
            "2026-07-14T10:20:00+00:00",
            "completed",
        ),
    ]
    assert classifier._new_terminal_tasks("worker-1", "group-1") == ([], True)


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
