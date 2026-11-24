import sqlite3
from types import SimpleNamespace

import pytest

from worker_health.pool_classifier import PoolClassifier
from worker_health.pool_classifier_web.scripts import retry_task_status
from worker_health.pool_classifier_web.storage import SqliteStorage


FIRST = "2026-08-05T10:00:00+00:00"
LAST = "2026-08-05T10:15:00+00:00"
POOL = "test/pool"


def test_sqlite_migrates_legacy_run_evidence_once(tmp_path):
    db_path = tmp_path / "pool_classifier.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE task_results (task_id TEXT, worker_id TEXT, run_id INTEGER, run_state TEXT NOT NULL,
                category TEXT, reason_resolved TEXT, run_started TEXT, classified_at TEXT NOT NULL,
                observed_at TEXT, last_checked_at TEXT)
        """)
        conn.executemany(
            "INSERT INTO task_results VALUES (?, 'w', 0, ?, ?, NULL, NULL, ?, ?, ?)",
            [(task, state, category, LAST, FIRST, LAST) for task, state, category in [
                ("reference", "observed", None), ("gone", "expired", None),
                ("success", "completed", None), ("classified", "failed", "infra"),
                ("recover", "exception", None),
            ]],
        )
    for _ in range(2):
        storage = SqliteStorage(POOL, tmp_path)
        storage.init_schema()
        rows = storage.db.execute(
            "SELECT task_id, run_state, classified_at, classification_state, status_fetch_state,"
            " status_retry_enabled, observed_at, last_checked_at, status_fetched_at FROM task_results"
        )
        assert {row[0]: tuple(row)[1:] for row in rows} == {
            "reference": (None, None, "unknown", "unknown", 1, FIRST, LAST, None),
            "gone": (None, None, "unknown", "not_found", 0, FIRST, LAST, None),
            "success": ("completed", None, "not_applicable", "available", 0, FIRST, LAST, None),
            "classified": ("failed", LAST, "classified", "available", 0, FIRST, LAST, None),
            "recover": ("exception", None, "pending", "available", 0, FIRST, LAST, None),
        }
        with pytest.raises(sqlite3.IntegrityError):
            storage.db.execute(
                "INSERT INTO task_results (task_id, worker_id, run_state, classified_at) VALUES ('old', 'w', 'observed', ?)",
                (LAST,),
            )
        storage.close()


def test_operator_retries_only_the_selected_unavailable_run(tmp_path):
    storage = SqliteStorage(POOL, tmp_path)
    storage.init_schema()
    for run_id in (None, 0):
        storage.record_observed_task_run("retry", "w", run_id, FIRST)
        assert storage.expire_task_run("retry", run_id, LAST)
    storage.commit()
    storage.close()

    command = ["--pool", POOL, "--task-id", "retry", "--run-id", "legacy", "--results-dir", str(tmp_path)]
    assert retry_task_status.main(command) == 0
    assert retry_task_status.main(command) == 1
    with sqlite3.connect(tmp_path / "pool_classifier.db") as conn:
        assert conn.execute(
            "SELECT run_id, status_fetch_state, status_retry_enabled, last_checked_at"
            " FROM task_results ORDER BY run_id"
        ).fetchall() == [(None, "not_found", 1, LAST), (0, "not_found", 0, LAST)]


def test_pending_classification_and_task_name_resume_without_another_status_lookup(tmp_path, monkeypatch):
    worker = {"workerId": "w", "workerGroup": "g"}
    name_calls = []

    def make_classifier():
        storage = SqliteStorage(POOL, tmp_path)
        classifier = PoolClassifier("test", "pool", storage=storage, results_dir=tmp_path, use_color=False)
        classifier._init_db()
        monkeypatch.setattr(classifier, "_update_reports", lambda: None)
        monkeypatch.setattr(classifier, "_record_worker_availability", lambda _workers: 0)
        monkeypatch.setattr(classifier, "_get_recent_tasks", lambda *_: [{"taskId": "job", "runId": 0}])
        return classifier, storage

    classifier, storage = make_classifier()
    run = {"runId": 0, "workerId": "w", "state": "running", "started": FIRST}
    monkeypatch.setattr(classifier, "_get_task_status", lambda _: {"status": {"runs": [run]}})

    def fail_name(task_id):
        name_calls.append(task_id)
        raise RuntimeError("temporary metadata failure")

    classifier.tc_queue = SimpleNamespace(task=fail_name)
    classifier.classify_cycle(workers=[worker])
    assert tuple(storage.db.execute(
        "SELECT run_state, run_started, classification_state, classified_at FROM task_results",
    ).fetchone()) == ("running", FIRST, "unknown", None)

    run.update(state="failed", resolved=LAST)

    def retry_log(*_):
        with sqlite3.connect(tmp_path / "pool_classifier.db") as db:
            assert db.execute("SELECT run_state, classification_state, classified_at FROM task_results").fetchone() == (
                "failed", "pending", None,
            )
        return "", "retry"

    monkeypatch.setattr(classifier, "_fetch_log_tail", retry_log)
    classifier.classify_cycle(workers=[worker])
    assert storage.query_workers()["w"]["failures"] == 1
    assert name_calls == ["job", "job"]
    storage.close()

    restarted, storage = make_classifier()
    status_calls = []
    monkeypatch.setattr(restarted, "_get_task_status", lambda task_id: status_calls.append(task_id))
    monkeypatch.setattr(restarted, "_fetch_log_tail", lambda *_: ("", "unavailable"))
    restarted.tc_queue = SimpleNamespace(task=lambda _: {"metadata": {"name": "browsertime-tp6m-fenix"}})
    assert restarted.classify_cycle(workers=[worker])["new_terminal"] == 0
    assert status_calls == []
    assert storage.query_workers()["w"]["failures"] == 1
    category, classified_at = storage.db.execute("SELECT category, classified_at FROM task_results").fetchone()
    assert category == "unclassified" and classified_at is not None
    assert tuple(storage.db.execute("SELECT task_name, job_family, name_retry_enabled FROM task_sources").fetchone()) == (
        "browsertime-tp6m-fenix", "Page load", 0,
    )

    monkeypatch.setattr(restarted, "_fetch_log_tail", lambda *_: ("", "retry"))
    restarted.reclassify_unclassified()
    assert tuple(restarted.storage.db.execute("SELECT category, classified_at FROM task_results").fetchone()) == (
        category, classified_at,
    )
    monkeypatch.setattr(restarted, "_fetch_log_tail", lambda *_: ("resolved log", "ok"))
    monkeypatch.setattr(restarted, "_classify", lambda *_: "infra")
    restarted.reclassify_unclassified()
    category, replacement_at = restarted.storage.db.execute("SELECT category, classified_at FROM task_results").fetchone()
    assert category == "infra" and replacement_at > classified_at
