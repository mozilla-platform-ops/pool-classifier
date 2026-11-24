from collections import Counter
from datetime import datetime, timezone
from threading import Barrier, Lock, get_ident
from types import SimpleNamespace

import taskcluster

from worker_health import pool_classifier as classifier_module
from worker_health.pool_classifier import PoolClassifier
from worker_health.pool_classifier_web import app as app_module
from worker_health.pool_classifier_web.job_families import task_job_metadata
from worker_health.pool_classifier_web.job_sources import AUDIT_WORKER_SOURCE, SourceMethod, classify_job_source
from worker_health.pool_classifier_web.storage import SqliteStorage


def test_project_tag_wins_over_other_signals():
    result = classify_job_source({"tags": {"project": "try"}, "metadata": {"source": AUDIT_WORKER_SOURCE}})
    assert result.source == "try"
    assert result.method is SourceMethod.PROJECT_TAG


def test_audit_worker_is_explicitly_mapped():
    result = classify_job_source({"tags": {}, "metadata": {"source": AUDIT_WORKER_SOURCE}})
    assert result.source == "audit-worker"
    assert result.method is SourceMethod.METADATA_SOURCE


def test_missing_project_is_unknown_without_scheduler_inference():
    result = classify_job_source({"tags": {}, "schedulerId": "gecko-level-3"})
    assert result.source == "unknown"
    assert result.method is SourceMethod.MISSING_PROJECT_TAG


def test_unavailable_task_is_unknown():
    assert classify_job_source(None).method is SourceMethod.TASK_FETCH_FAILED


def test_source_volume_uses_cached_source_and_unknown(tmp_path):
    storage = SqliteStorage("proj/worker", tmp_path)
    storage.init_schema()
    storage.record_task_result("known", "worker", 0, "completed", None, None, "2026-08-05T12:00:00+00:00", None, "2026-08-05T12:01:00+00:00")
    storage.record_task_result("missing", "worker", 0, "completed", None, None, "2026-08-05T13:00:00+00:00", None, "2026-08-05T13:01:00+00:00")
    storage.record_task_source("known", "try", int(SourceMethod.PROJECT_TAG), "2026-08-05T12:01:00+00:00")
    assert storage.get_job_source_volume("2026-08-05T00:00:00+00:00", "2026-08-06T00:00:00+00:00") == [
        {"day": "2026-08-05", "source": "try", "tasks": 1},
        {"day": "2026-08-05", "source": "unknown", "tasks": 1},
    ]


def test_job_family_uses_test_suite_and_build_kind():
    assert task_job_metadata({"metadata": {"name": "test-android-browsertime-tp6m-1"}}) == (
        "test-android-browsertime-tp6m-1", "Page load",
    )
    assert task_job_metadata({"metadata": {"name": "test-linux-mochitest-media-2"}}) == (
        "test-linux-mochitest-media-2", "Mochitest media",
    )
    assert task_job_metadata({"extra": {"treeherder": {"jobKind": "build"}}}) == (None, "Builds")
    assert task_job_metadata({"metadata": {"name": "unmapped-job"}}) == ("unmapped-job", "Unknown")


def test_terminal_batches_fetch_definitions_concurrently_and_deduplicate(tmp_path, monkeypatch):
    storage = SqliteStorage("proj/worker", tmp_path)
    classifier = PoolClassifier("proj", "worker", results_dir=tmp_path, storage=storage, use_color=False)
    classifier._init_db()
    monkeypatch.setattr(classifier_module, "TASK_DEFINITION_BATCH_SIZE", 5)
    start, end = "2026-08-05T12:00:00+00:00", "2026-08-05T12:01:00+00:00"
    storage.record_task_source("cached", "try", int(SourceMethod.PROJECT_TAG), end)
    storage.commit()
    barrier = Barrier(4, timeout=3)
    lock = Lock()
    calls = Counter()
    fetch_threads = set()
    classifier_thread = get_ident()

    def fetch(task_id):
        with lock:
            calls[task_id] += 1
            fetch_threads.add(get_ident())
        if task_id in {"a", "b", "c", "unavailable"}:
            barrier.wait()
        if task_id == "unavailable":
            raise RuntimeError("temporary API failure")
        return {"tags": {"project": "try"}, "metadata": {"name": f"browsertime-speedometer3-{task_id}"}}

    classifier.tc_queue = SimpleNamespace(task=fetch)

    def terminal(task_id, run_id=0):
        return (task_id, run_id, "completed", start, end, None)

    classified = classifier._process_terminal_batches([
        ("worker-1", "group", [terminal(task) for task in ("cached", "a", "b", "c", "unavailable")]),
        ("worker-2", "group", [terminal("a", 1), terminal("unavailable", 1), terminal("d")]),
    ], None)

    assert len(fetch_threads) == 4 and classifier_thread not in fetch_threads
    assert calls == Counter({task: 1 for task in ("a", "b", "c", "unavailable", "d")})
    assert classified == {}
    assert storage.get_task_source("a") == {"source": "try", "source_method": int(SourceMethod.PROJECT_TAG)}
    assert storage.get_task_source("unavailable")["source_method"] == int(SourceMethod.TASK_FETCH_FAILED)
    assert storage.db.execute("SELECT COUNT(*) FROM task_results WHERE run_state = 'completed'").fetchone()[0] == 8


def test_job_family_report_clips_runs_and_reuses_task_metadata(tmp_path, monkeypatch):
    storage = SqliteStorage("proj/worker", tmp_path)
    storage.init_schema()
    classifier = PoolClassifier("proj", "worker", results_dir=tmp_path, storage=storage, use_color=False)
    midnight = datetime(2026, 8, 6, tzinfo=timezone.utc)
    name = "test-android-browsertime-speedometer3"
    calls = []

    def fetch(task_id):
        calls.append(task_id)
        return {"metadata": {"name": name}, "tags": {"project": "try"}}

    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    classifier.tc_queue = SimpleNamespace(task=fetch)
    classifier._record_job_source("known", midnight.isoformat())
    classifier._record_job_source("known", midnight.isoformat())
    assert calls == ["known"]

    def run(task, run_id, start, end, state="completed"):
        storage.record_task_result(task, "worker", run_id, state, None, None, start, end, midnight.isoformat())

    run("known", 0, "2026-08-04T23:00:00+00:00", "2026-08-05T01:00:00+00:00")
    run("known", 1, "2026-08-05T23:30:00+00:00", "2026-08-06T00:30:00+00:00", "failed")
    run("legacy", 0, "2026-08-05T01:00:00+00:00", "2026-08-05T02:00:00+00:00")
    run("before", 0, "2026-08-04T23:00:00+00:00", "2026-08-05T00:00:00+00:00")
    run("after", 0, "2026-08-06T00:00:00+00:00", "2026-08-06T01:00:00+00:00")
    run("open", 0, "2026-08-05T01:00:00+00:00", None, "running")
    storage.commit()

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return midnight.astimezone(tz)

    monkeypatch.setattr(app_module, "datetime", FixedDateTime)
    monkeypatch.setattr(app_module, "_get_classifier", lambda *_args: classifier)
    client = app_module.create_app().test_client()
    url = "/api/v1/pools/proj/worker/job-families"
    assert client.get(url).get_json()["jobs"] == [
        {"job_family": "Benchmarks", "task_name": name, "runs": 2, "worker_seconds": 5400.0},
        {"job_family": "Unknown", "task_name": None, "runs": 1, "worker_seconds": 3600.0},
    ]
    assert client.get(f"{url}?days=3").get_json()["days"] == 3
    assert client.get(f"{url}?days=7").get_json()["days"] == 7
    assert client.get(f"{url}?days=14").status_code == 400
    assert calls == ["known"]
    html = classifier.render_html()
    assert 'data-endpoint="/api/v1/pools/proj/worker/job-families"' in html
    assert '<div data-suite-explorer hidden></div>' in html
    assert 'src="/static/job-families.js?v=2"' in html


def test_task_source_chart_uses_accessible_colors_order_and_custom_tooltip(tmp_path):
    storage = SqliteStorage("proj/worker", tmp_path)
    storage.init_schema()
    classifier = PoolClassifier("proj", "worker", results_dir=tmp_path, storage=storage, use_color=False)

    html = classifier.render_html()

    assert "const SOURCE_COLORS=['#56b4e9','#e69f00','#009e73'" in html
    assert "source==='unknown'?'#777'" in html
    assert "sourceColors=new Map(sources.map" in html
    assert ".source-segment:focus-visible" in html
    assert "tabindex='0'" in html
    assert "sourceTotals=new Map(sources.map(s=>[s,0]))" in html
    assert "displaySources=[...sources].sort((a,b)=>sourceTotals.get(b)-sourceTotals.get(a)||a.localeCompare(b))" in html
    assert "rows.map(b=>{const key=encodeURIComponent(`${d}:${b.source}`);tooltipDetails.set" in html
    assert 'id="source-tooltip" class="source-tooltip" role="tooltip"' in html
    assert ".source-tooltip-row.active { background:#2a3545" in html
    assert "padding:.18rem .25rem .18rem .6rem" in html
    assert "grid-template-columns:minmax(0,1fr) auto" in html
    assert "font-variant-numeric:tabular-nums" in html
    assert "max-height:calc(100vh - 1rem); overflow-y:auto" in html
    assert "fmtSourcePct=(tasks,total)" in html
    assert "aria-describedby='source-tooltip'" in html
    assert "segment.addEventListener('focus',show)" in html
    assert "if(event.key==='Escape')hideSourceTooltip()" in html
    assert "SOURCE_DAYS_KEY='pc-task-source-days'" in html
    assert "localStorage.getItem(SOURCE_DAYS_KEY)" in html
    assert "localStorage.setItem(SOURCE_DAYS_KEY,String(sourceDays))" in html
    assert "grid-template-columns:3rem minmax(0,1fr)" in html
    assert ".source-gridline" in html
    assert "sourceAxis=max=>" in html
    assert "ticks>=2&&ticks<=4" in html
    assert "fmtSourceAxis=value=>value>=1000?`${value/1000}k`:String(value)" in html
    assert "<div class='source-axis' aria-label='Task count axis'>" in html
    assert "<div class='source-grid' aria-hidden='true'>" in html
    assert "let h=2166136261" not in html


def test_job_source_backfill_is_bounded_idempotent_and_preserves_no_inference_policy(tmp_path, monkeypatch):
    storage = SqliteStorage("proj/worker", tmp_path)
    storage.init_schema()
    for task_id in ("project", "audit", "unknown", "old"):
        started = "2026-08-05T12:00:00+00:00" if task_id != "old" else "2026-07-01T12:00:00+00:00"
        storage.record_task_result(task_id, "worker", 0, "completed", None, None, started, None, started)
    storage.commit()
    classifier = PoolClassifier("proj", "worker", results_dir=tmp_path, storage=storage, use_color=False)
    tasks = {
        "project": {"tags": {"project": "try"}},
        "audit": {"metadata": {"source": AUDIT_WORKER_SOURCE}},
        "unknown": {"schedulerId": "gecko-level-3"},
    }
    calls = []
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    classifier.tc_queue = type("Queue", (), {"task": lambda _self, task_id: calls.append(task_id) or tasks[task_id]})()
    monkeypatch.setattr(
        storage,
        "classify_lock",
        lambda: (_ for _ in ()).throw(AssertionError("job-source backfill must not block classification")),
    )

    first = classifier.backfill_job_sources(
        batch_size=2, concurrency=1, retries=0, requests_per_second=1000,
        not_before="2026-08-01T00:00:00+00:00",
    )
    second = classifier.backfill_job_sources(
        batch_size=2, concurrency=1, retries=0, requests_per_second=1000,
        not_before="2026-08-01T00:00:00+00:00",
    )
    final = classifier.backfill_job_sources(
        batch_size=2, concurrency=1, retries=0, requests_per_second=1000,
        not_before="2026-08-01T00:00:00+00:00",
    )

    assert first["selected_tasks"] == 2
    assert second["selected_tasks"] == 1
    assert final["selected_tasks"] == 0
    assert set(calls) == {"project", "audit", "unknown"}
    assert storage.get_task_source("project") == {"source": "try", "source_method": 0}
    assert storage.get_task_source("audit") == {"source": "audit-worker", "source_method": 1}
    assert storage.get_task_source("unknown") == {"source": "unknown", "source_method": 2}
    assert storage.get_task_source("old") is None


def test_job_source_backfill_leaves_failed_fetches_eligible_for_resume(tmp_path, monkeypatch):
    storage = SqliteStorage("proj/worker", tmp_path)
    storage.init_schema()
    storage.record_task_result("retry-me", "worker", 0, "completed", None, None, "2026-08-05T12:00:00+00:00", None, "2026-08-05T12:00:00+00:00")
    storage.commit()
    classifier = PoolClassifier("proj", "worker", results_dir=tmp_path, storage=storage, use_color=False)
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    classifier.tc_queue = type("Queue", (), {"task": lambda *_args: (_ for _ in ()).throw(RuntimeError("temporary"))})()

    result = classifier.backfill_job_sources(batch_size=1, concurrency=1, retries=0, requests_per_second=1000)

    assert result["errors"] == 1
    assert storage.get_task_source("retry-me") is None


def test_job_source_backfill_records_permanently_unavailable_tasks_as_unknown(tmp_path, monkeypatch):
    storage = SqliteStorage("proj/worker", tmp_path)
    storage.init_schema()
    storage.record_task_result("legacy-invalid-id", "worker", 0, "completed", None, None, "2026-08-05T12:00:00+00:00", None, "2026-08-05T12:00:00+00:00")
    storage.commit()
    classifier = PoolClassifier("proj", "worker", results_dir=tmp_path, storage=storage, use_color=False)
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    task_error = taskcluster.exceptions.TaskclusterRestFailure("invalid task ID", status_code=400, superExc=None)
    classifier.tc_queue = type("Queue", (), {"task": lambda *_args: (_ for _ in ()).throw(task_error)})()

    result = classifier.backfill_job_sources(batch_size=1, concurrency=1, retries=0, requests_per_second=1000)

    assert result == {"selected_tasks": 1, "fetched": 0, "classified": 1, "unknown": 1, "errors": 0}
    assert storage.get_task_source("legacy-invalid-id") == {
        "source": "unknown", "source_method": int(SourceMethod.TASK_FETCH_FAILED),
    }


def test_job_source_backfill_rate_limits_each_task_definition_request(tmp_path, monkeypatch):
    storage = SqliteStorage("proj/worker", tmp_path)
    storage.init_schema()
    for task_id in ("one", "two"):
        storage.record_task_result(task_id, "worker", 0, "completed", None, None, "2026-08-05T12:00:00+00:00", None, "2026-08-05T12:00:00+00:00")
    storage.commit()
    classifier = PoolClassifier("proj", "worker", results_dir=tmp_path, storage=storage, use_color=False)
    waits = []
    monkeypatch.setattr(classifier, "_ensure_tc", lambda: None)
    monkeypatch.setattr("worker_health.pool_classifier._RequestRateLimiter.wait", lambda _self: waits.append(1))
    classifier.tc_queue = type("Queue", (), {"task": lambda _self, _task_id: {"tags": {"project": "try"}}})()

    classifier.backfill_job_sources(batch_size=2, concurrency=1, retries=0, requests_per_second=1)

    assert waits == [1, 1]
