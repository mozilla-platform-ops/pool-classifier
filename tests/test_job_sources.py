import taskcluster

from worker_health.pool_classifier import PoolClassifier
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
