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
