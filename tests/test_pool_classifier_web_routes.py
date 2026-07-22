from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from worker_health.pool_classifier_web import app as app_module
from worker_health.pool_classifier_web.app import create_app
from worker_health.pool_classifier_web.storage import SqliteStorage


API_PATH = "/api/v1/pools/provisioner/worker-type/utilization"
API_START = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)


def _api_storage(tmp_path, available=True, coverage_minutes=60):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    end = API_START + timedelta(minutes=coverage_minutes)
    for source in ("task_runs", "worker_availability"):
        storage.record_collection_coverage(source, API_START.isoformat(), True, 3600)
        storage.record_collection_coverage(source, end.isoformat(), True, 3600)
    storage.record_worker_availability_transition(
        "worker-1",
        "group-1",
        available,
        False,
        API_START.isoformat(),
        None,
        "online" if available else "contact_timeout",
        API_START.isoformat(),
        API_START.isoformat(),
    )
    if available:
        storage.record_task_result(
            "task-1",
            "worker-1",
            0,
            "completed",
            None,
            None,
            (API_START + timedelta(minutes=15)).isoformat(),
            (API_START + timedelta(minutes=45)).isoformat(),
            (API_START + timedelta(minutes=45)).isoformat(),
        )
    storage.commit()
    return storage


def _api_client(monkeypatch, storage, availability_mode="recent_contact"):
    classifier = SimpleNamespace(storage=storage, availability_mode=availability_mode)
    monkeypatch.setattr(
        app_module,
        "_get_classifier",
        lambda provisioner, worker_type: (
            classifier
            if (provisioner, worker_type) == ("provisioner", "worker-type")
            else None
        ),
    )
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


def test_favicon_serves_svg_icon():
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/favicon.ico")

    assert response.status_code == 200
    assert response.content_type.startswith("image/svg+xml")
    assert b"<svg" in response.data


def test_classify_all_logs_summary_counts(monkeypatch, caplog):
    pool_ok = SimpleNamespace(provisioner="proj", worker_type="ok")
    pool_busy = SimpleNamespace(provisioner="proj", worker_type="busy")

    class OkClassifier:
        def classify_cycle(self):
            return {"scanned": 1}

    def fake_get_classifier(provisioner, worker_type):
        if worker_type == "ok":
            return OkClassifier()
        raise app_module.ClassifyLockBusy("busy")

    monkeypatch.delenv("CLASSIFY_OIDC_AUDIENCE", raising=False)
    monkeypatch.setattr(app_module.registry, "all_pools", lambda: [pool_ok, pool_busy])
    monkeypatch.setattr(app_module, "_get_classifier", fake_get_classifier)

    app = create_app()
    app.config["TESTING"] = True

    with caplog.at_level(logging.INFO, logger="worker_health.pool_classifier_web.app"):
        with app.test_client() as client:
            response = client.post("/classify-all")

    assert response.status_code == 200
    assert response.json["status_counts"] == {"busy": 1, "ok": 1}
    assert "classify-all summary: pools=2 ok=1 busy=1 error=0 not_found=0" in caplog.text


def test_classify_all_warns_on_partial_failure(monkeypatch, caplog):
    pool_ok = SimpleNamespace(provisioner="proj", worker_type="ok")
    pool_error = SimpleNamespace(provisioner="proj", worker_type="error")

    class OkClassifier:
        def classify_cycle(self):
            return {"scanned": 1}

    class ErrorClassifier:
        def classify_cycle(self):
            raise RuntimeError("db unavailable")

    def fake_get_classifier(provisioner, worker_type):
        return OkClassifier() if worker_type == "ok" else ErrorClassifier()

    monkeypatch.delenv("CLASSIFY_OIDC_AUDIENCE", raising=False)
    monkeypatch.setattr(app_module.registry, "all_pools", lambda: [pool_ok, pool_error])
    monkeypatch.setattr(app_module, "_get_classifier", fake_get_classifier)

    app = create_app()
    app.config["TESTING"] = True

    with caplog.at_level(logging.WARNING, logger="worker_health.pool_classifier_web.app"):
        with app.test_client() as client:
            response = client.post("/classify-all")

    assert response.status_code == 200
    assert response.json["status_counts"] == {"error": 1, "ok": 1}
    assert "classify-all summary: pools=2 ok=1 busy=0 error=1 not_found=0" in caplog.text


def test_utilization_api_filters_range_and_buckets(monkeypatch, tmp_path):
    client = _api_client(monkeypatch, _api_storage(tmp_path))
    response = client.get(
        API_PATH,
        query_string={
            "start": API_START.isoformat(),
            "end": (API_START + timedelta(hours=1)).isoformat(),
            "bucket_seconds": "1800",
        },
    )

    assert response.status_code == 200
    assert response.json["api_version"] == 1
    assert response.json["availability_mode"] == "recent_contact"
    assert response.json["pool_id"] == "provisioner/worker-type"
    assert response.json["start_at"] == API_START.isoformat()
    assert response.json["end_at"] == (API_START + timedelta(hours=1)).isoformat()
    assert response.json["bucket_seconds"] == 1800
    assert response.json["collection_started"] == API_START.isoformat()
    assert response.json["coverage_pct"] == 100
    assert response.json["complete"] is True
    assert set(response.json) == {
        "api_version",
        "availability_mode",
        "pool_id",
        "start_at",
        "end_at",
        "bucket_seconds",
        "collection_started",
        "coverage_pct",
        "complete",
        "buckets",
    }
    assert len(response.json["buckets"]) == 2
    assert set(response.json["buckets"][0]) == {
        "start_at",
        "end_at",
        "coverage_pct",
        "complete",
        "status",
        "busy_worker_hours",
        "available_worker_hours",
        "worker_equivalents",
        "utilization_pct",
    }
    assert [bucket["busy_worker_hours"] for bucket in response.json["buckets"]] == [0.25, 0.25]
    assert [bucket["utilization_pct"] for bucket in response.json["buckets"]] == [50, 50]


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ({}, "start is required"),
        ({"start": "not-a-date"}, "start must be an ISO 8601 timestamp"),
        ({"start": "2026-07-21T10:00:00"}, "start must include a timezone"),
        (
            {"start": "2026-07-21T11:00:00Z", "end": "2026-07-21T10:00:00Z", "bucket_seconds": "60"},
            "end must be after start",
        ),
        (
            {"start": "2026-07-21T10:00:00Z", "end": "2026-07-21T11:00:00Z"},
            "bucket_seconds is required",
        ),
        (
            {"start": "2026-07-21T10:00:00Z", "end": "2026-07-21T11:00:00Z", "bucket_seconds": "1.5"},
            "bucket_seconds must be an integer",
        ),
        (
            {"start": "2026-07-21T10:00:00Z", "end": "2026-07-21T11:00:00Z", "bucket_seconds": "0"},
            "bucket_seconds must be greater than zero",
        ),
        (
            {"start": "2026-07-21T10:00:00Z", "end": "2026-07-21T11:00:00Z", "bucket_seconds": "7776001"},
            "bucket_seconds must not exceed 7776000",
        ),
        (
            {"start": "2026-07-21T10:00:00Z", "end": "2026-07-21T11:00:00Z", "bucket_seconds": "1"},
            "bucket_seconds would produce more than 2000 buckets",
        ),
        (
            {"start": "2026-01-01T00:00:00Z", "end": "2026-04-02T00:00:00Z", "bucket_seconds": "86400"},
            "time range must not exceed 90 days",
        ),
    ],
)
def test_utilization_api_rejects_invalid_parameters(monkeypatch, tmp_path, query, message):
    client = _api_client(monkeypatch, _api_storage(tmp_path))
    response = client.get(API_PATH, query_string=query)

    assert response.status_code == 400
    assert response.json == {"error": {"code": "invalid_parameter", "message": message}}


def test_utilization_api_zero_availability(monkeypatch, tmp_path):
    client = _api_client(monkeypatch, _api_storage(tmp_path, available=False))
    response = client.get(
        API_PATH,
        query_string={
            "start": API_START.isoformat(),
            "end": (API_START + timedelta(hours=1)).isoformat(),
            "bucket_seconds": "3600",
        },
    )

    bucket = response.json["buckets"][0]
    assert bucket["status"] == "unavailable"
    assert bucket["available_worker_hours"] == 0
    assert bucket["utilization_pct"] is None


def test_utilization_api_incomplete_data(monkeypatch, tmp_path):
    client = _api_client(monkeypatch, _api_storage(tmp_path, coverage_minutes=30))
    response = client.get(
        API_PATH,
        query_string={
            "start": API_START.isoformat(),
            "end": (API_START + timedelta(hours=1)).isoformat(),
            "bucket_seconds": "3600",
        },
    )

    assert response.json["coverage_pct"] == 50
    assert response.json["complete"] is False
    bucket = response.json["buckets"][0]
    assert bucket["status"] == "incomplete"
    assert bucket["busy_worker_hours"] is None
    assert bucket["available_worker_hours"] is None
    assert bucket["worker_equivalents"] is None
    assert bucket["utilization_pct"] is None


def test_utilization_api_unknown_pool_returns_404(monkeypatch, tmp_path):
    client = _api_client(monkeypatch, _api_storage(tmp_path))
    response = client.get(
        "/api/v1/pools/unknown/pool/utilization",
        query_string={
            "start": API_START.isoformat(),
            "end": (API_START + timedelta(hours=1)).isoformat(),
            "bucket_seconds": "3600",
        },
    )
    assert response.status_code == 404
    assert response.json == {"error": {"code": "not_found", "message": "pool not found"}}
