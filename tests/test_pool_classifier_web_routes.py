from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from worker_health.pool_classifier_web import app as app_module
from worker_health.pool_classifier_web.app import create_app
from worker_health.pool_classifier_web.registry import Pool
from worker_health.pool_classifier_web.storage import SqliteStorage


API_PATH = "/api/v1/pools/provisioner/worker-type/utilization"
SUMMARY_PATH = f"{API_PATH}/summary"
FAILURES_PATH = "/api/v1/pools/provisioner/worker-type/failures"
WORKERS_PATH = "/api/v1/pools/provisioner/worker-type/workers"
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


def test_api_v1_discovery_lists_versioned_endpoints():
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/api/v1")

    assert response.status_code == 200
    assert response.json["api_version"] == 1
    assert {endpoint["path"] for endpoint in response.json["endpoints"]} >= {
        "/api/v1/pools",
        "/api/v1/pools/{provisioner}/{worker_type}/summary",
    }


def test_pools_api_returns_enabled_and_disabled_pool_configuration(monkeypatch):
    enabled = Pool("enabled", "proj", "worker", "*/15 * * * *")
    disabled = Pool("disabled", "proj", "disabled-worker", "0 * * * *", enabled=False, reason="retired")
    monkeypatch.setattr(app_module.registry, "all_pools_including_disabled", lambda: [enabled, disabled])
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/api/v1/pools")

    assert response.status_code == 200
    assert response.json == {
        "api_version": 1,
        "pools": [
            {
                "id": "enabled", "provisioner": "proj", "worker_type": "worker", "os": "linux",
                "enabled": True, "reason": None, "schedule": "*/15 * * * *", "availability_mode": "recent_contact",
            },
            {
                "id": "disabled", "provisioner": "proj", "worker_type": "disabled-worker", "os": "linux",
                "enabled": False, "reason": "retired", "schedule": "0 * * * *", "availability_mode": "recent_contact",
            },
        ],
    }


def test_pool_summary_api_returns_metrics_coverage_and_freshness(monkeypatch):
    pool = Pool("pool", "proj", "worker", "*/15 * * * *", availability_mode="listed")
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(app_module.registry, "get_pool", lambda p, w: pool if (p, w) == ("proj", "worker") else None)
    monkeypatch.setattr(
        app_module,
        "pool_summaries_global",
        lambda *_args: {
            "proj/worker": {
                "workers": 4, "alerting": 1, "task_runs": 12, "successes": 9, "errors": 3,
                "ok_1h": 3, "err_1h": 1, "ok_24h": 9, "err_24h": 3,
                "task_collection_started": "2026-07-20T00:00:00+00:00",
                "collection_latest": "2026-07-21T11:00:00+00:00",
                "availability_collection_started": "2026-07-20T00:00:00+00:00",
                "availability_collection_latest": "2026-07-21T11:15:00+00:00",
            },
        },
    )
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/api/v1/pools/proj/worker/summary")

    assert response.status_code == 200
    assert response.json["api_version"] == 1
    assert response.json["pool"]["availability_mode"] == "listed"
    assert response.json["metrics"] == {
        "workers": 4, "alerting_workers": 1, "task_runs": 12, "successes": 9, "errors": 3,
        "success_rate_pct": 75.0,
        "windows": {
            "1h": {"successes": 3, "errors": 1, "success_rate_pct": 75.0},
            "24h": {"successes": 9, "errors": 3, "success_rate_pct": 75.0},
        },
    }
    assert response.json["coverage"]["task_runs"]["through"] == "2026-07-21T11:00:00+00:00"
    assert response.json["freshness"]["collected_at"] == "2026-07-21T11:15:00+00:00"


def test_pool_summary_api_returns_404_for_unknown_pool(monkeypatch):
    monkeypatch.setattr(app_module.registry, "get_pool", lambda *_args: None)
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/api/v1/pools/no/such/summary")

    assert response.status_code == 404
    assert response.json == {"error": {"code": "not_found", "message": "pool not found"}}


def test_pool_discovery_page_and_refetch(monkeypatch):
    calls = []
    def fake_discover(force=False):
        calls.append(force)
        return {"fetched_at": "2026-07-30T00:00:00+00:00", "rows": [
            {"provisioner": "releng-hardware", "worker_type": "new-pool", "status": "uncovered", "reason": ""},
        ]}
    monkeypatch.setattr(app_module.discovery, "discover", fake_discover)
    app = create_app(); app.config["TESTING"] = True
    with app.test_client() as client:
        page = client.get("/pool-discovery").data
        assert b"Pool Discovery" in page
        assert b"new-pool" in page
        assert b"Copy YAML" in page
        assert b"data-worker-type=\"new-pool\"" in page
        response = client.post("/pool-discovery/refetch")
    assert response.status_code == 200
    assert calls == [False, True]


def test_failures_api_groups_terminal_failure_categories(monkeypatch, tmp_path):
    storage = _api_storage(tmp_path)
    storage.record_task_result(
        "failed-task", "worker-1", 0, "failed", "device_error", None,
        API_START.isoformat(), (API_START + timedelta(minutes=10)).isoformat(), (API_START + timedelta(minutes=10)).isoformat(),
    )
    storage.record_task_result(
        "unknown-task", "worker-2", 0, "exception", None, None,
        API_START.isoformat(), (API_START + timedelta(minutes=20)).isoformat(), (API_START + timedelta(minutes=20)).isoformat(),
    )
    storage.commit()
    client = _api_client(monkeypatch, storage)

    response = client.get(
        FAILURES_PATH,
        query_string={"start": API_START.isoformat(), "end": (API_START + timedelta(hours=1)).isoformat()},
    )

    assert response.status_code == 200
    assert response.json["failures"] == [{"category": "device_error", "count": 1}, {"category": "unclassified", "count": 1}]
    filtered = client.get(
        FAILURES_PATH,
        query_string={"start": API_START.isoformat(), "end": (API_START + timedelta(hours=1)).isoformat(), "category": "device_error"},
    )
    assert filtered.json["failures"] == [{"category": "device_error", "count": 1}]


def test_workers_api_filters_and_paginates(monkeypatch, tmp_path):
    storage = _api_storage(tmp_path)
    for worker_id, category, quarantined in (("alert-worker", "device_error", True), ("normal-worker", "network", False)):
        storage.upsert_worker(worker_id, "group")
        storage.record_task_result(
            f"task-{worker_id}", worker_id, 0, "failed", category, None,
            API_START.isoformat(), (API_START + timedelta(minutes=10)).isoformat(), (API_START + timedelta(minutes=10)).isoformat(),
        )
        storage.increment_failure(worker_id, API_START.isoformat(), category)
        if worker_id == "alert-worker":
            storage.increment_failure(worker_id, API_START.isoformat(), category)
            storage.increment_failure(worker_id, API_START.isoformat(), category)
        storage.upsert_worker_availability_state(
            worker_id, "group", True, quarantined, API_START.isoformat(), None, "test",
            API_START.isoformat(), API_START.isoformat(),
        )
    storage.commit()
    client = _api_client(monkeypatch, storage)
    params = {"start": API_START.isoformat(), "end": (API_START + timedelta(hours=1)).isoformat(), "limit": "1"}

    first = client.get(WORKERS_PATH, query_string=params)
    assert first.status_code == 200
    assert first.json["workers"][0]["worker_id"] == "alert-worker"
    assert first.json["workers"][0]["alerting"] is True
    assert first.json["workers"][0]["top_category"] == "device_error"
    cursor = first.json["pagination"]["next_cursor"]
    assert cursor
    second = client.get(WORKERS_PATH, query_string={**params, "cursor": cursor})
    assert [worker["worker_id"] for worker in second.json["workers"]] == ["normal-worker"]
    filtered = client.get(WORKERS_PATH, query_string={**params, "quarantined": "true"})
    assert [worker["worker_id"] for worker in filtered.json["workers"]] == ["alert-worker"]


def test_patterns_api_exposes_enabled_and_disabled_registry_entries(monkeypatch):
    monkeypatch.setattr(
        app_module.patterns_registry,
        "_patterns",
        [
            SimpleNamespace(name="enabled", severity="high", tags=["android"], description="test", enabled=True),
            SimpleNamespace(name="disabled", severity="low", tags=[], description="", enabled=False),
        ],
    )
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/api/v1/patterns")

    assert response.status_code == 200
    assert response.json == {
        "api_version": 1,
        "patterns": [
            {"name": "enabled", "severity": "high", "tags": ["android"], "description": "test", "enabled": True},
            {"name": "disabled", "severity": "low", "tags": [], "description": "", "enabled": False},
        ],
    }


@pytest.mark.parametrize(
    ("path", "query", "message"),
    [
        (FAILURES_PATH, {}, "start and end must be provided together"),
        (WORKERS_PATH, {"limit": "0"}, "limit must be between 1 and 200"),
        (WORKERS_PATH, {"cursor": "not-a-cursor"}, "cursor is invalid"),
    ],
)
def test_new_api_endpoints_reject_invalid_parameters(monkeypatch, tmp_path, path, query, message):
    client = _api_client(monkeypatch, _api_storage(tmp_path))
    response = client.get(path, query_string=query)
    assert response.status_code == 400
    assert response.json == {"error": {"code": "invalid_parameter", "message": message}}


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


def test_utilization_summary_uses_one_common_freshness_boundary(monkeypatch, tmp_path):
    client = _api_client(monkeypatch, _api_storage(tmp_path, coverage_minutes=60))
    response = client.get(SUMMARY_PATH)

    assert response.status_code == 200
    assert response.json["data_through"] == (API_START + timedelta(hours=1)).isoformat()
    assert response.json["availability_mode"] == "recent_contact"
    assert set(response.json["windows"]) == {"1h", "24h", "7d", "30d"}
    assert response.json["windows"]["1h"]["status"] == "ok"
    assert response.json["windows"]["1h"]["utilization"]["complete"] is True
    assert response.json["windows"]["24h"]["utilization"]["complete"] is False


def test_utilization_summary_collects_before_any_common_coverage(monkeypatch, tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    client = _api_client(monkeypatch, storage)

    response = client.get(SUMMARY_PATH)

    assert response.status_code == 200
    assert response.json["data_through"] is None
    assert response.json["windows"] == {}


def test_pool_utilization_guide_is_pool_aware(monkeypatch):
    pool = SimpleNamespace(provisioner="provisioner", worker_type="worker-type")
    monkeypatch.setattr(app_module.registry, "get_pool", lambda *_args: pool)
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/pools/provisioner/worker-type/utilization-api-guide")

    assert response.status_code == 200
    assert b"/api/v1/pools/provisioner/worker-type/utilization?start=" in response.data
    assert b"Copy curl" in response.data
    assert b"inclusive" in response.data
    assert b"90 days" in response.data


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
