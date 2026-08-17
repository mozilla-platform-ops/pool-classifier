from __future__ import annotations

from datetime import datetime, timedelta, timezone

from worker_health.pool_classifier_web import app as app_module
from worker_health.pool_classifier_web import auth
from worker_health.pool_classifier_web.app import create_app
from worker_health.pool_classifier_web.registry import Pool


def _admin_client(monkeypatch, email: str = "aerickson@mozilla.com"):
    monkeypatch.setenv("IAP_JWT_AUDIENCE", "test-audience")
    monkeypatch.setattr(auth, "_verify_iap", lambda token, audience: {"email": email})
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


def test_admin_rejects_requests_without_a_signed_iap_assertion(monkeypatch):
    client = _admin_client(monkeypatch)

    response = client.get("/admin")

    assert response.status_code == 401
    assert b"Pool Classifier" in response.data
    assert b"Authentication required" in response.data


def test_admin_rejects_a_valid_non_admin_iap_identity(monkeypatch):
    client = _admin_client(monkeypatch, email="other@mozilla.com")

    response = client.get("/admin", headers={"X-Goog-IAP-JWT-Assertion": "signed"})

    assert response.status_code == 403
    assert b"Access denied" in response.data
    assert b"Return to the dashboard" in response.data


def test_admin_iap_bypass_allows_local_development(monkeypatch):
    monkeypatch.setenv("ADMIN_IAP_BYPASS", "1")
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(app_module, "_admin_dashboard_data", lambda _dsn: (_ for _ in ()).throw(RuntimeError()))
    app = create_app()
    app.config["TESTING"] = True

    response = app.test_client().get("/admin")

    assert response.status_code == 200
    assert b"Database status is unavailable" in response.data
    assert b'<header class="site-header">' in response.data
    assert b"a { color: inherit; text-decoration: none; }" in response.data
    assert b'<span class="site-title">Admin</span>' in response.data
    assert b'aria-label="Global navigation"' in response.data
    assert b'href="/patterns"' in response.data


def test_admin_shows_migration_and_snapshot_freshness(monkeypatch):
    now = datetime.now(timezone.utc)
    pool = Pool("display", "provisioner", "worker-type", "*/15 * * * *")
    disabled_pool = Pool("disabled", "provisioner", "disabled-type", "*/15 * * * *", enabled=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(app_module.registry, "all_pools_including_disabled", lambda: [pool, disabled_pool])
    monkeypatch.setattr(
        app_module,
        "_admin_dashboard_data",
        lambda dsn: {
            "migrations": [
                {"version": "001_init", "applied_at": now - timedelta(minutes=30)},
                {"version": "002_task_run_intervals", "applied_at": None},
            ],
            "snapshots": {
                "provisioner/worker-type": {
                    "source_at": now - timedelta(minutes=20),
                    "generated_at": now - timedelta(minutes=19),
                },
            },
            "overview": {
                "source_at": now,
                "generated_at": now,
                "payload": {
                    "classify_all_duration_seconds": 134.2,
                    "classify_all_completed_at": now.isoformat(),
                    "pool_timings": {
                        "provisioner/worker-type": {
                            "completed_at": now.isoformat(),
                            "duration_seconds": 120,
                        },
                        "provisioner/disabled-type": {
                            "completed_at": now.isoformat(),
                            "duration_seconds": 14,
                        },
                    },
                },
            },
        },
    )
    client = _admin_client(monkeypatch)

    response = client.get("/admin", headers={"X-Goog-IAP-JWT-Assertion": "signed"})

    assert response.status_code == 200
    assert b'<nav class="page-nav" aria-label="Admin sections">' in response.data
    assert b'href="#database-migrations"' in response.data
    assert b'href="#classify-all"' in response.data
    assert b'href="#pool-runtime"' in response.data
    assert b'href="#pool-snapshots"' in response.data
    assert response.data.index(b'aria-label="Admin sections"') < response.data.index(b"generated on")
    assert b"001_init" in response.data
    assert b"applied" in response.data
    assert b"002_task_run_intervals" in response.data
    assert b"pending" in response.data
    assert b"provisioner/worker-type" in response.data
    assert b"20m ago" in response.data
    assert b"provisioner/disabled-type" in response.data
    assert b"never" in response.data
    assert b"disabled" in response.data
    assert b"Last successful classify-all" in response.data
    assert b"2m 14s" in response.data
    assert b"Per-pool runtime" in response.data
    assert b"2m 0s" in response.data
    assert response.data.count(b'<table class="sortable-table">') == 2
    assert b'data-sort-value="120"' in response.data
    assert b'class="runtime-bar"' in response.data
    assert b'style="width: 100.0%"' in response.data
    assert b"initSortableTable(table" in response.data
    assert response.data.count(b'href="/pools/provisioner/worker-type"') == 2
    assert b'href="/pools/provisioner/disabled-type"' in response.data
    assert b'data-timezone="local"' in response.data
    assert b'data-timezone="utc"' in response.data
    assert b'Applied (<span data-timezone-heading>Local</span>)' in response.data
    assert b'Completed (<span data-timezone-heading>Local</span>)' in response.data
    assert response.data.count(b'class="utc-time"') == 5
    assert b'class="utc-tooltip"' in response.data
    assert b"element.title = formatTime(element.dataset.utc, mode)" in response.data


def test_admin_shows_curated_runtime_mode(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setenv("POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS", "1")
    monkeypatch.setattr(app_module.registry, "all_pools_including_disabled", lambda: [])
    monkeypatch.setattr(
        app_module,
        "_admin_dashboard_data",
        lambda _dsn: {"migrations": [], "snapshots": {}, "overview": {}},
    )
    client = _admin_client(monkeypatch)
    client.application.debug = True

    response = client.get("/admin", headers={"X-Goog-IAP-JWT-Assertion": "signed"})

    assert response.status_code == 200
    assert b'href="#runtime-mode"' in response.data
    assert b"Runtime mode" in response.data
    assert b"Request host and port" in response.data
    assert b"localhost" in response.data
    assert b"Current setting" in response.data
    assert b"Flask debug" in response.data
    assert b"enabled" in response.data
    assert "live rendering — snapshot reads disabled in this runtime".encode() in response.data
    assert b"POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS" not in response.data

    monkeypatch.delenv("POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS")
    response = client.get("/admin", headers={"X-Goog-IAP-JWT-Assertion": "signed"})

    assert b"stored dashboard snapshots enabled (used when available)" in response.data


def test_relative_age_is_compact_and_handles_future_timestamps():
    now = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)

    assert app_module._relative_age(now - timedelta(seconds=30), now=now) == "30s ago"
    assert app_module._relative_age(now - timedelta(minutes=20), now=now) == "20m ago"
    assert app_module._relative_age(now - timedelta(hours=3), now=now) == "3h ago"
    assert app_module._relative_age(now - timedelta(days=2), now=now) == "2d ago"
    assert app_module._relative_age(now + timedelta(seconds=30), now=now) == "in 30s"
