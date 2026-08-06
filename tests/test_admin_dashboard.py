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
        },
    )
    client = _admin_client(monkeypatch)

    response = client.get("/admin", headers={"X-Goog-IAP-JWT-Assertion": "signed"})

    assert response.status_code == 200
    assert b"001_init" in response.data
    assert b"applied" in response.data
    assert b"002_task_run_intervals" in response.data
    assert b"pending" in response.data
    assert b"provisioner/worker-type" in response.data
    assert b"20m ago" in response.data
    assert b"provisioner/disabled-type" in response.data
    assert b"never" in response.data
    assert b"disabled" in response.data


def test_relative_age_is_compact_and_handles_future_timestamps():
    now = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)

    assert app_module._relative_age(now - timedelta(seconds=30), now=now) == "30s ago"
    assert app_module._relative_age(now - timedelta(minutes=20), now=now) == "20m ago"
    assert app_module._relative_age(now - timedelta(hours=3), now=now) == "3h ago"
    assert app_module._relative_age(now - timedelta(days=2), now=now) == "2d ago"
    assert app_module._relative_age(now + timedelta(seconds=30), now=now) == "in 30s"
