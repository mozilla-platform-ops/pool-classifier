"""Tests for public informational pages that do not require a database."""

from worker_health.pool_classifier_web.app import create_app


def test_api_overview_renders():
    app = create_app()
    response = app.test_client().get("/api")

    assert response.status_code == 200
    assert b"/api/v1/pools/{provisioner}/{worker_type}/utilization" in response.data
    assert b'aria-current="page">API' in response.data


def test_about_renders_build_metadata(monkeypatch):
    monkeypatch.setenv("POOL_CLASSIFIER_VERSION", "test-version")
    monkeypatch.setenv("POOL_CLASSIFIER_COMMIT", "abc1234")
    app = create_app()
    response = app.test_client().get("/about")

    assert response.status_code == 200
    assert b"test-version" in response.data
    assert b"abc1234" in response.data
    assert b"mozilla-platform-ops/pool-classifier" in response.data
    assert b'aria-current="page">About' in response.data
