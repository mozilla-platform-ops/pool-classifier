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
    assert "Pool Classifier’s goal is to make Taskcluster worker-pool health operationally legible.".encode() in response.data
    assert "It collects recent task and worker signals, classifies failures into actionable categories".encode() in response.data


def test_global_navigation_has_five_consistent_items():
    app = create_app()
    menu = app.jinja_env.get_template("base.html").module.navigation("Overview")

    assert menu.count('class="global-menu"') == 1
    assert menu.count('aria-current="page"') == 1
    assert '!menu.contains(event.target)' in menu
    # The logo plus the four non-current navigation items are links; the fifth
    # navigation item is rendered as the current-page span.
    assert menu.count('<a href="') == 5
    for label, path in (
        ("Overview", "/"),
        ("Patterns", "/patterns"),
        ("Pool Discovery", "/pool-discovery"),
        ("API", "/api"),
        ("About", "/about"),
    ):
        assert label in menu
        assert f'href="{path}"' in menu or label == "Overview"


def test_global_navigation_shows_admin_only_for_iap_admin_hint():
    app = create_app()
    client = app.test_client()

    response = client.get("/")

    assert b'href="/admin">Admin</a>' not in response.data

    response = client.get(
        "/",
        headers={"X-Goog-Authenticated-User-Email": "accounts.google.com:aerickson@mozilla.com"},
    )

    assert response.status_code == 200
    assert b'href="/admin">Admin</a>' in response.data
    assert response.data.index(b'href="/admin">Admin</a>') < response.data.index(b'href="/about">About</a>')


def test_global_navigation_shows_admin_when_local_iap_bypass_is_enabled(monkeypatch):
    monkeypatch.setenv("ADMIN_IAP_BYPASS", "1")
    app = create_app()

    response = app.test_client().get("/")

    assert response.status_code == 200
    assert b'href="/admin">Admin</a>' in response.data
