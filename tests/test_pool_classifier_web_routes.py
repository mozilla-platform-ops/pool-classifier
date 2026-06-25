from __future__ import annotations

import logging
from types import SimpleNamespace

from worker_health.pool_classifier_web import app as app_module
from worker_health.pool_classifier_web.app import create_app


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
