from __future__ import annotations

from worker_health.pool_classifier_web.app import create_app


def test_favicon_serves_svg_icon():
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/favicon.ico")

    assert response.status_code == 200
    assert response.content_type.startswith("image/svg+xml")
    assert b"<svg" in response.data
