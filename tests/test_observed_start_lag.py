from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker_health.pool_classifier_web import app as app_module
from worker_health.pool_classifier_web.app import create_app
from worker_health.pool_classifier_web.queue_lag import (
    calculate_observed_start_lag,
    calculate_observed_start_lag_visualization,
)
from worker_health.pool_classifier_web.storage import SqliteStorage


START = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)
END = START + timedelta(hours=1)
PATH = "/api/v1/pools/provisioner/worker-type/observed-start-lag"


def _iso(seconds: int) -> str:
    return (START + timedelta(seconds=seconds)).isoformat()


def test_calculate_observed_start_lag_uses_ordered_pairs_and_nearest_rank_percentiles():
    result = calculate_observed_start_lag(
        "provisioner/worker-type",
        START.isoformat(),
        END.isoformat(),
        120,
        [
            {"scheduled": _iso(0), "started": _iso(60)},
            {"scheduled": _iso(1), "started": _iso(121)},
            {"scheduled": _iso(2), "started": _iso(182)},
            {"scheduled": _iso(3), "started": _iso(243)},
            {"scheduled": _iso(4), "started": _iso(244)},
            {"scheduled": _iso(5), "started": _iso(4)},  # clock/order invalid
            {"scheduled": None, "started": _iso(30)},
            {"scheduled": END.isoformat(), "started": END.isoformat()},  # end-exclusive window
        ],
    )

    assert result["sample_count"] == 5
    assert result["p50_seconds"] == 180
    assert result["p95_seconds"] == 240
    assert result["started_within_slo_count"] == 2
    assert result["started_within_slo_pct"] == 40.0
    assert "never started" in result["scope"]


def test_calculate_observed_start_lag_returns_null_percentiles_without_samples():
    result = calculate_observed_start_lag("pool", START.isoformat(), END.isoformat(), 60, [])

    assert result["sample_count"] == 0
    assert result["p50_seconds"] is None
    assert result["p95_seconds"] is None
    assert result["started_within_slo_pct"] is None


def test_start_lag_visualization_aggregates_hourly_and_weekday_hour_cells():
    result = calculate_observed_start_lag_visualization(
        "pool", START.isoformat(), (START + timedelta(hours=2)).isoformat(), 120, 2,
        [
            {"scheduled": _iso(60), "started": _iso(120)},
            {"scheduled": _iso(120), "started": _iso(300)},
            {"scheduled": _iso(3605), "started": _iso(3665)},
        ],
    )

    first_hour, second_hour = result["buckets"][:2]
    assert first_hour["sample_count"] == 2
    assert first_hour["p50_seconds"] == 60
    assert first_hour["p95_seconds"] == 180
    assert first_hour["sufficient_samples"] is True
    assert second_hour["sample_count"] == 1
    assert second_hour["sufficient_samples"] is False
    start_hour = next(cell for cell in result["heatmap"] if cell["weekday"] == START.weekday() and cell["hour"] == START.hour)
    assert start_hour["sample_count"] == 2


def _storage(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    storage.record_task_result(
        "task-1", "worker-1", 0, "completed", None, "completed",
        _iso(180), _iso(240), _iso(300), run_scheduled=_iso(0), reason_created="scheduled",
    )
    storage.commit()
    assert tuple(storage.db.execute(
        "SELECT run_scheduled, reason_created FROM task_results WHERE task_id = 'task-1'",
    ).fetchone()) == (_iso(0), "scheduled")
    return storage


def _client(monkeypatch, storage):
    classifier = type("Classifier", (), {"storage": storage})()
    monkeypatch.setattr(
        app_module, "_get_classifier",
        lambda provisioner, worker_type: classifier if (provisioner, worker_type) == ("provisioner", "worker-type") else None,
    )
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


def test_observed_start_lag_api_includes_scope_and_configurable_slo(monkeypatch, tmp_path):
    response = _client(monkeypatch, _storage(tmp_path)).get(
        PATH,
        query_string={"start": START.isoformat(), "end": END.isoformat(), "slo_seconds": "200"},
    )

    assert response.status_code == 200
    assert response.json == {
        "api_version": 1,
        "metric": "observed_scheduled_to_start_lag",
        "scope": "Only task runs that started and were later observed terminal by the per-worker collector; tasks that never started are not represented.",
        "pool_id": "provisioner/worker-type",
        "start_at": START.isoformat(),
        "end_at": END.isoformat(),
        "sample_count": 1,
        "p50_seconds": 180.0,
        "p95_seconds": 180.0,
        "slo_seconds": 200,
        "started_within_slo_count": 1,
        "started_within_slo_pct": 100.0,
    }


def test_observed_start_lag_visualization_api(monkeypatch, tmp_path):
    response = _client(monkeypatch, _storage(tmp_path)).get(
        f"{PATH}/visualization",
        query_string={"start": START.isoformat(), "end": END.isoformat(), "min_samples": "1"},
    )

    assert response.status_code == 200
    assert response.json["api_version"] == 1
    assert response.json["min_samples"] == 1
    assert len(response.json["buckets"]) == 1
    assert len(response.json["heatmap"]) == 168


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ({}, "start is required"),
        ({"start": START.isoformat(), "end": END.isoformat(), "slo_seconds": "0"}, "slo_seconds must be greater than zero"),
        ({"start": START.isoformat(), "end": END.isoformat(), "slo_seconds": "fast"}, "slo_seconds must be an integer"),
    ],
)
def test_observed_start_lag_api_validates_parameters(monkeypatch, tmp_path, query, message):
    response = _client(monkeypatch, _storage(tmp_path)).get(PATH, query_string=query)

    assert response.status_code == 400
    assert response.json == {"error": {"code": "invalid_parameter", "message": message}}
