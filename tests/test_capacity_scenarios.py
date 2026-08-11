from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker_health.pool_classifier_web import app as app_module
from worker_health.pool_classifier_web.app import create_app
from worker_health.pool_classifier_web.capacity_scenarios import busy_turnaround_summary, calculate_capacity_scenarios
from worker_health.pool_classifier_web.storage import SqliteStorage


START = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)
END = START + timedelta(hours=1)
PATH = "/api/v1/pools/provisioner/worker-type/capacity-scenarios"


def _iso(seconds: int) -> str:
    return (START + timedelta(seconds=seconds)).isoformat()


def _runs():
    return [
        {"scheduled": _iso(0), "started": _iso(0), "resolved": _iso(10)},
        {"scheduled": _iso(0), "started": _iso(10), "resolved": _iso(20)},
    ]


def _transitions():
    return [{"id": 1, "worker_id": "worker-1", "available": True, "effective_at": START.isoformat(), "observed_at": START.isoformat()}]


def test_capacity_scenarios_replays_observed_durations_against_added_capacity():
    result = calculate_capacity_scenarios(
        "provisioner/worker-type", START.isoformat(), END.isoformat(), 5, [0, 1, 2], 0, _runs(), _transitions(),
    )

    baseline, plus_one, plus_two = result["scenarios"]
    assert baseline["modeled_p95_seconds"] == 10
    assert baseline["max_queue_depth"] == 1
    assert baseline["meets_target"] is False
    assert plus_one["modeled_p95_seconds"] == plus_two["modeled_p95_seconds"] == 0
    assert plus_one["meets_target"] is True
    assert result["minimum_additional_hosts_meeting_target"] == 1
    assert result["observed_baseline"]["p95_seconds"] == 10
    assert result["calibration"]["p95_difference_seconds"] == 0
    assert result["model"]["status"] == "uncalibrated"
    assert "never started" in result["model"]["scope"]


def test_capacity_scenarios_excludes_invalid_observed_runs():
    result = calculate_capacity_scenarios(
        "pool", START.isoformat(), END.isoformat(), 5, [0], 0,
        _runs() + [{"scheduled": _iso(1), "started": None, "resolved": None}], _transitions(),
    )

    assert result["observed_run_count"] == 2
    assert result["excluded_run_count"] == 1


def test_capacity_scenarios_applies_turnaround_before_releasing_a_host():
    result = calculate_capacity_scenarios(
        "pool", START.isoformat(), END.isoformat(), 120, [0], 120, _runs(), _transitions(),
    )

    assert result["model"]["assumptions"]["turnaround_seconds"] == 120
    assert result["scenarios"][0]["modeled_p95_seconds"] == 130
    assert result["scenarios"][0]["started_within_target_pct"] == 50.0


def test_busy_turnaround_uses_only_a_following_task_that_was_already_waiting():
    runs = [
        {"worker_id": "worker-1", "scheduled": _iso(0), "started": _iso(0), "resolved": _iso(10)},
        {"worker_id": "worker-1", "scheduled": _iso(5), "started": _iso(70), "resolved": _iso(80)},
        {"worker_id": "worker-1", "scheduled": _iso(90), "started": _iso(100), "resolved": _iso(110)},
    ]

    summary = busy_turnaround_summary(runs, START.isoformat(), END.isoformat())

    assert summary["sample_count"] == 1
    assert summary["p50_seconds"] == 60
    assert summary["available"] is False


def _storage(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    storage.init_schema()
    storage.record_worker_availability_transition(
        "worker-1", "group-1", True, False, START.isoformat(), None, "online", START.isoformat(), START.isoformat(),
    )
    for index, run in enumerate(_runs()):
        storage.record_task_result(
            f"task-{index}", "worker-1", index, "completed", None, "completed", run["started"], run["resolved"], run["resolved"],
            run_scheduled=run["scheduled"], reason_created="scheduled",
        )
    for source in ("task_runs", "worker_availability"):
        storage.record_collection_coverage(source, START.isoformat(), True, 3600)
        storage.record_collection_coverage(source, END.isoformat(), True, 3600)
    storage.commit()
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


def test_capacity_scenarios_api_returns_model_and_coverage(monkeypatch, tmp_path):
    response = _client(monkeypatch, _storage(tmp_path)).get(
        PATH,
        query_string={"start": START.isoformat(), "end": END.isoformat(), "target_p95_seconds": "5", "additional_hosts": "0,1"},
    )

    assert response.status_code == 200
    assert response.json["api_version"] == 1
    assert response.json["minimum_additional_hosts_meeting_target"] == 1
    assert response.json["observed_baseline"]["p95_seconds"] == 10
    assert response.json["warnings"] == ["Model outputs are experimental until calibrated against a known capacity change."]
    assert response.json["model"]["assumptions"]["turnaround_seconds"] == 120
    sensitivity = response.json["turnaround_sensitivity"]
    assert sensitivity["busy_turnaround"]["sample_count"] == 1
    assert [variant["id"] for variant in sensitivity["variants"]] == ["fixed_120_seconds"]
    assert response.json["coverage"]["task_runs"]["complete"] is True
    assert [row["additional_hosts"] for row in response.json["scenarios"]] == [0, 1]


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ({}, "start is required"),
        ({"start": START.isoformat(), "end": END.isoformat(), "additional_hosts": "one"}, "additional_hosts must be comma-separated integers"),
        ({"start": START.isoformat(), "end": END.isoformat(), "additional_hosts": "-1"}, "additional_hosts must not contain negative values"),
        ({"start": START.isoformat(), "end": END.isoformat(), "turnaround_seconds": "fast"}, "turnaround_seconds must be an integer"),
        ({"start": START.isoformat(), "end": END.isoformat(), "turnaround_seconds": "1801"}, "turnaround_seconds must be between 0 and 1800"),
    ],
)
def test_capacity_scenarios_api_validates_parameters(monkeypatch, tmp_path, query, message):
    response = _client(monkeypatch, _storage(tmp_path)).get(PATH, query_string=query)

    assert response.status_code == 400
    assert response.json == {"error": {"code": "invalid_parameter", "message": message}}
