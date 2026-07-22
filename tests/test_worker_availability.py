from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from worker_health.pool_classifier import PoolClassifier
from worker_health.pool_classifier_web.storage import SqliteStorage


def _classifier(tmp_path, threshold=3600, availability_mode="recent_contact"):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    classifier = PoolClassifier(
        "provisioner",
        "worker-type",
        results_dir=tmp_path,
        storage=storage,
        use_color=False,
        availability_mode=availability_mode,
        worker_contact_threshold_seconds=threshold,
    )
    classifier._init_db()
    return classifier, storage


def _worker(last_contact, quarantine_until=None):
    return {
        "workerId": "worker-1",
        "workerGroup": "group-1",
        "lastDateActive": last_contact.isoformat() if last_contact else None,
        "quarantineUntil": quarantine_until.isoformat() if quarantine_until else None,
    }


def _transitions(storage):
    return [
        dict(row)
        for row in storage.db.execute(
            "SELECT * FROM worker_availability_transitions ORDER BY id",
        )
    ]


def test_online_timeout_disappearance_and_return(tmp_path):
    classifier, storage = _classifier(tmp_path)
    first_observation = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
    first_contact = first_observation - timedelta(minutes=5)

    assert classifier._record_worker_availability([_worker(first_contact)], first_observation) == 1
    assert _transitions(storage)[0]["reason"] == "online"
    assert _transitions(storage)[0]["effective_at"] == first_contact.isoformat()

    latest_contact = first_observation + timedelta(minutes=20)
    assert classifier._record_worker_availability(
        [_worker(latest_contact)],
        first_observation + timedelta(minutes=30),
    ) == 0
    assert classifier._record_worker_availability(
        [_worker(first_contact)],
        first_observation + timedelta(minutes=40),
    ) == 0
    assert len(_transitions(storage)) == 1
    state = storage.get_worker_availability_states()["worker-1"]
    assert state["last_contact"] == latest_contact.isoformat()

    timeout_observation = first_observation + timedelta(hours=1, minutes=21)
    assert classifier._record_worker_availability([], timeout_observation) == 1
    timeout = _transitions(storage)[1]
    assert timeout["reason"] == "contact_timeout"
    assert timeout["effective_at"] == (latest_contact + timedelta(hours=1)).isoformat()

    return_contact = first_observation + timedelta(hours=2)
    assert classifier._record_worker_availability(
        [_worker(return_contact)],
        return_contact + timedelta(minutes=1),
    ) == 1
    returned = _transitions(storage)[2]
    assert returned["reason"] == "return"
    assert returned["effective_at"] == return_contact.isoformat()
    assert storage.db.execute("SELECT COUNT(*) FROM worker_availability_state").fetchone()[0] == 1


def test_quarantine_and_unquarantine_transitions(tmp_path):
    classifier, storage = _classifier(tmp_path)
    observed = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
    classifier._record_worker_availability([_worker(observed)], observed)

    quarantined_at = observed + timedelta(minutes=10)
    classifier._record_worker_availability(
        [_worker(observed, observed + timedelta(hours=2))],
        quarantined_at,
    )
    quarantine = _transitions(storage)[1]
    assert quarantine["reason"] == "quarantine"
    assert quarantine["available"] == 0
    assert quarantine["effective_at"] == quarantined_at.isoformat()

    unquarantined_at = observed + timedelta(minutes=20)
    classifier._record_worker_availability([_worker(observed)], unquarantined_at)
    unquarantine = _transitions(storage)[2]
    assert unquarantine["reason"] == "unquarantine"
    assert unquarantine["available"] == 1
    assert unquarantine["effective_at"] == unquarantined_at.isoformat()


def test_configurable_threshold_and_missing_contact(tmp_path):
    classifier, storage = _classifier(tmp_path, threshold=600)
    observed = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
    contact = observed - timedelta(minutes=10)

    classifier._record_worker_availability([_worker(contact)], observed)
    timeout = _transitions(storage)[0]
    assert timeout["reason"] == "contact_timeout"
    assert timeout["effective_at"] == observed.isoformat()

    classifier_2, storage_2 = _classifier(tmp_path / "missing", threshold=600)
    classifier_2._record_worker_availability([_worker(None)], observed)
    missing = _transitions(storage_2)[0]
    assert missing["reason"] == "contact_timeout"
    assert missing["effective_at"] == observed.isoformat()


def test_contact_threshold_configuration(tmp_path, monkeypatch):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    with pytest.raises(ValueError, match="greater than zero"):
        PoolClassifier(
            "provisioner",
            "worker-type",
            results_dir=tmp_path,
            storage=storage,
            worker_contact_threshold_seconds=0,
        )

    monkeypatch.setenv("WORKER_CONTACT_THRESHOLD_SECONDS", "123")
    classifier = PoolClassifier(
        "provisioner",
        "worker-type",
        results_dir=tmp_path,
        storage=storage,
    )
    assert classifier.worker_contact_threshold == timedelta(seconds=123)


def test_listed_mode_ignores_contact_age_and_missing_contact(tmp_path):
    classifier, storage = _classifier(tmp_path, availability_mode="listed")
    observed = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)

    stale = observed - timedelta(days=30)
    assert classifier._record_worker_availability([_worker(stale)], observed) == 1
    state = storage.get_worker_availability_states()["worker-1"]
    assert state["available"] == 1
    assert state["reason"] == "listed"
    assert state["effective_at"] == observed.isoformat()

    later = observed + timedelta(hours=2)
    assert classifier._record_worker_availability([_worker(None)], later) == 0
    assert storage.get_worker_availability_states()["worker-1"]["available"] == 1


def test_listed_mode_quarantine_disappearance_and_return(tmp_path):
    classifier, storage = _classifier(tmp_path, availability_mode="listed")
    observed = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
    classifier._record_worker_availability([_worker(None)], observed)

    quarantined_at = observed + timedelta(minutes=15)
    classifier._record_worker_availability(
        [_worker(None, observed + timedelta(hours=2))],
        quarantined_at,
    )
    assert _transitions(storage)[-1]["reason"] == "quarantine"
    assert _transitions(storage)[-1]["available"] == 0

    unquarantined_at = observed + timedelta(minutes=30)
    classifier._record_worker_availability([_worker(None)], unquarantined_at)
    assert _transitions(storage)[-1]["reason"] == "unquarantine"
    assert _transitions(storage)[-1]["available"] == 1

    missing_at = observed + timedelta(minutes=45)
    classifier._record_worker_availability([], missing_at)
    assert _transitions(storage)[-1]["reason"] == "not_listed"
    assert _transitions(storage)[-1]["effective_at"] == missing_at.isoformat()

    returned_at = observed + timedelta(hours=1)
    classifier._record_worker_availability([_worker(None)], returned_at)
    assert _transitions(storage)[-1]["reason"] == "listed"
    assert _transitions(storage)[-1]["effective_at"] == returned_at.isoformat()


def test_failed_listed_observation_does_not_evict_workers(tmp_path, monkeypatch):
    classifier, storage = _classifier(tmp_path, availability_mode="listed")
    observed = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
    classifier._record_worker_availability([_worker(None)], observed)
    initial_transitions = list(_transitions(storage))
    monkeypatch.setattr(classifier, "_update_reports", lambda: None)

    result = classifier.classify_cycle(workers=[], availability_collection_success=False)

    assert result["availability_transitions"] == 0
    assert _transitions(storage) == initial_transitions
    assert storage.get_worker_availability_states()["worker-1"]["available"] == 1
    coverage = storage.get_collection_coverage("worker_availability")
    assert coverage["intervals"] == []


def test_invalid_availability_mode_rejected(tmp_path):
    storage = SqliteStorage("provisioner/worker-type", tmp_path)
    with pytest.raises(ValueError, match="availability_mode must be one of"):
        PoolClassifier(
            "provisioner",
            "worker-type",
            results_dir=tmp_path,
            storage=storage,
            availability_mode="unknown",
        )
