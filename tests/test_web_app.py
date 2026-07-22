"""Integration tests for the Flask web layer.

Requires a live Postgres instance. Skip unless PC_TEST_DATABASE_URL is set.

  docker compose -f worker_health/pool_classifier_web/docker-compose.yml up -d postgres
  PC_TEST_DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier \\
    pipenv run pytest tests/test_web_app.py -v
"""

from __future__ import annotations

import os

import pytest

psycopg = pytest.importorskip("psycopg")

from worker_health.pool_classifier_web.scripts.migrate import apply_migrations  # noqa: E402

DSN = os.environ.get("PC_TEST_DATABASE_URL", "")
if not DSN:
    pytest.skip("PC_TEST_DATABASE_URL not set", allow_module_level=True)

PROVISIONER = "proj-autophone"
WORKER_TYPE = "gecko-t-lambda-perf-a55"
POOL_ID = f"{PROVISIONER}/{WORKER_TYPE}"
POOL_URL_PREFIX = f"/pools/{PROVISIONER}/{WORKER_TYPE}"
UTILIZATION_URL = f"/api/v1/pools/{PROVISIONER}/{WORKER_TYPE}/utilization"
CLASSIFY_URL = f"/classify/{PROVISIONER}/{WORKER_TYPE}"


@pytest.fixture(scope="module", autouse=True)
def _apply_migrations():
    apply_migrations(DSN)


@pytest.fixture(autouse=True)
def _truncate_pg():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            for tbl in (
                "task_results",
                "workers",
                "quarantine_cache",
                "unclassified_logs",
                "worker_availability_transitions",
                "worker_availability_state",
                "collection_coverage_state",
                "collection_coverage_intervals",
            ):
                cur.execute(f"DELETE FROM {tbl} WHERE pool_id = %s", (POOL_ID,))
        conn.commit()
    yield


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", DSN)
    # Clear the module-level classifier cache between tests.
    import worker_health.pool_classifier_web.app as app_module

    app_module._classifiers.clear()
    from worker_health.pool_classifier_web.app import create_app

    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.data == b"ok"


def test_index_renders(client):
    r = client.get("/")
    assert r.status_code == 200
    assert WORKER_TYPE.encode() in r.data


def test_pool_html(client):
    r = client.get(POOL_URL_PREFIX)
    assert r.status_code == 200
    assert b"Pool Classifier" in r.data


def test_pool_unknown_returns_404(client):
    r = client.get("/pools/unknown-provisioner/unknown-worker-type")
    assert r.status_code == 404


def test_utilization_api_postgres_integration(client):
    start = "2026-07-21T10:00:00+00:00"
    middle = "2026-07-21T10:30:00+00:00"
    end = "2026-07-21T11:00:00+00:00"
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            for source in ("task_runs", "worker_availability"):
                cur.execute(
                    "INSERT INTO collection_coverage_intervals (pool_id, source, start_at, end_at)"
                    " VALUES (%s,%s,%s,%s)",
                    (POOL_ID, source, start, end),
                )
            cur.execute(
                "INSERT INTO worker_availability_transitions"
                " (pool_id, worker_id, worker_group, available, quarantined, last_contact,"
                "  reason, effective_at, observed_at)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (POOL_ID, "w1", "group-1", True, False, start, "online", start, start),
            )
            cur.execute(
                "INSERT INTO task_results"
                " (pool_id, task_id, worker_id, run_id, run_state, run_started, run_resolved, classified_at)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (POOL_ID, "t1", "w1", 0, "completed", start, middle, middle),
            )
        conn.commit()

    response = client.get(
        UTILIZATION_URL,
        query_string={"start": start, "end": end, "bucket_seconds": "1800"},
    )
    assert response.status_code == 200
    assert response.json["pool_id"] == POOL_ID
    assert response.json["complete"] is True
    assert [bucket["utilization_pct"] for bucket in response.json["buckets"]] == [100, 0]


def test_classify_lock_conflict_returns_409(client):
    """A second classify request while the lock is held returns 409."""
    # Acquire the same advisory lock that classify_cycle() would acquire.
    with psycopg.connect(DSN) as lock_conn:
        with lock_conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext('classify:' || %s)::bigint)",
                (POOL_ID,),
            )
            acquired = cur.fetchone()[0]
        assert acquired, "could not acquire advisory lock for test"

        r = client.post(CLASSIFY_URL)
        assert r.status_code == 409

        lock_conn.rollback()  # release lock


def test_unclassified_log_found(client):
    """Seeded log text is returned by the log route."""
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO unclassified_logs (pool_id, task_id, run_id, worker_id, log_text)"
                " VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT (pool_id, task_id) DO UPDATE SET log_text = EXCLUDED.log_text",
                (POOL_ID, "t-test-1", 0, "w1", "test log content"),
            )
        conn.commit()

    r = client.get(f"{POOL_URL_PREFIX}/unclassified/t-test-1.log")
    assert r.status_code == 200
    assert b"test log content" in r.data


def test_unclassified_log_missing_returns_404(client):
    r = client.get(f"{POOL_URL_PREFIX}/unclassified/no-such-task.log")
    assert r.status_code == 404


def test_patterns_renders(client):
    r = client.get("/patterns")
    assert r.status_code == 200
    # An always-present pattern from patterns.yaml.
    assert b"adb_no_ack" in r.data
    # Anchor id on rows.
    assert b'id="pattern-adb_no_ack"' in r.data


def test_classify_missing_oidc_returns_401(client, monkeypatch):
    """With CLASSIFY_OIDC_AUDIENCE set, a request without a Bearer token is rejected."""
    monkeypatch.setenv("CLASSIFY_OIDC_AUDIENCE", "https://example.com/")
    r = client.post(CLASSIFY_URL)
    assert r.status_code == 401


def test_classify_invalid_oidc_returns_401(client, monkeypatch):
    """A garbage Bearer token fails verification."""
    monkeypatch.setenv("CLASSIFY_OIDC_AUDIENCE", "https://example.com/")
    r = client.post(CLASSIFY_URL, headers={"Authorization": "Bearer not-a-real-jwt"})
    assert r.status_code == 401
