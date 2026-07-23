"""Parity tests: PostgresStorage vs SqliteStorage.

Requires:
  - psycopg[binary] installed
  - PC_TEST_DATABASE_URL env var pointing at a live Postgres instance
    (e.g. postgresql://pc:pc@127.0.0.1:5433/pool_classifier)

To run locally:
  docker compose -f worker_health/pool_classifier_web/docker-compose.yml up -d
  DATABASE_URL=$PC_TEST_DATABASE_URL pipenv run python -m worker_health.pool_classifier_web.scripts.migrate
  PC_TEST_DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier pipenv run pytest tests/test_postgres_storage.py -v
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

psycopg = pytest.importorskip("psycopg")

from worker_health.pool_classifier_web.scripts.migrate import apply_migrations  # noqa: E402
from worker_health.pool_classifier_web.storage import (  # noqa: E402
    PostgresStorage,
    SqliteStorage,
    pool_summaries_global,
)

DSN = os.environ.get("PC_TEST_DATABASE_URL", "")
if not DSN:
    pytest.skip("PC_TEST_DATABASE_URL not set", allow_module_level=True)

POOL_ID = "test-pool/parity"


@pytest.fixture(scope="module", autouse=True)
def _apply_migrations():
    apply_migrations(DSN)


@pytest.fixture(autouse=True)
def _truncate_pg():
    """Wipe test pool rows before each test."""
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            for tbl in (
                "task_results",
                "workers",
                "quarantine_cache",
                "unclassified_logs",
                "worker_availability_transitions",
                "worker_availability_state",
                "worker_availability_mode",
                "collection_coverage_state",
                "collection_coverage_intervals",
            ):
                cur.execute(f"DELETE FROM {tbl} WHERE pool_id = %s", (POOL_ID,))
        conn.commit()
    yield


@pytest.fixture()
def sqlite(tmp_path):
    s = SqliteStorage(pool_id=POOL_ID, results_dir=tmp_path)
    s.init_schema()
    yield s
    s.close()


@pytest.fixture()
def pg():
    s = PostgresStorage(pool_id=POOL_ID, dsn=DSN)
    s.init_schema()
    yield s
    s.close()


def _now_iso(delta_hours=0):
    return (datetime.now(timezone.utc) + timedelta(hours=delta_hours)).isoformat()


def _seed(storage):
    """Insert a worker + a few task results so query methods have data."""
    storage.upsert_worker("w1", "grp-a")
    storage.upsert_worker("w2", None)
    storage.record_task_result("t1", "w1", 0, "completed", None, None, _now_iso(-1), _now_iso(), _now_iso())
    storage.record_task_result("t2", "w1", 1, "failed", "bad_device", "infra", _now_iso(-2), _now_iso(), _now_iso())
    storage.record_task_result("t3", "w2", 0, "failed", "bad_device", "infra", _now_iso(-3), _now_iso(), _now_iso())
    storage.increment_success("w1", _now_iso(-1))
    storage.increment_failure("w1", _now_iso(-2), "bad_device")
    storage.increment_failure("w2", _now_iso(-3), "bad_device")
    storage.commit()


# --- get_seen_tasks ---


def test_get_seen_tasks(sqlite, pg):
    _seed(sqlite)
    _seed(pg)
    sq = sqlite.get_seen_tasks()
    pq = pg.get_seen_tasks()
    assert set(sq.keys()) == set(pq.keys())
    for wid in sq:
        assert sq[wid] == pq[wid]
    assert sqlite.get_seen_task_runs() == pg.get_seen_task_runs()


# --- record_task_result idempotency (ON CONFLICT DO NOTHING) ---


def test_record_task_result_idempotent(sqlite, pg):
    for s in (sqlite, pg):
        s.upsert_worker("w1", "g")
        s.record_task_result("t1", "w1", 0, "completed", None, None, _now_iso(-1), _now_iso(), _now_iso())
        s.record_task_result("t1", "w1", 0, "completed", None, None, _now_iso(-1), _now_iso(), _now_iso())
        s.commit()
    assert sqlite.get_seen_tasks()["w1"] == pg.get_seen_tasks()["w1"]


def test_record_task_result_preserves_retries_and_resolved_time(sqlite, pg):
    started = "2026-07-14T10:00:00+00:00"
    resolved = "2026-07-14T10:05:00+00:00"
    classified = "2026-07-14T10:10:00+00:00"
    for s in (sqlite, pg):
        s.record_task_result("t1", "w1", 0, "failed", "infra", None, started, resolved, classified)
        s.record_task_result("t1", "w1", 1, "completed", None, None, started, resolved, classified)
        s.record_task_result("t1", "w1", 2, "exception", "exception", "malformed-payload", None, None, classified)
        s.commit()

    assert sqlite.get_seen_task_runs() == pg.get_seen_task_runs() == {
        "w1": {("t1", 0), ("t1", 1), ("t1", 2)},
    }
    sqlite_rows = sqlite.db.execute(
        "SELECT run_id, run_resolved FROM task_results ORDER BY run_id",
    ).fetchall()
    with psycopg.connect(DSN) as conn:
        pg_rows = conn.execute(
            "SELECT run_id, run_resolved FROM task_results"
            " WHERE pool_id = %s ORDER BY run_id",
            (POOL_ID,),
        ).fetchall()
    assert [row[0] for row in sqlite_rows] == [row[0] for row in pg_rows]
    assert sqlite_rows[0]["run_resolved"] == pg_rows[0][1].isoformat()
    assert sqlite_rows[2]["run_resolved"] is pg_rows[2][1] is None


# --- upsert_worker: worker_group not overwritten by None ---


def test_upsert_worker_group_preserved(sqlite, pg):
    for s in (sqlite, pg):
        s.upsert_worker("w1", "grp-a")
        s.upsert_worker("w1", None)
        s.commit()
    for s in (sqlite, pg):
        workers = s.query_workers()
        assert workers["w1"]["worker_group"] == "grp-a"


def test_worker_availability_storage_parity(sqlite, pg):
    values = (
        "w1",
        "group-1",
        True,
        False,
        "2026-07-14T10:00:00+00:00",
        None,
        "online",
        "2026-07-14T10:00:00+00:00",
        "2026-07-14T10:01:00+00:00",
    )
    for storage in (sqlite, pg):
        storage.record_worker_availability_transition(*values)
        storage.upsert_worker_availability_state(*values)
        storage.commit()

    sqlite_state = sqlite.get_worker_availability_states()["w1"]
    postgres_state = pg.get_worker_availability_states()["w1"]
    for field in (
        "worker_id",
        "worker_group",
        "available",
        "quarantined",
        "last_contact",
        "quarantine_until",
        "reason",
        "effective_at",
        "observed_at",
    ):
        assert sqlite_state[field] == postgres_state[field]


def test_worker_availability_mode_cutover_resets_only_availability_history(sqlite, pg):
    observed = "2026-07-22T10:00:00+00:00"
    for storage in (sqlite, pg):
        storage.record_task_result(
            "t1",
            "w1",
            0,
            "completed",
            None,
            None,
            "2026-07-22T09:00:00+00:00",
            observed,
            observed,
        )
        storage.record_worker_availability_transition(
            "w1",
            "group-1",
            False,
            False,
            "2026-07-22T08:00:00+00:00",
            None,
            "contact_timeout",
            "2026-07-22T09:00:00+00:00",
            observed,
        )
        storage.upsert_worker_availability_state(
            "w1",
            "group-1",
            False,
            False,
            "2026-07-22T08:00:00+00:00",
            None,
            "contact_timeout",
            "2026-07-22T09:00:00+00:00",
            observed,
        )
        storage.record_collection_coverage("worker_availability", observed, True, 900)
        storage.commit()

        assert storage.ensure_worker_availability_mode("listed", observed) is True
        storage.commit()
        assert storage.get_worker_availability_states() == {}
        assert storage.get_collection_coverage("worker_availability")["intervals"] == []
        assert storage.get_seen_task_runs() == {"w1": {("t1", 0)}}

        assert storage.ensure_worker_availability_mode("listed", observed) is False
        storage.commit()


def test_collection_coverage_storage_parity(sqlite, pg):
    start = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)
    observations = ((0, True), (10, True), (20, False), (30, True), (40, True))
    for storage in (sqlite, pg):
        for minutes, success in observations:
            storage.record_collection_coverage(
                "task_runs",
                (start + timedelta(minutes=minutes)).isoformat(),
                success,
                900,
            )
        storage.commit()

    range_start = start.isoformat()
    range_end = (start + timedelta(minutes=40)).isoformat()
    assert sqlite.get_collection_coverage("task_runs", range_start, range_end) == pg.get_collection_coverage(
        "task_runs",
        range_start,
        range_end,
    )


def test_utilization_storage_parity(sqlite, pg):
    start = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    for storage in (sqlite, pg):
        for source in ("task_runs", "worker_availability"):
            storage.record_collection_coverage(source, start.isoformat(), True, 3600)
            storage.record_collection_coverage(source, end.isoformat(), True, 3600)
        storage.record_worker_availability_transition(
            "w1",
            "group-1",
            True,
            False,
            start.isoformat(),
            None,
            "online",
            start.isoformat(),
            start.isoformat(),
        )
        storage.record_task_result(
            "t1",
            "w1",
            0,
            "completed",
            None,
            None,
            (start + timedelta(minutes=15)).isoformat(),
            (start + timedelta(minutes=45)).isoformat(),
            end.isoformat(),
        )
        storage.commit()

    assert sqlite.get_utilization(start.isoformat(), end.isoformat(), 1800) == pg.get_utilization(
        start.isoformat(),
        end.isoformat(),
        1800,
    )


# --- count_alerting ---


def test_count_alerting(sqlite, pg):
    for s in (sqlite, pg):
        s.upsert_worker("w1", "g")
        s.upsert_worker("w2", "g")
        s.increment_failure("w1", _now_iso(-1), "x")
        s.increment_failure("w1", _now_iso(-2), "x")
        s.increment_failure("w1", _now_iso(-3), "x")
        s.commit()
    assert sqlite.count_alerting(3) == pg.count_alerting(3)
    assert sqlite.count_alerting(4) == pg.count_alerting(4)


# --- count_workers_without_group ---


def test_count_workers_without_group(sqlite, pg):
    for s in (sqlite, pg):
        s.upsert_worker("w1", "grp")
        s.upsert_worker("w2", None)
        s.commit()
    assert sqlite.count_workers_without_group() == pg.count_workers_without_group() == 1


# --- backfill_worker_groups ---


def test_backfill_worker_groups(sqlite, pg):
    for s in (sqlite, pg):
        s.upsert_worker("w1", None)
        s.commit()
        s.backfill_worker_groups([{"workerId": "w1", "workerGroup": "filled"}])
        s.commit()
    assert sqlite.get_worker_group("w1") == pg.get_worker_group("w1") == "filled"


# --- quarantine_cache round-trip ---


def test_quarantine_cache(sqlite, pg):
    until = _now_iso(24)
    for s in (sqlite, pg):
        s.upsert_quarantine_entry("w1", until, "reason", _now_iso(-1), "client", _now_iso())
        s.commit()
    sc = sqlite.get_quarantine_cache()
    pc = pg.get_quarantine_cache()
    assert set(sc.keys()) == set(pc.keys())
    # quarantine_until comparison (both should be parseable ISO strings)
    assert sc["w1"]["reason"] == pc["w1"]["reason"]


# --- update_task_category + update_worker_last_category ---


def test_update_category(sqlite, pg):
    ts = _now_iso(-1)  # same value for both record and increment so the WHERE match works
    for s in (sqlite, pg):
        s.upsert_worker("w1", "g")
        s.record_task_result("t1", "w1", 0, "failed", "unclassified", None, ts, _now_iso(), _now_iso())
        s.increment_failure("w1", ts, "unclassified")
        s.commit()
        s.update_task_category("t1", "w1", "bad_device")
        s.update_worker_last_category("t1", "w1", "bad_device")
        s.commit()
    sw = sqlite.query_workers()
    pw = pg.query_workers()
    assert sw["w1"]["last_failure_category"] == pw["w1"]["last_failure_category"] == "bad_device"


# --- oldest_classified_at ---


def test_oldest_classified_at(sqlite, pg):
    _seed(sqlite)
    _seed(pg)
    sv = sqlite.oldest_classified_at()
    pv = pg.oldest_classified_at()
    assert sv is not None and pv is not None


# --- unclassified log round-trip ---


def test_unclassified_log_round_trip(sqlite, pg):
    for s in (sqlite, pg):
        s.save_unclassified_log("t99", 5, "w1", "log content here")
        # list returns at least this entry
        entries = list(s.list_unclassified_logs())
        assert any(tid == "t99" and text == "log content here" for tid, text, _ in entries)


def test_unclassified_log_unlink(pg):
    """_PgLogRef.unlink() deletes the row."""
    pg.save_unclassified_log("tdel", 1, "w1", "data")
    entries = list(pg.list_unclassified_logs())
    ref = next(ref for tid, _, ref in entries if tid == "tdel")
    ref.unlink()
    pg.commit()
    assert not any(tid == "tdel" for tid, _, _ in pg.list_unclassified_logs())


# --- get_task_info / db_rows_for_category ---


def test_get_task_info(sqlite, pg):
    _seed(sqlite)
    _seed(pg)
    si = sqlite.get_task_info("t1")
    pi = pg.get_task_info("t1")
    assert si["worker_id"] == pi["worker_id"] == "w1"
    assert si["run_state"] == pi["run_state"] == "completed"


def test_db_rows_for_category(sqlite, pg):
    _seed(sqlite)
    _seed(pg)
    sr = sqlite.db_rows_for_category("bad_device")
    pr = pg.db_rows_for_category("bad_device")
    assert {r["task_id"] for r in sr} == {r["task_id"] for r in pr}


# --- top_offenders ---


def test_top_offenders(sqlite, pg):
    _seed(sqlite)
    _seed(pg)
    st = sqlite.top_offenders("bad_device")
    pt = pg.top_offenders("bad_device")
    assert st == pt


# --- query_windowed_sr ---


def test_query_windowed_sr(sqlite, pg):
    _seed(sqlite)
    _seed(pg)
    sv = sqlite.query_windowed_sr()
    pv = pg.query_windowed_sr()
    assert set(sv.keys()) == set(pv.keys())
    for wid in sv:
        for key in ("succ_1d", "fail_1d"):
            assert sv[wid][key] == pv[wid][key], f"{wid}.{key}: sqlite={sv[wid][key]} pg={pv[wid][key]}"


# --- pool_summaries_global: batched query matches the per-pool methods ---


def test_pool_summaries_global_parity(pg):
    _seed(pg)
    since_1h = _now_iso(-1)
    since_24h = _now_iso(-24)
    threshold = 1

    s = pool_summaries_global(DSN, threshold, since_1h, since_24h).get(POOL_ID)
    assert s is not None, "seeded pool should appear in the grouped result"

    # Each batched field must equal the per-pool method it replaces.
    assert s["workers"] == pg.count_workers()
    assert s["alerting"] == pg.count_alerting(threshold)
    assert s["oldest"] == pg.oldest_classified_at()
    assert s["latest"] is not None
    assert s["err_1h"] == pg.count_recent_errors(since_1h)
    assert s["ok_1h"] == pg.count_recent_successes(since_1h)
    assert s["err_24h"] == pg.count_recent_errors(since_24h)
    assert s["ok_24h"] == pg.count_recent_successes(since_24h)


def test_pool_summaries_global_absent_for_empty_pool(pg):
    # No seed → pool has no rows → it should simply not appear in the result.
    summaries = pool_summaries_global(DSN, 1, _now_iso(-1), _now_iso(-24))
    assert POOL_ID not in summaries
