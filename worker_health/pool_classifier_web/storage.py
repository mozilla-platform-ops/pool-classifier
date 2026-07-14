"""Storage adapters for pool_classifier: Storage protocol + SqliteStorage impl."""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple


class ClassifyLockBusy(Exception):
    """Raised when a classify cycle is already running for this pool."""


DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id              TEXT PRIMARY KEY,
    worker_group           TEXT,
    successes              INTEGER NOT NULL DEFAULT 0,
    failures               INTEGER NOT NULL DEFAULT 0,
    consecutive_failures   INTEGER NOT NULL DEFAULT 0,
    last_active            TEXT,
    last_success           TEXT,
    last_failure           TEXT,
    last_failure_category  TEXT
);

CREATE TABLE IF NOT EXISTS task_results (
    task_id          TEXT NOT NULL,
    worker_id        TEXT NOT NULL,
    run_id           INTEGER,
    run_state        TEXT NOT NULL,
    category         TEXT,
    reason_resolved  TEXT,
    run_started      TEXT,
    run_resolved     TEXT,
    classified_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_results_worker ON task_results (worker_id);
CREATE INDEX IF NOT EXISTS idx_task_results_started ON task_results (run_started);
CREATE INDEX IF NOT EXISTS idx_task_results_cat ON task_results (category);

CREATE TABLE IF NOT EXISTS quarantine_cache (
    worker_id        TEXT PRIMARY KEY,
    quarantine_until TEXT NOT NULL,
    reason           TEXT,
    set_at           TEXT,
    client_id        TEXT,
    fetched_at       TEXT NOT NULL
);
"""


class SqliteStorage:
    """SQLite-backed storage for a single pool. Uses files in results_dir."""

    def __init__(self, pool_id: str, results_dir: Path):
        self.pool_id = pool_id
        self.results_dir = results_dir
        self._db: Optional[sqlite3.Connection] = None

    @property
    def db(self) -> sqlite3.Connection:
        assert self._db is not None, "init_schema() not called"
        return self._db

    def init_schema(self) -> None:
        self.results_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.results_dir / "pool_classifier.db"
        self._db = sqlite3.connect(db_path, timeout=30)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(DB_SCHEMA)
        self._migrate_task_results_schema()
        try:
            self._db.execute("ALTER TABLE workers ADD COLUMN worker_group TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

    def _migrate_task_results_schema(self) -> None:
        """Upgrade legacy SQLite task rows to one row per Taskcluster run."""
        table_info = list(self.db.execute("PRAGMA table_info(task_results)"))
        columns = {row["name"] for row in table_info}
        if "run_resolved" not in columns:
            self.db.execute("ALTER TABLE task_results ADD COLUMN run_resolved TEXT")

        legacy_primary_key = any(row["pk"] for row in table_info)
        indexes = {row["name"] for row in self.db.execute("PRAGMA index_list(task_results)")}
        if not legacy_primary_key:
            if "idx_task_results_task_run" not in indexes:
                self.db.execute(
                    "CREATE UNIQUE INDEX idx_task_results_task_run"
                    " ON task_results (task_id, COALESCE(run_id, -1))",
                )
            return

        self.db.executescript(
            """
            DROP INDEX IF EXISTS idx_task_results_worker;
            DROP INDEX IF EXISTS idx_task_results_started;
            DROP INDEX IF EXISTS idx_task_results_cat;
            ALTER TABLE task_results RENAME TO task_results_legacy;
            CREATE TABLE task_results (
                task_id          TEXT NOT NULL,
                worker_id        TEXT NOT NULL,
                run_id           INTEGER,
                run_state        TEXT NOT NULL,
                category         TEXT,
                reason_resolved  TEXT,
                run_started      TEXT,
                run_resolved     TEXT,
                classified_at    TEXT NOT NULL
            );
            INSERT OR IGNORE INTO task_results
                (task_id, worker_id, run_id, run_state, category, reason_resolved,
                 run_started, run_resolved, classified_at)
            SELECT task_id, worker_id, run_id, run_state, category, reason_resolved,
                   run_started, run_resolved, classified_at
            FROM task_results_legacy;
            DROP TABLE task_results_legacy;
            CREATE UNIQUE INDEX idx_task_results_task_run
                ON task_results (task_id, COALESCE(run_id, -1));
            CREATE INDEX idx_task_results_worker ON task_results (worker_id);
            CREATE INDEX idx_task_results_started ON task_results (run_started);
            CREATE INDEX idx_task_results_cat ON task_results (category);
            """
        )

    def get_seen_tasks(self) -> Dict[str, set]:
        seen: Dict[str, set] = {}
        for row in self.db.execute("SELECT worker_id, task_id FROM task_results"):
            seen.setdefault(row["worker_id"], set()).add(row["task_id"])
        return seen

    def get_seen_task_runs(self) -> Dict[str, set]:
        seen: Dict[str, set] = {}
        for row in self.db.execute("SELECT worker_id, task_id, run_id FROM task_results"):
            seen.setdefault(row["worker_id"], set()).add((row["task_id"], row["run_id"]))
        return seen

    def record_task_result(
        self,
        task_id: str,
        worker_id: str,
        run_id: Optional[int],
        run_state: str,
        category: Optional[str],
        reason_resolved: Optional[str],
        run_started: Optional[str],
        run_resolved: Optional[str],
        classified_at: str,
    ) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO task_results"
            " (task_id, worker_id, run_id, run_state, category, reason_resolved,"
            "  run_started, run_resolved, classified_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                task_id,
                worker_id,
                run_id,
                run_state,
                category,
                reason_resolved,
                run_started,
                run_resolved,
                classified_at,
            ),
        )

    def upsert_worker(self, worker_id: str, worker_group: Optional[str]) -> None:
        self.db.execute(
            "INSERT INTO workers (worker_id, worker_group) VALUES (?,?)"
            " ON CONFLICT(worker_id) DO UPDATE SET worker_group=excluded.worker_group"
            " WHERE excluded.worker_group IS NOT NULL",
            (worker_id, worker_group),
        )

    def increment_success(self, worker_id: str, run_started: Optional[str]) -> None:
        self.db.execute(
            """UPDATE workers SET
                successes = successes + 1,
                consecutive_failures = 0,
                last_active = MAX(COALESCE(last_active, ''), COALESCE(?, '')),
                last_success = MAX(COALESCE(last_success, ''), COALESCE(?, ''))
            WHERE worker_id = ?""",
            (run_started, run_started, worker_id),
        )

    def increment_failure(self, worker_id: str, run_started: Optional[str], category: Optional[str]) -> None:
        self.db.execute(
            """UPDATE workers SET
                failures = failures + 1,
                consecutive_failures = consecutive_failures + 1,
                last_active = MAX(COALESCE(last_active, ''), COALESCE(?, '')),
                last_failure = MAX(COALESCE(last_failure, ''), COALESCE(?, '')),
                last_failure_category = ?
            WHERE worker_id = ?""",
            (run_started, run_started, category, worker_id),
        )

    def commit(self) -> None:
        self.db.commit()

    def update_task_category(self, task_id: str, worker_id: str, category: str) -> None:
        self.db.execute(
            "UPDATE task_results SET category = ? WHERE task_id = ? AND worker_id = ?",
            (category, task_id, worker_id),
        )

    def update_worker_last_category(self, task_id: str, worker_id: str, category: str) -> None:
        self.db.execute(
            """UPDATE workers SET last_failure_category = ?
               WHERE worker_id = ?
                 AND last_failure = (SELECT run_started FROM task_results WHERE task_id = ? AND worker_id = ?)""",
            (category, worker_id, task_id, worker_id),
        )

    def count_alerting(self, threshold: int) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM workers WHERE consecutive_failures >= ?",
            (threshold,),
        ).fetchone()[0]

    def count_workers(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM workers").fetchone()[0]

    def count_recent_errors(self, since: str) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM task_results WHERE run_state IN ('failed','exception') AND classified_at >= ?",
            (since,),
        ).fetchone()[0]

    def count_recent_successes(self, since: str) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM task_results WHERE run_state = 'completed' AND classified_at >= ?",
            (since,),
        ).fetchone()[0]

    def count_workers_without_group(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM workers WHERE worker_group IS NULL").fetchone()[0]

    def backfill_worker_groups(self, workers: List[dict]) -> None:
        try:
            for w in workers:
                self.db.execute(
                    "UPDATE workers SET worker_group = ? WHERE worker_id = ? AND worker_group IS NULL",
                    (w["workerGroup"], w["workerId"]),
                )
        except sqlite3.OperationalError:
            pass  # DB locked, skip

    def get_quarantine_cache(self) -> Dict[str, dict]:
        return {row["worker_id"]: dict(row) for row in self.db.execute("SELECT * FROM quarantine_cache")}

    def get_worker_group(self, worker_id: str) -> Optional[str]:
        row = self.db.execute("SELECT worker_group FROM workers WHERE worker_id = ?", (worker_id,)).fetchone()
        return row["worker_group"] if row else None

    def upsert_quarantine_entry(
        self,
        worker_id: str,
        quarantine_until: str,
        reason: str,
        set_at: str,
        client_id: str,
        fetched_at: str,
    ) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO quarantine_cache"
            " (worker_id, quarantine_until, reason, set_at, client_id, fetched_at)"
            " VALUES (?,?,?,?,?,?)",
            (worker_id, quarantine_until, reason, set_at, client_id, fetched_at),
        )

    def query_workers(self) -> Dict[str, dict]:
        workers = {}
        for row in self.db.execute("SELECT * FROM workers ORDER BY worker_id"):
            w = dict(row)
            cats = {}
            for cat_row in self.db.execute(
                "SELECT category, COUNT(*) as cnt FROM task_results"
                " WHERE worker_id = ? AND run_state != 'completed' AND category IS NOT NULL"
                " GROUP BY category ORDER BY cnt DESC",
                (w["worker_id"],),
            ):
                cats[cat_row["category"]] = cat_row["cnt"]
            w["failures_by_category"] = cats
            workers[w["worker_id"]] = w
        return workers

    def query_windowed_sr(self) -> Dict[str, dict]:
        now = datetime.now(timezone.utc)
        c1d = (now - timedelta(days=1)).isoformat()
        c3d = (now - timedelta(days=3)).isoformat()
        c7d = (now - timedelta(days=7)).isoformat()
        result = {}
        for row in self.db.execute(
            """
            SELECT
                worker_id,
                SUM(CASE WHEN run_state = 'completed' AND run_started >= :c1d THEN 1 ELSE 0 END) AS succ_1d,
                SUM(CASE WHEN run_state != 'completed' AND run_started >= :c1d THEN 1 ELSE 0 END) AS fail_1d,
                SUM(CASE WHEN run_state = 'completed' AND run_started >= :c3d THEN 1 ELSE 0 END) AS succ_3d,
                SUM(CASE WHEN run_state != 'completed' AND run_started >= :c3d THEN 1 ELSE 0 END) AS fail_3d,
                SUM(CASE WHEN run_state = 'completed' AND run_started >= :c7d THEN 1 ELSE 0 END) AS succ_7d,
                SUM(CASE WHEN run_state != 'completed' AND run_started >= :c7d THEN 1 ELSE 0 END) AS fail_7d
            FROM task_results
            GROUP BY worker_id
            """,
            {"c1d": c1d, "c3d": c3d, "c7d": c7d},
        ):
            result[row["worker_id"]] = dict(row)
        return result

    def query_heatmap(
        self,
        since: str,
        severity_map: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Dict[int, dict]]:
        cat_to_sev: Dict[str, str] = {}
        if severity_map:
            for sev, cats in severity_map.items():
                for cat in cats:
                    cat_to_sev[cat] = sev

        rows = self.db.execute(
            """
            SELECT
                worker_id,
                CAST((strftime('%s', 'now') - strftime('%s', run_started)) / 3600 AS INTEGER) AS hour_ago,
                run_state,
                COALESCE(category, 'unclassified') AS category,
                COUNT(*) AS cnt
            FROM task_results
            WHERE run_started >= ?
            GROUP BY worker_id, hour_ago, run_state, category
            HAVING hour_ago BETWEEN 0 AND 11
            ORDER BY worker_id, hour_ago
            """,
            (since,),
        )
        heatmap: Dict[str, Dict[int, dict]] = {}
        for row in rows:
            cell = heatmap.setdefault(row["worker_id"], {}).setdefault(
                row["hour_ago"],
                {"s": 0, "critical": 0, "high": 0, "low": 0, "cats": {}},
            )
            if row["run_state"] == "completed":
                cell["s"] += row["cnt"]
            else:
                cat = row["category"]
                cell["cats"][cat] = cell["cats"].get(cat, 0) + row["cnt"]
                sev = cat_to_sev.get(cat, "low")
                cell[sev] += row["cnt"]
        return heatmap

    def top_offenders(self, category: str, n: int = 5, since: Optional[str] = None) -> List[Tuple[str, int]]:
        if since:
            rows = self.db.execute(
                "SELECT worker_id, COUNT(*) as cnt FROM task_results"
                " WHERE category = ? AND run_state != 'completed' AND run_started >= ?"
                " GROUP BY worker_id ORDER BY cnt DESC LIMIT ?",
                (category, since, n),
            )
        else:
            rows = self.db.execute(
                "SELECT worker_id, COUNT(*) as cnt FROM task_results"
                " WHERE category = ? AND run_state != 'completed'"
                " GROUP BY worker_id ORDER BY cnt DESC LIMIT ?",
                (category, n),
            )
        return [(row["worker_id"], row["cnt"]) for row in rows]

    def oldest_classified_at(self) -> Optional[str]:
        row = self.db.execute("SELECT MIN(classified_at) FROM task_results").fetchone()
        return row[0] if row and row[0] else None

    def save_unclassified_log(self, task_id: str, run_id: Optional[int], worker_id: str, log_text: str) -> None:
        unclassified_dir = self.results_dir / "unclassified"
        unclassified_dir.mkdir(parents=True, exist_ok=True)
        out = unclassified_dir / f"{task_id}.log"
        header = f"# worker={worker_id} run={run_id} task={task_id}\n\n"
        out.write_text(header + log_text)

    def list_unclassified_logs(self) -> Iterator[Tuple[str, str, Path]]:
        """Yield (task_id, log_text, path) for each saved unclassified log file."""
        unclassified_dir = self.results_dir / "unclassified"
        if not unclassified_dir.exists():
            return
        for log_path in unclassified_dir.glob("*.log"):
            task_id = log_path.stem
            raw = log_path.read_text()
            log_text = raw.split("\n", 2)[2] if raw.count("\n") >= 2 else raw
            yield task_id, log_text, log_path

    def get_task_info(self, task_id: str) -> Optional[dict]:
        row = self.db.execute(
            "SELECT worker_id, run_id, run_state, reason_resolved FROM task_results WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        return dict(row) if row else None

    def db_rows_for_category(self, category: str) -> List[dict]:
        return [
            dict(row)
            for row in self.db.execute(
                "SELECT task_id, worker_id, run_id, run_state, reason_resolved FROM task_results WHERE category = ?",
                (category,),
            ).fetchall()
        ]

    @contextmanager
    def classify_lock(self):
        """No-op for SQLite; single-process CLI has no concurrent callers."""
        yield

    def close(self) -> None:
        if self._db:
            self._db.close()
            self._db = None


try:
    import psycopg
    from psycopg.rows import dict_row as _dict_row
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore
    _dict_row = None  # type: ignore

try:
    import psycopg_pool
except ImportError:  # pragma: no cover
    psycopg_pool = None  # type: ignore


_PG_POOLS = {}
_PG_POOLS_LOCK = threading.Lock()


def _postgres_pool(dsn: str):
    if psycopg_pool is None:
        raise ImportError("psycopg-pool is required for PostgresStorage")

    with _PG_POOLS_LOCK:
        pool = _PG_POOLS.get(dsn)
        if pool is None:
            min_size = int(os.environ.get("PC_DB_POOL_MIN", "1"))
            max_size = int(os.environ.get("PC_DB_POOL_MAX", "5"))
            pool = psycopg_pool.ConnectionPool(
                conninfo=dsn,
                min_size=min_size,
                max_size=max_size,
                kwargs={"row_factory": _dict_row},
                check=psycopg_pool.ConnectionPool.check_connection,
                open=True,
            )
            _PG_POOLS[dsn] = pool
        return pool


class _PgLogRef:
    """Returned by PostgresStorage.list_unclassified_logs() in place of a Path.
    Calling .unlink() deletes the corresponding DB row so reclassify code works unchanged.
    """

    def __init__(self, pool, pool_id: str, task_id: str) -> None:
        self._pool = pool
        self._pool_id = pool_id
        self._task_id = task_id

    def unlink(self) -> None:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM unclassified_logs WHERE pool_id = %s AND task_id = %s",
                    (self._pool_id, self._task_id),
                )


def _to_iso(v) -> Optional[str]:
    """Convert a datetime (returned by psycopg) to an ISO string, pass None through."""
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def count_category_hits_global(dsn: str, since_iso: str) -> Dict[str, int]:
    """Return {category: count} across all pools for task_results.classified_at > since_iso."""
    if psycopg is None:
        raise ImportError("psycopg (psycopg[binary]) is required")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT category, COUNT(*) FROM task_results"
                " WHERE classified_at > %s AND category IS NOT NULL"
                " GROUP BY category",
                (since_iso,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}


def pool_summaries_global(dsn: str, alert_threshold: int, since_1h: str, since_24h: str) -> Dict[str, dict]:
    """Per-pool dashboard summary for ALL pools in two grouped queries.

    Replaces ~7 per-pool queries (run on a per-pool connection) with two
    GROUP BY pool_id queries on one connection. Returns
    {pool_id: {workers, alerting, oldest, err_1h, ok_1h, err_24h, ok_24h}}.
    Pools with no rows simply won't appear — callers must default them.
    """
    if psycopg is None:
        raise ImportError("psycopg (psycopg[binary]) is required")

    summaries: Dict[str, dict] = {}

    def _entry(pool_id: str) -> dict:
        return summaries.setdefault(
            pool_id,
            {"workers": 0, "alerting": 0, "oldest": None, "err_1h": 0, "ok_1h": 0, "err_24h": 0, "ok_24h": 0},
        )

    with psycopg.connect(dsn) as conn:
        # workers table → worker count + alerting count per pool
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pool_id, COUNT(*) AS workers,"
                " COUNT(*) FILTER (WHERE consecutive_failures >= %s) AS alerting"
                " FROM workers GROUP BY pool_id",
                (alert_threshold,),
            )
            for pool_id, workers, alerting in cur.fetchall():
                e = _entry(pool_id)
                e["workers"], e["alerting"] = workers, alerting
        # task_results → oldest + windowed error/success counts per pool, one scan
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pool_id, MIN(classified_at) AS oldest,"
                " COUNT(*) FILTER (WHERE run_state IN ('failed','exception') AND classified_at >= %(s1h)s) AS err_1h,"
                " COUNT(*) FILTER (WHERE run_state = 'completed'            AND classified_at >= %(s1h)s) AS ok_1h,"
                " COUNT(*) FILTER (WHERE run_state IN ('failed','exception') AND classified_at >= %(s24h)s) AS err_24h,"
                " COUNT(*) FILTER (WHERE run_state = 'completed'            AND classified_at >= %(s24h)s) AS ok_24h"
                " FROM task_results GROUP BY pool_id",
                {"s1h": since_1h, "s24h": since_24h},
            )
            for pool_id, oldest, err_1h, ok_1h, err_24h, ok_24h in cur.fetchall():
                e = _entry(pool_id)
                e["oldest"] = _to_iso(oldest)
                e["err_1h"], e["ok_1h"], e["err_24h"], e["ok_24h"] = err_1h, ok_1h, err_24h, ok_24h
    return summaries


class PostgresStorage:
    """Postgres-backed storage for a single pool. Intended for Cloud Run / Cloud SQL."""

    def __init__(self, pool_id: str, dsn: str) -> None:
        if psycopg is None:
            raise ImportError("psycopg (psycopg[binary]) is required for PostgresStorage")
        if psycopg_pool is None:
            raise ImportError("psycopg-pool is required for PostgresStorage")
        self.pool_id = pool_id
        self._dsn = dsn
        self._pool = None
        self._tx_cm = None
        self._tx_conn = None

    def init_schema(self) -> None:
        from worker_health.pool_classifier_web.scripts.migrate import apply_migrations

        apply_migrations(self._dsn)
        self._pool = _postgres_pool(self._dsn)

    def _ensure_pool(self):
        assert self._pool is not None, "init_schema() not called"
        return self._pool

    @contextmanager
    def _cursor(self):
        """Borrow a pooled connection for a read-only operation."""
        if self._tx_conn is not None:
            with self._tx_conn.cursor() as cur:
                yield cur
            return

        with self._ensure_pool().connection() as conn:
            with conn.cursor() as cur:
                yield cur

    @contextmanager
    def _write_cursor(self):
        """Use one checked-out connection for writes until commit()."""
        if self._tx_conn is None:
            self._tx_cm = self._ensure_pool().connection()
            self._tx_conn = self._tx_cm.__enter__()
        try:
            with self._tx_conn.cursor() as cur:
                yield cur
        except Exception as exc:
            self._release_tx(type(exc), exc, exc.__traceback__)
            raise

    def _release_tx(self, exc_type=None, exc=None, tb=None) -> None:
        try:
            self._tx_cm.__exit__(exc_type, exc, tb)
        finally:
            self._tx_cm = None
            self._tx_conn = None

    def get_seen_tasks(self) -> Dict[str, set]:
        seen: Dict[str, set] = {}
        with self._cursor() as cur:
            cur.execute(
                "SELECT worker_id, task_id FROM task_results WHERE pool_id = %s",
                (self.pool_id,),
            )
            for row in cur.fetchall():
                seen.setdefault(row["worker_id"], set()).add(row["task_id"])
        return seen

    def get_seen_task_runs(self) -> Dict[str, set]:
        seen: Dict[str, set] = {}
        with self._cursor() as cur:
            cur.execute(
                "SELECT worker_id, task_id, run_id FROM task_results WHERE pool_id = %s",
                (self.pool_id,),
            )
            for row in cur.fetchall():
                seen.setdefault(row["worker_id"], set()).add((row["task_id"], row["run_id"]))
        return seen

    def record_task_result(
        self,
        task_id: str,
        worker_id: str,
        run_id: Optional[int],
        run_state: str,
        category: Optional[str],
        reason_resolved: Optional[str],
        run_started: Optional[str],
        run_resolved: Optional[str],
        classified_at: str,
    ) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                "INSERT INTO task_results"
                " (pool_id, task_id, worker_id, run_id, run_state, category,"
                "  reason_resolved, run_started, run_resolved, classified_at)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s::timestamptz,%s::timestamptz,%s::timestamptz)"
                " ON CONFLICT DO NOTHING",
                (
                    self.pool_id,
                    task_id,
                    worker_id,
                    run_id,
                    run_state,
                    category,
                    reason_resolved,
                    run_started,
                    run_resolved,
                    classified_at,
                ),
            )

    def upsert_worker(self, worker_id: str, worker_group: Optional[str]) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                "INSERT INTO workers (pool_id, worker_id, worker_group) VALUES (%s,%s,%s)"
                " ON CONFLICT (pool_id, worker_id) DO UPDATE"
                " SET worker_group = EXCLUDED.worker_group"
                " WHERE EXCLUDED.worker_group IS NOT NULL",
                (self.pool_id, worker_id, worker_group),
            )

    def increment_success(self, worker_id: str, run_started: Optional[str]) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                """UPDATE workers SET
                    successes = successes + 1,
                    consecutive_failures = 0,
                    last_active  = GREATEST(last_active,  (%s)::timestamptz),
                    last_success = GREATEST(last_success, (%s)::timestamptz)
                WHERE pool_id = %s AND worker_id = %s""",
                (run_started, run_started, self.pool_id, worker_id),
            )

    def increment_failure(self, worker_id: str, run_started: Optional[str], category: Optional[str]) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                """UPDATE workers SET
                    failures = failures + 1,
                    consecutive_failures = consecutive_failures + 1,
                    last_active  = GREATEST(last_active,  (%s)::timestamptz),
                    last_failure = GREATEST(last_failure, (%s)::timestamptz),
                    last_failure_category = %s
                WHERE pool_id = %s AND worker_id = %s""",
                (run_started, run_started, category, self.pool_id, worker_id),
            )

    def commit(self) -> None:
        if self._tx_conn is None:
            return
        try:
            self._tx_conn.commit()
        finally:
            self._release_tx()

    def update_task_category(self, task_id: str, worker_id: str, category: str) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                "UPDATE task_results SET category = %s WHERE pool_id = %s AND task_id = %s AND worker_id = %s",
                (category, self.pool_id, task_id, worker_id),
            )

    def update_worker_last_category(self, task_id: str, worker_id: str, category: str) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                """UPDATE workers SET last_failure_category = %s
                   WHERE pool_id = %s AND worker_id = %s
                     AND last_failure = (
                         SELECT run_started FROM task_results
                         WHERE pool_id = %s AND task_id = %s AND worker_id = %s
                     )""",
                (category, self.pool_id, worker_id, self.pool_id, task_id, worker_id),
            )

    def count_alerting(self, threshold: int) -> int:
        with self._cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM workers WHERE pool_id = %s AND consecutive_failures >= %s",
                (self.pool_id, threshold),
            )
            return cur.fetchone()["cnt"]

    def count_workers_without_group(self) -> int:
        with self._cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM workers WHERE pool_id = %s AND worker_group IS NULL",
                (self.pool_id,),
            )
            return cur.fetchone()["cnt"]

    def backfill_worker_groups(self, workers: List[dict]) -> None:
        with self._write_cursor() as cur:
            for w in workers:
                cur.execute(
                    "UPDATE workers SET worker_group = %s"
                    " WHERE pool_id = %s AND worker_id = %s AND worker_group IS NULL",
                    (w["workerGroup"], self.pool_id, w["workerId"]),
                )

    def get_quarantine_cache(self) -> Dict[str, dict]:
        with self._cursor() as cur:
            cur.execute("SELECT * FROM quarantine_cache WHERE pool_id = %s", (self.pool_id,))
            result = {}
            for row in cur.fetchall():
                d = dict(row)
                d["quarantine_until"] = _to_iso(d["quarantine_until"])
                d["set_at"] = _to_iso(d["set_at"])
                d["fetched_at"] = _to_iso(d["fetched_at"])
                result[d["worker_id"]] = d
        return result

    def get_worker_group(self, worker_id: str) -> Optional[str]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT worker_group FROM workers WHERE pool_id = %s AND worker_id = %s",
                (self.pool_id, worker_id),
            )
            row = cur.fetchone()
        return row["worker_group"] if row else None

    def upsert_quarantine_entry(
        self,
        worker_id: str,
        quarantine_until: str,
        reason: str,
        set_at: str,
        client_id: str,
        fetched_at: str,
    ) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                "INSERT INTO quarantine_cache"
                " (pool_id, worker_id, quarantine_until, reason, set_at, client_id, fetched_at)"
                " VALUES (%s,%s,%s::timestamptz,%s,%s::timestamptz,%s,%s::timestamptz)"
                " ON CONFLICT (pool_id, worker_id) DO UPDATE SET"
                " quarantine_until = EXCLUDED.quarantine_until,"
                " reason = EXCLUDED.reason,"
                " set_at = EXCLUDED.set_at,"
                " client_id = EXCLUDED.client_id,"
                " fetched_at = EXCLUDED.fetched_at",
                (self.pool_id, worker_id, quarantine_until, reason, set_at, client_id, fetched_at),
            )

    def query_workers(self) -> Dict[str, dict]:
        workers = {}
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM workers WHERE pool_id = %s ORDER BY worker_id",
                (self.pool_id,),
            )
            rows = cur.fetchall()
        for w in rows:
            d = dict(w)
            d["last_active"] = _to_iso(d["last_active"])
            d["last_success"] = _to_iso(d["last_success"])
            d["last_failure"] = _to_iso(d["last_failure"])
            cats: Dict[str, int] = {}
            with self._cursor() as cur:
                cur.execute(
                    "SELECT category, COUNT(*) AS cnt FROM task_results"
                    " WHERE pool_id = %s AND worker_id = %s"
                    "   AND run_state != 'completed' AND category IS NOT NULL"
                    " GROUP BY category ORDER BY cnt DESC",
                    (self.pool_id, d["worker_id"]),
                )
                for cat_row in cur.fetchall():
                    cats[cat_row["category"]] = cat_row["cnt"]
            d["failures_by_category"] = cats
            workers[d["worker_id"]] = d
        return workers

    def query_windowed_sr(self) -> Dict[str, dict]:
        now = datetime.now(timezone.utc)
        c1d = (now - timedelta(days=1)).isoformat()
        c3d = (now - timedelta(days=3)).isoformat()
        c7d = (now - timedelta(days=7)).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT
                    worker_id,
                    SUM(CASE WHEN run_state = 'completed' AND run_started >= %(c1d)s::timestamptz THEN 1 ELSE 0 END) AS succ_1d,
                    SUM(CASE WHEN run_state != 'completed' AND run_started >= %(c1d)s::timestamptz THEN 1 ELSE 0 END) AS fail_1d,
                    SUM(CASE WHEN run_state = 'completed' AND run_started >= %(c3d)s::timestamptz THEN 1 ELSE 0 END) AS succ_3d,
                    SUM(CASE WHEN run_state != 'completed' AND run_started >= %(c3d)s::timestamptz THEN 1 ELSE 0 END) AS fail_3d,
                    SUM(CASE WHEN run_state = 'completed' AND run_started >= %(c7d)s::timestamptz THEN 1 ELSE 0 END) AS succ_7d,
                    SUM(CASE WHEN run_state != 'completed' AND run_started >= %(c7d)s::timestamptz THEN 1 ELSE 0 END) AS fail_7d
                FROM task_results
                WHERE pool_id = %(pool_id)s
                GROUP BY worker_id
                """,
                {"c1d": c1d, "c3d": c3d, "c7d": c7d, "pool_id": self.pool_id},
            )
            return {row["worker_id"]: dict(row) for row in cur.fetchall()}

    def query_heatmap(
        self,
        since: str,
        severity_map: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Dict[int, dict]]:
        cat_to_sev: Dict[str, str] = {}
        if severity_map:
            for sev, cats in severity_map.items():
                for cat in cats:
                    cat_to_sev[cat] = sev

        with self._cursor() as cur:
            cur.execute(
                """
                SELECT
                    worker_id,
                    CAST(EXTRACT(EPOCH FROM (now() - run_started)) / 3600 AS INTEGER) AS hour_ago,
                    run_state,
                    COALESCE(category, 'unclassified') AS category,
                    COUNT(*) AS cnt
                FROM task_results
                WHERE pool_id = %s AND run_started >= %s::timestamptz
                GROUP BY worker_id, hour_ago, run_state, category
                HAVING CAST(EXTRACT(EPOCH FROM (now() - run_started)) / 3600 AS INTEGER) BETWEEN 0 AND 11
                ORDER BY worker_id, hour_ago
                """,
                (self.pool_id, since),
            )
            heatmap: Dict[str, Dict[int, dict]] = {}
            for row in cur.fetchall():
                cell = heatmap.setdefault(row["worker_id"], {}).setdefault(
                    row["hour_ago"],
                    {"s": 0, "critical": 0, "high": 0, "low": 0, "cats": {}},
                )
                if row["run_state"] == "completed":
                    cell["s"] += row["cnt"]
                else:
                    cat = row["category"]
                    cell["cats"][cat] = cell["cats"].get(cat, 0) + row["cnt"]
                    sev = cat_to_sev.get(cat, "low")
                    cell[sev] += row["cnt"]
        return heatmap

    def top_offenders(self, category: str, n: int = 5, since: Optional[str] = None) -> List[Tuple[str, int]]:
        with self._cursor() as cur:
            if since:
                cur.execute(
                    "SELECT worker_id, COUNT(*) AS cnt FROM task_results"
                    " WHERE pool_id = %s AND category = %s"
                    "   AND run_state != 'completed' AND run_started >= %s::timestamptz"
                    " GROUP BY worker_id ORDER BY cnt DESC LIMIT %s",
                    (self.pool_id, category, since, n),
                )
            else:
                cur.execute(
                    "SELECT worker_id, COUNT(*) AS cnt FROM task_results"
                    " WHERE pool_id = %s AND category = %s AND run_state != 'completed'"
                    " GROUP BY worker_id ORDER BY cnt DESC LIMIT %s",
                    (self.pool_id, category, n),
                )
            return [(row["worker_id"], row["cnt"]) for row in cur.fetchall()]

    def oldest_classified_at(self) -> Optional[str]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT MIN(classified_at) AS oldest FROM task_results WHERE pool_id = %s",
                (self.pool_id,),
            )
            row = cur.fetchone()
        return _to_iso(row["oldest"]) if row and row["oldest"] else None

    def save_unclassified_log(self, task_id: str, run_id: Optional[int], worker_id: str, log_text: str) -> None:
        with self._write_cursor() as cur:
            cur.execute(
                "INSERT INTO unclassified_logs (pool_id, task_id, run_id, worker_id, log_text)"
                " VALUES (%s,%s,%s,%s,%s)"
                " ON CONFLICT (pool_id, task_id) DO UPDATE SET"
                " run_id = EXCLUDED.run_id, worker_id = EXCLUDED.worker_id,"
                " log_text = EXCLUDED.log_text, saved_at = now()",
                (self.pool_id, task_id, run_id, worker_id, log_text),
            )

    def list_unclassified_logs(self) -> Iterator[Tuple[str, str, "_PgLogRef"]]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT task_id, log_text FROM unclassified_logs WHERE pool_id = %s",
                (self.pool_id,),
            )
            rows = cur.fetchall()
        for row in rows:
            yield row["task_id"], row["log_text"], _PgLogRef(self._ensure_pool(), self.pool_id, row["task_id"])

    def get_task_info(self, task_id: str) -> Optional[dict]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT worker_id, run_id, run_state, reason_resolved"
                " FROM task_results WHERE pool_id = %s AND task_id = %s",
                (self.pool_id, task_id),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def db_rows_for_category(self, category: str) -> List[dict]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT task_id, worker_id, run_id, run_state, reason_resolved"
                " FROM task_results WHERE pool_id = %s AND category = %s",
                (self.pool_id, category),
            )
            return [dict(row) for row in cur.fetchall()]

    def count_workers(self) -> int:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM workers WHERE pool_id = %s", (self.pool_id,))
            return cur.fetchone()["cnt"]

    def count_recent_errors(self, since: str) -> int:
        with self._cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM task_results"
                " WHERE pool_id = %s AND run_state IN ('failed','exception') AND classified_at >= %s",
                (self.pool_id, since),
            )
            return cur.fetchone()["cnt"]

    def count_recent_successes(self, since: str) -> int:
        with self._cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM task_results"
                " WHERE pool_id = %s AND run_state = 'completed' AND classified_at >= %s",
                (self.pool_id, since),
            )
            return cur.fetchone()["cnt"]

    @contextmanager
    def classify_lock(self):
        """Postgres advisory lock scoped to this pool. Raises ClassifyLockBusy if already held."""
        lock_conn = psycopg.connect(self._dsn)
        try:
            with lock_conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_lock(hashtext('classify:' || %s)::bigint)",
                    (self.pool_id,),
                )
                acquired = cur.fetchone()[0]
            if not acquired:
                raise ClassifyLockBusy(f"classify cycle already running for {self.pool_id}")
            yield
        finally:
            lock_conn.close()

    def close(self) -> None:
        if self._tx_conn is not None:
            try:
                self._tx_conn.rollback()
            finally:
                self._release_tx()
