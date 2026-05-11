"""Storage adapters for pool_classifier: Storage protocol + SqliteStorage impl."""

from __future__ import annotations

import sqlite3
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
    classified_at    TEXT NOT NULL,
    PRIMARY KEY (task_id, worker_id)
);

CREATE INDEX IF NOT EXISTS idx_task_results_worker ON task_results (worker_id);

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
        try:
            self._db.execute("ALTER TABLE workers ADD COLUMN worker_group TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

    def get_seen_tasks(self) -> Dict[str, set]:
        seen: Dict[str, set] = {}
        for row in self.db.execute("SELECT worker_id, task_id FROM task_results"):
            seen.setdefault(row["worker_id"], set()).add(row["task_id"])
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
        classified_at: str,
    ) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO task_results"
            " (task_id, worker_id, run_id, run_state, category, reason_resolved, run_started, classified_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (task_id, worker_id, run_id, run_state, category, reason_resolved, run_started, classified_at),
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

    def query_heatmap(self, since: str) -> Dict[str, Dict[int, dict]]:
        rows = self.db.execute(
            """
            SELECT
                worker_id,
                CAST((strftime('%s', 'now') - strftime('%s', run_started)) / 3600 AS INTEGER) AS hour_ago,
                SUM(CASE WHEN run_state = 'completed' THEN 1 ELSE 0 END) AS successes,
                SUM(CASE WHEN category = 'browsertime-device-timeout' THEN 1 ELSE 0 END) AS bdt,
                SUM(CASE WHEN category = 'browsertime_samples' THEN 1 ELSE 0 END) AS bts,
                SUM(CASE WHEN run_state != 'completed'
                          AND (category NOT IN ('browsertime-device-timeout', 'browsertime_samples')
                               OR category IS NULL)
                         THEN 1 ELSE 0 END) AS other_fail
            FROM task_results
            WHERE run_started >= ?
            GROUP BY worker_id, hour_ago
            HAVING hour_ago BETWEEN 0 AND 11
            ORDER BY worker_id, hour_ago
            """,
            (since,),
        )
        heatmap: Dict[str, Dict[int, dict]] = {}
        for row in rows:
            heatmap.setdefault(row["worker_id"], {})[row["hour_ago"]] = {
                "s": row["successes"],
                "bdt": row["bdt"],
                "bts": row["bts"],
                "o": row["other_fail"],
            }
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


class _PgLogRef:
    """Returned by PostgresStorage.list_unclassified_logs() in place of a Path.
    Calling .unlink() deletes the corresponding DB row so reclassify code works unchanged.
    """

    def __init__(self, conn, pool_id: str, task_id: str) -> None:
        self._conn = conn
        self._pool_id = pool_id
        self._task_id = task_id

    def unlink(self) -> None:
        with self._conn.cursor() as cur:
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


class PostgresStorage:
    """Postgres-backed storage for a single pool. Intended for Cloud Run / Cloud SQL."""

    def __init__(self, pool_id: str, dsn: str) -> None:
        if psycopg is None:
            raise ImportError("psycopg (psycopg[binary]) is required for PostgresStorage")
        self.pool_id = pool_id
        self._dsn = dsn
        self._conn: Optional[psycopg.Connection] = None  # type: ignore

    @property
    def _db(self) -> psycopg.Connection:  # type: ignore
        assert self._conn is not None, "init_schema() not called"
        return self._conn

    def init_schema(self) -> None:
        from worker_health.pool_classifier_web.scripts.migrate import apply_migrations

        apply_migrations(self._dsn)
        self._conn = psycopg.connect(self._dsn, row_factory=_dict_row)

    def get_seen_tasks(self) -> Dict[str, set]:
        seen: Dict[str, set] = {}
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT worker_id, task_id FROM task_results WHERE pool_id = %s",
                (self.pool_id,),
            )
            for row in cur.fetchall():
                seen.setdefault(row["worker_id"], set()).add(row["task_id"])
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
        classified_at: str,
    ) -> None:
        with self._db.cursor() as cur:
            cur.execute(
                "INSERT INTO task_results"
                " (pool_id, task_id, worker_id, run_id, run_state, category,"
                "  reason_resolved, run_started, classified_at)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s::timestamptz,%s::timestamptz)"
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
                    classified_at,
                ),
            )

    def upsert_worker(self, worker_id: str, worker_group: Optional[str]) -> None:
        with self._db.cursor() as cur:
            cur.execute(
                "INSERT INTO workers (pool_id, worker_id, worker_group) VALUES (%s,%s,%s)"
                " ON CONFLICT (pool_id, worker_id) DO UPDATE"
                " SET worker_group = EXCLUDED.worker_group"
                " WHERE EXCLUDED.worker_group IS NOT NULL",
                (self.pool_id, worker_id, worker_group),
            )

    def increment_success(self, worker_id: str, run_started: Optional[str]) -> None:
        with self._db.cursor() as cur:
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
        with self._db.cursor() as cur:
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
        self._db.commit()

    def update_task_category(self, task_id: str, worker_id: str, category: str) -> None:
        with self._db.cursor() as cur:
            cur.execute(
                "UPDATE task_results SET category = %s WHERE pool_id = %s AND task_id = %s AND worker_id = %s",
                (category, self.pool_id, task_id, worker_id),
            )

    def update_worker_last_category(self, task_id: str, worker_id: str, category: str) -> None:
        with self._db.cursor() as cur:
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
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM workers WHERE pool_id = %s AND consecutive_failures >= %s",
                (self.pool_id, threshold),
            )
            return cur.fetchone()["cnt"]

    def count_workers_without_group(self) -> int:
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM workers WHERE pool_id = %s AND worker_group IS NULL",
                (self.pool_id,),
            )
            return cur.fetchone()["cnt"]

    def backfill_worker_groups(self, workers: List[dict]) -> None:
        with self._db.cursor() as cur:
            for w in workers:
                cur.execute(
                    "UPDATE workers SET worker_group = %s"
                    " WHERE pool_id = %s AND worker_id = %s AND worker_group IS NULL",
                    (w["workerGroup"], self.pool_id, w["workerId"]),
                )

    def get_quarantine_cache(self) -> Dict[str, dict]:
        with self._db.cursor() as cur:
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
        with self._db.cursor() as cur:
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
        with self._db.cursor() as cur:
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
        with self._db.cursor() as cur:
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
            with self._db.cursor() as cur:
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
        with self._db.cursor() as cur:
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

    def query_heatmap(self, since: str) -> Dict[str, Dict[int, dict]]:
        with self._db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    worker_id,
                    CAST(EXTRACT(EPOCH FROM (now() - run_started)) / 3600 AS INTEGER) AS hour_ago,
                    SUM(CASE WHEN run_state = 'completed' THEN 1 ELSE 0 END) AS successes,
                    SUM(CASE WHEN category = 'browsertime-device-timeout' THEN 1 ELSE 0 END) AS bdt,
                    SUM(CASE WHEN category = 'browsertime_samples' THEN 1 ELSE 0 END) AS bts,
                    SUM(CASE WHEN run_state != 'completed'
                              AND (category NOT IN ('browsertime-device-timeout', 'browsertime_samples')
                                   OR category IS NULL)
                             THEN 1 ELSE 0 END) AS other_fail
                FROM task_results
                WHERE pool_id = %s AND run_started >= %s::timestamptz
                GROUP BY worker_id, hour_ago
                HAVING CAST(EXTRACT(EPOCH FROM (now() - run_started)) / 3600 AS INTEGER) BETWEEN 0 AND 11
                ORDER BY worker_id, hour_ago
                """,
                (self.pool_id, since),
            )
            heatmap: Dict[str, Dict[int, dict]] = {}
            for row in cur.fetchall():
                heatmap.setdefault(row["worker_id"], {})[row["hour_ago"]] = {
                    "s": row["successes"],
                    "bdt": row["bdt"],
                    "bts": row["bts"],
                    "o": row["other_fail"],
                }
        return heatmap

    def top_offenders(self, category: str, n: int = 5, since: Optional[str] = None) -> List[Tuple[str, int]]:
        with self._db.cursor() as cur:
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
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT MIN(classified_at) AS oldest FROM task_results WHERE pool_id = %s",
                (self.pool_id,),
            )
            row = cur.fetchone()
        return _to_iso(row["oldest"]) if row and row["oldest"] else None

    def save_unclassified_log(self, task_id: str, run_id: Optional[int], worker_id: str, log_text: str) -> None:
        with self._db.cursor() as cur:
            cur.execute(
                "INSERT INTO unclassified_logs (pool_id, task_id, run_id, worker_id, log_text)"
                " VALUES (%s,%s,%s,%s,%s)"
                " ON CONFLICT (pool_id, task_id) DO UPDATE SET"
                " run_id = EXCLUDED.run_id, worker_id = EXCLUDED.worker_id,"
                " log_text = EXCLUDED.log_text, saved_at = now()",
                (self.pool_id, task_id, run_id, worker_id, log_text),
            )

    def list_unclassified_logs(self) -> Iterator[Tuple[str, str, "_PgLogRef"]]:
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT task_id, log_text FROM unclassified_logs WHERE pool_id = %s",
                (self.pool_id,),
            )
            rows = cur.fetchall()
        for row in rows:
            yield row["task_id"], row["log_text"], _PgLogRef(self._db, self.pool_id, row["task_id"])

    def get_task_info(self, task_id: str) -> Optional[dict]:
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT worker_id, run_id, run_state, reason_resolved"
                " FROM task_results WHERE pool_id = %s AND task_id = %s",
                (self.pool_id, task_id),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def db_rows_for_category(self, category: str) -> List[dict]:
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT task_id, worker_id, run_id, run_state, reason_resolved"
                " FROM task_results WHERE pool_id = %s AND category = %s",
                (self.pool_id, category),
            )
            return [dict(row) for row in cur.fetchall()]

    def count_workers(self) -> int:
        with self._db.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM workers WHERE pool_id = %s", (self.pool_id,))
            return cur.fetchone()["cnt"]

    def count_recent_errors(self, since: str) -> int:
        with self._db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM task_results"
                " WHERE pool_id = %s AND run_state IN ('failed','exception') AND classified_at >= %s",
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
        if self._conn:
            self._conn.close()
            self._conn = None
