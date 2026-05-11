"""Storage adapters for pool_classifier: Storage protocol + SqliteStorage impl."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

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

    def close(self) -> None:
        if self._db:
            self._db.close()
            self._db = None
