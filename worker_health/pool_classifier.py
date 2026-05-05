"""Pool failure classifier: monitors all workers in a TC pool and classifies task failures from logs."""

import json
import logging
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import taskcluster
from alive_progress import alive_bar

from worker_health.utils import human_delta

TC_ROOT = "https://firefox-ci-tc.services.mozilla.com"
LOG_HEAD_BYTES = 20480  # 20 KB
LOG_TAIL_BYTES = 51200  # 50 KB

DEFAULT_PROVISIONER = "proj-autophone"
DEFAULT_WORKER_TYPE = "gecko-t-lambda-perf-a55"
DEFAULT_POLL_INTERVAL = 900  # seconds (15 minutes)
WORKER_REFRESH_INTERVAL = 300  # seconds between re-listing workers
WORKER_THREAD_COUNT = 8
CONSECUTIVE_FAILURE_ALERT = 2

# Patterns are checked in order; first match wins.
# Edit these to match the failure modes you care about.
FAILURE_PATTERNS = [
    (r"TypeError: Cannot read properties of undefined \(reading 'samples'\)", "browsertime_samples"),
    (r"ADB server didn't ACK", "adb_no_ack"),
    (r"DEVICE_UNAVAILABLE", "device_unavailable"),
    (r"mozdevice\.DeviceError", "device_error"),
    (r"error: device .* not found", "device_not_found"),
    (r"DeviceDisconnectedError", "device_disconnected"),
    (r"TEST-UNEXPECTED-TIMEOUT \|.+\| Test timed out", "test-unexpected-timeout"),
    (
        r"CRITICAL -  raptor-browsertime Critical: Browsertime process timed out after waiting \d+ seconds for output",
        "browsertime-device-timeout",
    ),
    (r"raptor-browsertime Critical: No data to collect", "raptor-no-data-to-collect"),
    (r"FileNotFoundError:.*mozinfo/android_os_to_api_map\.yaml", "mozinfo-import-error"),
    (r"WARNING - Got \d+ unexpected crashes", "test-failure-unexpected-crashes"),
    (r"WARNING - Got \d+ unexpected statuses", "test-failure-unexpected-statuses"),
    (r"Could not fetch from url https://hg\.[^ ]+ into file .* due to \(Permanent\) HTTP response code", "hg_error"),
    (r"abort: error applying bundle", "hg_error"),
    (r"Must have exactly one connected Android USB device\. 0 found\.", "android-no-devices-found"),
    (
        r"TEST-UNEXPECTED-FAIL \| runtests\.py \| Timed out while waiting for server startup\.",
        "test-failure-unexpected-server-start-timeout",
    ),
    (r"Exception: Difference in Images is too high, suspected faulty run", "test-exception-image-difference-too-high"),
    (r"WARNING -  One or more unittests failed\.", "tests-failed"),
    (r"Unimplemented streams encountered:", "app-crashed-minidump"),
    (r"ERROR -  raptor-mitmproxy Error: Failed to download file", "raptor-mitmproxy-download-failed"),
    (
        r"task payload does not declare a required value, so content authenticity cannot be verified",
        "tc-task-payload-invalid-missing-value",
    ),
    (r"raptor-browsertime Info:.*code: 'ECONNRESET'", "raptor-browsertime-econnreset"),
    (
        r"\[mozharness: \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+Z\] Finished install step \(failed\)",
        "mozharness-failed-to-install",
    ),
]

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id              TEXT PRIMARY KEY,
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
"""

logger = logging.getLogger(__name__)


def _c(code: str, text: str, use_color: bool = True) -> str:
    return f"\033[{code}m{text}\033[0m" if use_color else text


class PoolClassifier:
    def __init__(
        self,
        provisioner: str,
        worker_type: str,
        results_dir: Path,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        use_color: bool = True,
    ):
        self.provisioner = provisioner
        self.worker_type = worker_type
        self.results_dir = results_dir
        self.poll_interval = poll_interval
        self.queue_base = f"{TC_ROOT}/api/queue/v1"
        self.seen_tasks: Dict[str, set] = {}  # in-memory cache, loaded from DB at startup
        self._interrupted = False
        self.db: Optional[sqlite3.Connection] = None
        self.use_color = use_color
        self._init_tc()

    def _color(self, code: str, text: str) -> str:
        return _c(code, text, self.use_color)

    def _init_tc(self):
        token_file = os.path.expanduser(os.environ.get("TC_TOKEN_FILE", "~/.tc_token"))
        with open(token_file) as f:
            data = json.load(f)
        self.tc_queue = taskcluster.Queue(
            {
                "rootUrl": TC_ROOT,
                "credentials": {"clientId": data["clientId"], "accessToken": data["accessToken"]},
            },
        )

    def _init_db(self):
        self.results_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.results_dir / "pool_classifier.db"
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(DB_SCHEMA)
        # load seen task IDs into memory so poll threads can check without hitting DB
        for row in self.db.execute("SELECT worker_id, task_id FROM task_results"):
            self.seen_tasks.setdefault(row["worker_id"], set()).add(row["task_id"])
        seen_count = sum(len(s) for s in self.seen_tasks.values())
        logger.info(f"DB: {db_path} ({seen_count} previously seen tasks across {len(self.seen_tasks)} workers)")

    # --- TC API calls ---

    def _list_workers(self) -> List[dict]:
        workers = []
        query: dict = {}
        while True:
            resp = self.tc_queue.listWorkers(self.provisioner, self.worker_type, query=query)
            workers.extend(resp.get("workers", []))
            token = resp.get("continuationToken")
            if not token:
                break
            query = {"continuationToken": token}
        return workers

    def _get_recent_tasks(self, worker_group: str, worker_id: str) -> List[dict]:
        url = (
            f"{self.queue_base}/provisioners/{self.provisioner}"
            f"/worker-types/{self.worker_type}/workers/{worker_group}/{worker_id}"
        )
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json().get("recentTasks", [])

    def _get_task_status(self, task_id: str) -> Optional[dict]:
        url = f"{self.queue_base}/task/{task_id}/status"
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def _fetch_log_tail(self, task_id: str, run_id: int) -> str:
        url = f"{self.queue_base}/task/{task_id}/runs/{run_id}/artifacts/public/logs/live_backing.log"
        try:
            head_r = requests.get(
                url,
                headers={"Range": f"bytes=0-{LOG_HEAD_BYTES - 1}", "Accept-Encoding": "identity"},
                timeout=60,
            )
            tail_r = requests.get(
                url,
                headers={"Range": f"bytes=-{LOG_TAIL_BYTES}", "Accept-Encoding": "identity"},
                timeout=60,
            )
            head = head_r.text if head_r.status_code in (200, 206) else ""
            tail = tail_r.text if tail_r.status_code in (200, 206) else ""
            return head + tail
        except Exception as e:
            logger.debug(f"Log fetch failed for {task_id}/{run_id}: {e}")
        return ""

    def _classify(self, log_text: str, run_state: str, reason_resolved: Optional[str]) -> str:
        for pattern, category in FAILURE_PATTERNS:
            if re.search(pattern, log_text):
                return category
        if run_state == "exception" and reason_resolved:
            return f"exception_{reason_resolved}"
        return "unclassified"

    # --- polling ---

    def _new_terminal_tasks(self, worker_id: str, worker_group: str) -> List[Tuple]:
        """Return list of (task_id, run_id, run_state, run_started, reason_resolved) for newly terminal runs."""
        if worker_id not in self.seen_tasks:
            self.seen_tasks[worker_id] = set()
        seen = self.seen_tasks[worker_id]
        results = []

        try:
            recent = self._get_recent_tasks(worker_group, worker_id)
        except Exception as e:
            logger.warning(f"{worker_id}: failed to fetch recent tasks: {e}")
            return results

        unseen_task_ids = [t["taskId"] for t in recent if t.get("taskId") and t["taskId"] not in seen]
        if unseen_task_ids:
            logger.debug(f"  {worker_id}: checking {len(unseen_task_ids)} unseen task(s)")

        for task_id in unseen_task_ids:
            try:
                logger.debug(f"  {worker_id}: fetching status for {task_id}")
                status_resp = self._get_task_status(task_id)
            except Exception as e:
                logger.debug(f"{task_id}: status fetch error: {e}")
                continue
            if not status_resp:
                seen.add(task_id)
                continue

            runs = status_resp.get("status", {}).get("runs", [])
            my_runs = [r for r in runs if r.get("workerId") == worker_id]

            if not my_runs:
                seen.add(task_id)
                continue

            latest = my_runs[-1]
            run_state = latest.get("state")
            if run_state not in ("completed", "failed", "exception"):
                logger.debug(f"  {worker_id}: task {task_id} still running (state={run_state}), will re-check")
                continue

            seen.add(task_id)
            logger.debug(f"  {worker_id}: task {task_id} terminal (state={run_state})")
            results.append(
                (task_id, latest.get("runId"), run_state, latest.get("started"), latest.get("reasonResolved")),
            )

        return results

    def _poll_one_worker(self, worker: dict) -> Tuple[str, List[Tuple]]:
        worker_id = worker["workerId"]
        worker_group = worker["workerGroup"]
        logger.debug(f"  polling {worker_id}")
        tasks = self._new_terminal_tasks(worker_id, worker_group)
        if tasks:
            logger.debug(f"  {worker_id}: {len(tasks)} new terminal task(s)")
        return worker_id, tasks

    def _process_results(self, worker_id: str, terminal_tasks: List[Tuple], bar=None):
        for task_id, run_id, run_state, run_started, reason_resolved in terminal_tasks:
            if self._interrupted:
                logger.info(f"  {worker_id}: interrupted, skipping remaining tasks")
                break
            if bar:
                bar()

            classified_at = datetime.now(timezone.utc).isoformat()

            if run_state == "completed":
                category = None
                logger.info(f"  {worker_id}: {self._color('1;32', 'completed')} task={task_id} run={run_id}")
            else:
                log_text = ""
                if run_id is not None:
                    logger.info(f"  {worker_id}: {run_state} task={task_id} run={run_id} — fetching log tail")
                    log_text = self._fetch_log_tail(task_id, run_id)
                    if log_text:
                        logger.info(f"  {worker_id}: task={task_id} log tail fetched ({len(log_text)} bytes)")
                    else:
                        logger.info(f"  {worker_id}: task={task_id} no log available")
                category = self._classify(log_text, run_state, reason_resolved)
                if category == "unclassified":
                    cat_colored = self._color("1;35", category)  # magenta
                else:
                    cat_colored = self._color("1;31", category)  # red
                logger.info(f"  {worker_id}: {run_state} task={task_id} run={run_id} → {cat_colored}")
                if category == "unclassified" and log_text:
                    self._save_unclassified(task_id, run_id, worker_id, log_text)

            self.db.execute(
                "INSERT OR IGNORE INTO task_results"
                " (task_id, worker_id, run_id, run_state, category, reason_resolved, run_started, classified_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (task_id, worker_id, run_id, run_state, category, reason_resolved, run_started, classified_at),
            )
            self.db.execute("INSERT OR IGNORE INTO workers (worker_id) VALUES (?)", (worker_id,))

            if run_state == "completed":
                self.db.execute(
                    """UPDATE workers SET
                        successes = successes + 1,
                        consecutive_failures = 0,
                        last_active = MAX(COALESCE(last_active, ''), COALESCE(?, '')),
                        last_success = MAX(COALESCE(last_success, ''), COALESCE(?, ''))
                    WHERE worker_id = ?""",
                    (run_started, run_started, worker_id),
                )
            else:
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

            self.db.commit()

    # --- main loop ---

    def run(self):
        signal.signal(signal.SIGINT, self._handle_interrupt)
        self._init_db()
        logger.info(f"Pool classifier starting: {self.provisioner}/{self.worker_type}")
        logger.info(f"Results dir: {self.results_dir.resolve()}")

        workers: List[dict] = []
        last_worker_refresh = 0.0

        while not self._interrupted:
            now = time.time()
            if now - last_worker_refresh > WORKER_REFRESH_INTERVAL or not workers:
                try:
                    workers = self._list_workers()
                    last_worker_refresh = now
                    logger.info(f"Worker list: {len(workers)} workers in pool")
                except Exception as e:
                    logger.warning(f"Failed to refresh worker list: {e}")

            total_workers = len(workers)
            logger.info(f"Scanning {total_workers} workers...")
            poll_results = []
            scanned = 0
            pool = ThreadPool(WORKER_THREAD_COUNT)
            terminated = False
            try:
                with alive_bar(total_workers, title="scanning workers", enrich_print=False) as bar:
                    for worker_id, tasks in pool.imap_unordered(self._poll_one_worker, workers):
                        scanned += 1
                        bar()
                        poll_results.append((worker_id, tasks))
                        if self._interrupted:
                            pool.terminate()
                            terminated = True
                            break
            except Exception as e:
                logger.warning(f"Poll error: {e}")
                pool.terminate()
                terminated = True
            finally:
                if not terminated:
                    pool.close()
                pool.join()

            new_total = sum(len(tasks) for _, tasks in poll_results if tasks)

            if new_total > 0 and not self._interrupted:
                with alive_bar(new_total, title="processing tasks", enrich_print=False) as bar:
                    for worker_id, terminal_tasks in poll_results:
                        if self._interrupted:
                            break
                        if terminal_tasks:
                            self._process_results(worker_id, terminal_tasks, bar)
            else:
                for worker_id, terminal_tasks in poll_results:
                    if self._interrupted:
                        break
                    if terminal_tasks:
                        self._process_results(worker_id, terminal_tasks)

            self._update_reports()

            alerting_count = self.db.execute(
                "SELECT COUNT(*) FROM workers WHERE consecutive_failures >= ?",
                (CONSECUTIVE_FAILURE_ALERT,),
            ).fetchone()[0]
            scan_summary = (
                f"{scanned}/{total_workers} workers" if scanned < total_workers else f"{total_workers} workers"
            )
            alert_str = self._color("1;31" if alerting_count > 0 else "1;32", str(alerting_count))
            logger.info(
                f"Scan done: {scan_summary} scanned, {new_total} new terminal tasks, "
                f"{alert_str} workers with ≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures. "
                f"{'Interrupted.' if self._interrupted else f'Sleeping {human_delta(self.poll_interval)}...'}",
            )

            for _ in range(self.poll_interval):
                if self._interrupted:
                    break
                time.sleep(1)

        logger.info("Interrupted — exiting.")
        if self.db:
            self.db.close()
        sys.exit(0)

    def _update_category(
        self,
        task_id: str,
        worker_id: str,
        run_state: str,
        reason_resolved: Optional[str],
        log_text: str,
    ) -> Optional[str]:
        """Classify log_text and update DB if not still unclassified. Returns new category or None."""
        category = self._classify(log_text, run_state, reason_resolved)
        if category == "unclassified":
            return None
        self.db.execute(
            "UPDATE task_results SET category = ? WHERE task_id = ? AND worker_id = ?",
            (category, task_id, worker_id),
        )
        self.db.execute(
            """UPDATE workers SET last_failure_category = ?
               WHERE worker_id = ?
                 AND last_failure = (SELECT run_started FROM task_results WHERE task_id = ? AND worker_id = ?)""",
            (category, worker_id, task_id, worker_id),
        )
        self.db.commit()
        return category

    def reclassify_unclassified(self, target_category: str = "unclassified", save_unmatched_logs: bool = False):
        """Re-run FAILURE_PATTERNS against saved logs and re-fetch logs for DB entries in target_category."""
        self._init_db()
        reclassified = 0
        refetch_total = 0

        unmatched_dir = self.results_dir / "reclassify_logs" / target_category
        if save_unmatched_logs:
            if unmatched_dir.exists():
                for f in unmatched_dir.glob("*.log"):
                    f.unlink()
            unmatched_dir.mkdir(parents=True, exist_ok=True)
            (unmatched_dir / "README.md").write_text(
                "# Temporary reclassify logs\n\n"
                "This directory is wiped and repopulated each time `--reclassify --save-unmatched-logs` is run.\n"
                "Do not store anything here you want to keep.\n",
            )

        # Pass 1: saved log files (only relevant when target is unclassified).
        saved_task_ids = set()
        if target_category == "unclassified":
            unclassified_dir = self.results_dir / "unclassified"
            if unclassified_dir.exists():
                for log_path in unclassified_dir.glob("*.log"):
                    task_id = log_path.stem
                    saved_task_ids.add(task_id)
                    raw = log_path.read_text()
                    log_text = raw.split("\n", 2)[2] if raw.count("\n") >= 2 else raw
                    row = self.db.execute(
                        "SELECT worker_id, run_id, run_state, reason_resolved FROM task_results WHERE task_id = ?",
                        (task_id,),
                    ).fetchone()
                    if row is None:
                        logger.warning(f"  {task_id}: not in DB, skipping")
                        continue
                    category = self._update_category(
                        task_id,
                        row["worker_id"],
                        row["run_state"],
                        row["reason_resolved"],
                        log_text,
                    )
                    if category:
                        log_path.unlink()
                        logger.info(f"  {task_id} ({row['worker_id']}): {target_category} → {category}")
                        reclassified += 1
                    else:
                        logger.info(f"  {task_id}: still {target_category}")
                        if save_unmatched_logs:
                            (unmatched_dir / f"{task_id}.log").write_text(log_text)

        # Pass 2: DB entries with no saved log — try re-fetching from TC.
        db_rows = self.db.execute(
            "SELECT task_id, worker_id, run_id, run_state, reason_resolved FROM task_results WHERE category = ?",
            (target_category,),
        ).fetchall()
        for row in db_rows:
            task_id = row["task_id"]
            if task_id in saved_task_ids:
                continue
            run_id = row["run_id"]
            if run_id is None:
                continue
            log_text = self._fetch_log_tail(task_id, run_id)
            if not log_text:
                continue
            refetch_total += 1
            category = self._update_category(
                task_id,
                row["worker_id"],
                row["run_state"],
                row["reason_resolved"],
                log_text,
            )
            if category:
                logger.info(f"  {task_id} ({row['worker_id']}): {target_category} → {category} (re-fetched)")
                reclassified += 1
            elif target_category == "unclassified":
                self._save_unclassified(task_id, run_id, row["worker_id"], log_text)
                logger.info(f"  {task_id}: still unclassified (log saved)")
            else:
                logger.info(f"  {task_id}: still {target_category} (no pattern match)")
                if save_unmatched_logs:
                    (unmatched_dir / f"{task_id}.log").write_text(log_text)

        logger.info(f"Reclassified {reclassified} tasks ({refetch_total} required re-fetch).")

    def _save_unclassified(self, task_id: str, run_id: int, worker_id: str, log_text: str):
        unclassified_dir = self.results_dir / "unclassified"
        unclassified_dir.mkdir(parents=True, exist_ok=True)
        out = unclassified_dir / f"{task_id}.log"
        header = f"# worker={worker_id} run={run_id} task={task_id}\n\n"
        out.write_text(header + log_text)
        logger.info(f"  saved unclassified log → {out}")

    def _handle_interrupt(self, sig, frame):
        if self._interrupted:
            sys.exit(130)
        self._interrupted = True
        msg = _c("1;33", "[Ctrl-C] Will stop at next best time. Press again to exit immediately.", self.use_color)
        print(f"\n{msg}", file=sys.stderr)

    # --- reports ---

    def _query_workers(self) -> Dict[str, dict]:
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

    def _fmt_dt(self, iso: Optional[str]) -> str:
        if not iso:
            return ""
        return iso[:19].replace("T", " ") + " UTC"

    def _top_category(self, worker_state: dict) -> str:
        cats = worker_state.get("failures_by_category", {})
        if not cats:
            return ""
        return max(cats, key=lambda k: cats[k])

    def _quarantine_duration(self, until_iso: Optional[str]) -> str:
        """Return human-readable time remaining in quarantine, or 'expired'."""
        if not until_iso:
            return ""
        try:
            until = datetime.fromisoformat(until_iso.replace("Z", "+00:00"))
            remaining = (until - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                return "expired"
            return human_delta(remaining)
        except Exception:
            return ""

    def _top_offenders(self, category: str, n: int = 5, since: Optional[str] = None) -> List[Tuple[str, int]]:
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

    def _sr_pct(self, worker_state: dict) -> Optional[float]:
        s = worker_state.get("successes", 0)
        f = worker_state.get("failures", 0)
        return self._sr_from_counts(s, f)

    def _sr_from_counts(self, succ: int, fail: int) -> Optional[float]:
        total = succ + fail
        if total == 0:
            return None
        return succ / total

    def _query_windowed_sr(self) -> Dict[str, dict]:
        """Return per-worker success/failure counts for 1d, 3d, and 7d windows."""
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

    def _list_quarantined_workers(self) -> Dict[str, Optional[str]]:
        """Return dict of worker_id -> quarantineUntil (ISO string) for quarantined workers."""
        quarantined: Dict[str, Optional[str]] = {}
        query: dict = {"quarantined": "true"}
        try:
            while True:
                resp = self.tc_queue.listWorkers(self.provisioner, self.worker_type, query=query)
                for w in resp.get("workers", []):
                    quarantined[w["workerId"]] = w.get("quarantineUntil")
                token = resp.get("continuationToken")
                if not token:
                    break
                query = {"quarantined": "true", "continuationToken": token}
        except Exception as e:
            logger.warning(f"Failed to fetch quarantined workers: {e}")
        return quarantined

    def update_report(self):
        """One-shot: init DB, fetch quarantine state, write reports, exit."""
        self._init_db()
        self._update_reports()
        if self.db:
            self.db.close()

    def _update_reports(self):
        workers = self._query_workers()
        quarantined = self._list_quarantined_workers()
        windowed_sr = self._query_windowed_sr()
        since_1d = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        self._write_md(workers, quarantined, windowed_sr, since_1d)
        self._write_html(workers, quarantined, windowed_sr, since_1d)

    def _write_md(
        self,
        workers: Dict[str, dict],
        quarantined: set = None,
        windowed_sr: Dict[str, dict] = None,
        since_1d: Optional[str] = None,
    ):
        now = datetime.now(timezone.utc)
        total_failures = sum(w.get("failures", 0) for w in workers.values())
        total_successes = sum(w.get("successes", 0) for w in workers.values())

        category_totals: Dict[str, int] = {}
        for w in workers.values():
            for cat, count in w.get("failures_by_category", {}).items():
                category_totals[cat] = category_totals.get(cat, 0) + count

        alerting = {
            wid: w for wid, w in workers.items() if w.get("consecutive_failures", 0) >= CONSECUTIVE_FAILURE_ALERT
        }

        lines = [
            f"# Pool Failure Classifier: {self.provisioner}/{self.worker_type}",
            "",
            "> **Auto-generated by pool_classifier.py — do not edit.**",
            "",
            f"_Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}_",
            "",
        ]

        if workers:
            lines.append(
                f"_{total_failures} failures, {total_successes} successes across {len(workers)} observed workers._",
            )
            lines.append("")

        if category_totals:
            lines += ["## Failure Categories", ""]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                lines.append(f"- {cat}: **{count}**")
            lines.append("")

        if alerting:
            lines += [f"## Needs Attention (≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures)", ""]
            for wid, w in sorted(alerting.items(), key=lambda x: -x[1].get("consecutive_failures", 0)):
                sr = self._sr_pct(w)
                sr_str = f"{sr:.0%}" if sr is not None else "—"
                if quarantined and wid in quarantined:
                    dur = self._quarantine_duration(quarantined[wid])
                    q_flag = f" 🔒 QUARANTINED ({dur} remaining)" if dur and dur != "expired" else " 🔒 QUARANTINED"
                else:
                    q_flag = ""
                lines.append(
                    f"- **{wid}**: {w['consecutive_failures']} consecutive failures "
                    f"({w.get('last_failure_category', '?')}), "
                    f"SR: {sr_str}, "
                    f"last: {self._fmt_dt(w.get('last_failure'))}{q_flag}",
                )
            lines.append("")

        if workers:

            def _wsr(wid, key):
                if not windowed_sr:
                    return "—"
                d = windowed_sr.get(wid, {})
                sr = self._sr_from_counts(d.get(f"succ_{key}", 0), d.get(f"fail_{key}", 0))
                return f"{sr:.0%}" if sr is not None else "—"

            lines += [
                "## All Workers",
                "",
                "| Worker | SR (1d) | SR (3d) | SR (7d) | SR (all) | Successes | Failures | Top Category | Consec Fails | Last Active |",
                "|--------|---------|---------|---------|----------|-----------|----------|--------------|--------------|-------------|",
            ]
            for wid, w in sorted(workers.items()):
                sr_all = self._sr_pct(w)
                sr_all_str = f"{sr_all:.0%}" if sr_all is not None else "—"
                q_flag = ""
                if quarantined and wid in quarantined:
                    dur = self._quarantine_duration(quarantined[wid])
                    q_flag = f" 🔒 ({dur})" if dur and dur != "expired" else " 🔒"
                lines.append(
                    f"| {wid}{q_flag} | {_wsr(wid, '1d')} | {_wsr(wid, '3d')} | {_wsr(wid, '7d')} | {sr_all_str} | "
                    f"{w.get('successes', 0)} | {w.get('failures', 0)} | "
                    f"{self._top_category(w)} | {w.get('consecutive_failures', 0)} | "
                    f"{self._fmt_dt(w.get('last_active'))} |",
                )
            lines.append("")

        if category_totals:
            lines += ["## Top Offenders by Category (last 1d)", ""]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                lines.append(f"### {cat} ({count} total all-time)")
                lines.append("")
                for wid, n in self._top_offenders(cat, since=since_1d):
                    q_flag = ""
                    if quarantined and wid in quarantined:
                        dur = self._quarantine_duration(quarantined[wid])
                        q_flag = f" 🔒 ({dur})" if dur and dur != "expired" else " 🔒"
                    lines.append(f"- {wid}{q_flag}: {n}")
                lines.append("")

        self.results_dir.mkdir(parents=True, exist_ok=True)
        (self.results_dir / "OVERVIEW.md").write_text("\n".join(lines) + "\n")

    def _write_html(
        self,
        workers: Dict[str, dict],
        quarantined: set = None,
        windowed_sr: Dict[str, dict] = None,
        since_1d: Optional[str] = None,
    ):
        now = datetime.now(timezone.utc)
        total_failures = sum(w.get("failures", 0) for w in workers.values())
        total_successes = sum(w.get("successes", 0) for w in workers.values())

        category_totals: Dict[str, int] = {}
        for w in workers.values():
            for cat, count in w.get("failures_by_category", {}).items():
                category_totals[cat] = category_totals.get(cat, 0) + count

        alerting = {
            wid: w for wid, w in workers.items() if w.get("consecutive_failures", 0) >= CONSECUTIVE_FAILURE_ALERT
        }

        def fmt(iso: Optional[str]) -> str:
            if not iso:
                return ""
            display = iso[:19].replace("T", " ") + " UTC"
            return f'<span class="utc-time" data-utc="{iso}">{display}</span>'

        def _humanize(iso: str) -> str:
            diff = (datetime.now(timezone.utc) - datetime.fromisoformat(iso)).total_seconds()
            if diff < 60:
                return f"{int(diff)}s ago"
            if diff < 3600:
                return f"{int(diff // 60)}m ago"
            if diff < 86400:
                return f"{int(diff // 3600)}h ago"
            if diff < 604800:
                return f"{int(diff // 86400)}d ago"
            if diff < 2592000:
                return f"{int(diff // 604800)}w ago"
            return f"{int(diff // 2592000)}mo ago"

        def fmt_relative(iso: Optional[str]) -> str:
            if not iso:
                return ""
            return f'<span class="relative-time" data-utc="{iso}">{_humanize(iso)}</span>'

        def wsr_td(wid: str, key: str) -> str:
            d = (windowed_sr or {}).get(wid, {})
            sr = self._sr_from_counts(d.get(f"succ_{key}", 0), d.get(f"fail_{key}", 0))
            if sr is None:
                return '<td class="">—</td>'
            cls = "ok" if sr >= 0.85 else ("warn" if sr >= 0.5 else "bad")
            return f'<td class="{cls}">{sr:.0%}</td>'

        def sr_class(w: dict) -> str:
            sr = self._sr_pct(w)
            if sr is None:
                return ""
            if sr >= 0.85:
                return "ok"
            if sr >= 0.5:
                return "warn"
            return "bad"

        def sr_str(w: dict) -> str:
            sr = self._sr_pct(w)
            return f"{sr:.0%}" if sr is not None else "—"

        parts = [
            "<!DOCTYPE html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            f"<title>Pool Classifier: {self.provisioner}/{self.worker_type}</title>",
            "<style>",
            "  body { font-family: monospace; background: #111; color: #ccc; padding: 1.5rem; }",
            "  h1 { color: #fff; }",
            "  h2 { color: #f90; margin-top: 2rem; }",
            "  p.gen { color: #666; font-size: .85em; margin-bottom: .5rem; }",
            "  .tz-toggle { margin: 1rem 0 1.5rem; font-size: .85em; color: #aaa; }",
            "  .tz-toggle label { margin-right: 1rem; cursor: pointer; }",
            "  table { border-collapse: collapse; width: 100%; margin-bottom: 2rem; }",
            "  th { background: #222; color: #aaa; text-align: left; padding: .4rem .8rem; border-bottom: 1px solid #444; cursor: pointer; user-select: none; }",
            "  th:hover { color: #fff; }",
            "  th[data-sort='asc']::after { content: ' ▲'; color: #f90; }",
            "  th[data-sort='desc']::after { content: ' ▼'; color: #f90; }",
            "  td { padding: .35rem .8rem; border-bottom: 1px solid #2a2a2a; }",
            "  tr:hover td { background: #1a1a1a; }",
            "  tr.alert td { background: #2a1a00; }",
            "  .ok { color: #4c4; }",
            "  .bad { color: #f44; }",
            "  .warn { color: #f90; }",
            "  ul { padding-left: 1.5rem; }",
            "  li.bad { color: #f44; margin-bottom: .3rem; }",
            "  .quarantine { color: #f90; font-size: .85em; margin-left: .4em; }",
            "  h3.cat-header { color: #ccc; font-size: .95em; margin: 1rem 0 .2rem; }",
            "  .cat-total { color: #666; font-weight: normal; }",
            "  ul.offenders { margin: 0 0 .6rem 1.2rem; padding: 0; list-style: none; font-size: .85em; color: #aaa; }",
            "  ul.offenders li { padding: .1rem 0; }",
            "</style>",
            "</head>",
            "<body>",
            f"<h1>Pool Failure Classifier: {self.provisioner}/{self.worker_type}</h1>",
            f'<p class="gen">Generated: <span class="utc-time" data-utc="{now.isoformat()}">{now.strftime("%Y-%m-%d %H:%M:%S UTC")}</span></p>',
        ]

        if workers:
            parts.append(
                f'<p class="gen">{total_failures} failures, {total_successes} successes '
                f"across {len(workers)} observed workers.</p>",
            )

        parts += [
            '<div class="tz-toggle">',
            '  <label><input type="radio" name="tz" value="local" checked> Local time</label>',
            '  <label><input type="radio" name="tz" value="utc"> UTC</label>',
            '  <label style="margin-left:2rem"><input type="checkbox" id="autorefresh" checked> Auto-refresh (60s)</label>',
            "</div>",
        ]

        if category_totals:
            parts += ["<h2>Failure Categories</h2>", "<ul>"]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                parts.append(f"  <li>{cat}: <strong>{count}</strong></li>")
            parts.append("</ul>")

        if alerting:
            parts += [f"<h2>&#x26A0; Needs Attention (≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures)</h2>", "<ul>"]
            for wid, w in sorted(alerting.items(), key=lambda x: -x[1].get("consecutive_failures", 0)):
                sr_display = f'<span class="{sr_class(w)}">{sr_str(w)}</span>'
                if quarantined and wid in quarantined:
                    dur = self._quarantine_duration(quarantined[wid])
                    dur_str = f" ({dur} remaining)" if dur and dur != "expired" else ""
                    q_badge = f' <span class="quarantine">&#x1F512; quarantined{dur_str}</span>'
                else:
                    q_badge = ""
                last_iso = w.get("last_failure")
                last_age = (
                    (datetime.now(timezone.utc) - datetime.fromisoformat(last_iso)).total_seconds() if last_iso else 0
                )
                if last_age > 7 * 86400:
                    last_style = ' style="color:#666"'
                elif last_age > 3 * 86400:
                    last_style = ' style="color:#ccc"'
                else:
                    last_style = ""
                parts.append(
                    f'  <li class="bad"><strong>{wid}</strong>: {w["consecutive_failures"]} consecutive failures '
                    f"({w.get('last_failure_category', '?')}) — SR: {sr_display} — "
                    f"<span{last_style}>last: {fmt_relative(last_iso)}</span>{q_badge}</li>",
                )
            parts.append("</ul>")

        parts += [
            "<h2>All Workers</h2>",
            "<table>",
            "  <thead><tr>",
            "    <th>Worker</th><th>SR (1d)</th><th>SR (3d)</th><th>SR (7d)</th><th>SR (all)</th>"
            "<th>Successes</th><th>Failures</th><th>Top Category</th><th>Consec Fails</th><th>Last Active</th>",
            "  </tr></thead>",
            "  <tbody>",
        ]

        for wid, w in sorted(workers.items()):
            consec = w.get("consecutive_failures", 0)
            row_class = ' class="alert"' if consec >= CONSECUTIVE_FAILURE_ALERT else ""
            consec_class = (
                ' class="bad"' if consec >= CONSECUTIVE_FAILURE_ALERT else (' class="warn"' if consec > 0 else "")
            )
            failures = w.get("failures", 0)
            fail_class = ' class="bad"' if failures > 0 else ""
            q_cell = ""
            if quarantined and wid in quarantined:
                dur = self._quarantine_duration(quarantined[wid])
                dur_str = f" ({dur})" if dur and dur != "expired" else ""
                q_cell = f' <span class="quarantine">&#x1F512;{dur_str}</span>'
            parts.append(
                f"  <tr{row_class}>"
                f"<td>{wid}{q_cell}</td>"
                f"{wsr_td(wid, '1d')}{wsr_td(wid, '3d')}{wsr_td(wid, '7d')}"
                f'<td class="{sr_class(w)}">{sr_str(w)}</td>'
                f'<td class="ok">{w.get("successes", 0)}</td>'
                f"<td{fail_class}>{failures}</td>"
                f"<td>{self._top_category(w)}</td>"
                f"<td{consec_class}>{consec}</td>"
                f"<td>{fmt(w.get('last_active'))}</td>"
                "</tr>",
            )

        parts += ["  </tbody>", "</table>"]

        if category_totals:
            parts += ["<h2>Top Offenders by Category <span class='cat-total'>(last 1d)</span></h2>"]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                offenders = self._top_offenders(cat, since=since_1d)
                offender_items = ""
                for wid, n in offenders:
                    q_badge = ""
                    if quarantined and wid in quarantined:
                        dur = self._quarantine_duration(quarantined[wid])
                        dur_str = f" ({dur})" if dur and dur != "expired" else ""
                        q_badge = f' <span class="quarantine">&#x1F512;{dur_str}</span>'
                    offender_items += f"<li>{wid}{q_badge}: {n}</li>"
                parts.append(
                    f'<h3 class="cat-header">{cat} <span class="cat-total">({count} total all-time)</span></h3>'
                    f'<ul class="offenders">{offender_items}</ul>',
                )

        parts += [
            "<script>",
            "  // Auto-refresh via localStorage so preference survives reloads.",
            "  const arBox = document.getElementById('autorefresh');",
            "  arBox.checked = localStorage.getItem('autorefresh') !== 'off';",
            "  let arTimer = arBox.checked ? setTimeout(() => location.reload(), 60000) : null;",
            "  arBox.addEventListener('change', () => {",
            "    localStorage.setItem('autorefresh', arBox.checked ? 'on' : 'off');",
            "    if (arBox.checked) { arTimer = setTimeout(() => location.reload(), 60000); }",
            "    else { clearTimeout(arTimer); }",
            "  });",
            "  function formatTime(iso, mode) {",
            "    const d = new Date(iso);",
            "    if (mode === 'utc') return iso.slice(0,19).replace('T',' ') + ' UTC';",
            "    return d.toLocaleString(undefined, {year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'});",
            "  }",
            "  function updateTimes() {",
            "    const mode = document.querySelector('input[name=\"tz\"]:checked').value;",
            "    document.querySelectorAll('.utc-time').forEach(el => el.textContent = formatTime(el.dataset.utc, mode));",
            "  }",
            "  document.querySelectorAll('input[name=\"tz\"]').forEach(r => r.addEventListener('change', updateTimes));",
            "  updateTimes();",
            "  function cellVal(tr, idx) {",
            "    const el = tr.children[idx];",
            "    const u = el.querySelector('.utc-time');",
            "    return u ? u.dataset.utc : el.textContent.trim();",
            "  }",
            "  function sortTable(th) {",
            "    const tbody = th.closest('table').querySelector('tbody');",
            "    const idx = [...th.parentElement.children].indexOf(th);",
            "    const asc = th.dataset.sort === 'desc';",
            "    th.closest('thead').querySelectorAll('th').forEach(h => delete h.dataset.sort);",
            "    th.dataset.sort = asc ? 'asc' : 'desc';",
            "    const rows = [...tbody.querySelectorAll('tr')];",
            "    rows.sort((a, b) => {",
            "      const av = cellVal(a,idx), bv = cellVal(b,idx);",
            "      const an = parseFloat(av.replace('%','')), bn = parseFloat(bv.replace('%',''));",
            "      const cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv);",
            "      return asc ? cmp : -cmp;",
            "    });",
            "    rows.forEach(r => tbody.appendChild(r));",
            "  }",
            "  document.querySelectorAll('th').forEach(th => th.addEventListener('click', () => sortTable(th)));",
            "</script>",
            "</body>",
            "</html>",
            "",
        ]

        self.results_dir.mkdir(parents=True, exist_ok=True)
        (self.results_dir / "OVERVIEW.html").write_text("\n".join(parts))
