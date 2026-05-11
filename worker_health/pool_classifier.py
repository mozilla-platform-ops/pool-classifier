"""Pool failure classifier: monitors all workers in a TC pool and classifies task failures from logs."""

import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import taskcluster

from worker_health import quarantine_graphql
from worker_health.pool_classifier_web.storage import SqliteStorage
from worker_health.utils import human_delta

try:
    from alive_progress import alive_bar as _alive_bar

    def alive_bar(*args, **kwargs):  # type: ignore[override]
        return _alive_bar(*args, **kwargs)

except ImportError:
    from contextlib import contextmanager

    @contextmanager  # type: ignore[no-redef]
    def alive_bar(total, **kwargs):
        yield lambda: None


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

logger = logging.getLogger(__name__)


def _c(code: str, text: str, use_color: bool = True) -> str:
    return f"\033[{code}m{text}\033[0m" if use_color else text


class PoolClassifier:
    def __init__(
        self,
        provisioner: str,
        worker_type: str,
        results_dir: Optional[Path] = None,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        use_color: bool = True,
        storage=None,
    ):
        self.provisioner = provisioner
        self.worker_type = worker_type
        self.results_dir = results_dir
        self.poll_interval = poll_interval
        self.queue_base = f"{TC_ROOT}/api/queue/v1"
        self.seen_tasks: Dict[str, set] = {}  # in-memory cache, reloaded from storage each cycle
        self._interrupted = False
        self.use_color = use_color
        if storage is not None:
            self.storage = storage
        else:
            assert results_dir is not None, "results_dir required when no storage provided"
            pool_id = f"{provisioner}/{worker_type}"
            self.storage = SqliteStorage(pool_id=pool_id, results_dir=results_dir)
        self._init_tc()

    def _color(self, code: str, text: str) -> str:
        return _c(code, text, self.use_color)

    def _init_tc(self):
        tc_token_json = os.environ.get("TC_TOKEN_JSON")
        if tc_token_json:
            data = json.loads(tc_token_json)
        else:
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
        self.storage.init_schema()
        self.seen_tasks = self.storage.get_seen_tasks()
        seen_count = sum(len(s) for s in self.seen_tasks.values())
        logger.info(f"Storage: {seen_count} previously seen tasks across {len(self.seen_tasks)} workers")

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

    def _poll_one_worker(self, worker: dict) -> Tuple[str, str, List[Tuple]]:
        worker_id = worker["workerId"]
        worker_group = worker["workerGroup"]
        logger.debug(f"  polling {worker_id}")
        tasks = self._new_terminal_tasks(worker_id, worker_group)
        if tasks:
            logger.debug(f"  {worker_id}: {len(tasks)} new terminal task(s)")
        return worker_id, worker_group, tasks

    def _process_results(self, worker_id: str, terminal_tasks: List[Tuple], bar=None, worker_group: str = None):
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

            self.storage.record_task_result(
                task_id,
                worker_id,
                run_id,
                run_state,
                category,
                reason_resolved,
                run_started,
                classified_at,
            )
            self.storage.upsert_worker(worker_id, worker_group)

            if run_state == "completed":
                self.storage.increment_success(worker_id, run_started)
            else:
                self.storage.increment_failure(worker_id, run_started, category)

            self.storage.commit()

    # --- main loop ---

    def classify_cycle(self, workers: Optional[List[dict]] = None) -> dict:
        """One classify pass: poll all workers, process results, write reports. Returns summary dict."""
        if workers is None:
            workers = self._list_workers()
            self._backfill_worker_groups(workers)

        total_workers = len(workers)
        logger.info(f"Scanning {total_workers} workers...")
        poll_results = []
        scanned = 0
        thread_pool = ThreadPool(WORKER_THREAD_COUNT)
        terminated = False
        try:
            with alive_bar(total_workers, title="scanning workers", enrich_print=False) as bar:
                for worker_id, worker_group, tasks in thread_pool.imap_unordered(self._poll_one_worker, workers):
                    scanned += 1
                    bar()
                    poll_results.append((worker_id, worker_group, tasks))
                    if self._interrupted:
                        thread_pool.terminate()
                        terminated = True
                        break
        except Exception as e:
            logger.warning(f"Poll error: {e}")
            thread_pool.terminate()
            terminated = True
        finally:
            if not terminated:
                thread_pool.close()
            thread_pool.join()

        new_total = sum(len(tasks) for _, _wg, tasks in poll_results if tasks)

        if new_total > 0 and not self._interrupted:
            with alive_bar(new_total, title="processing tasks", enrich_print=False) as bar:
                for worker_id, worker_group, terminal_tasks in poll_results:
                    if self._interrupted:
                        break
                    if terminal_tasks:
                        self._process_results(worker_id, terminal_tasks, bar, worker_group)
        else:
            for worker_id, worker_group, terminal_tasks in poll_results:
                if self._interrupted:
                    break
                if terminal_tasks:
                    self._process_results(worker_id, terminal_tasks, worker_group=worker_group)

        self._update_reports()

        alerting_count = self.storage.count_alerting(CONSECUTIVE_FAILURE_ALERT)
        scan_summary = f"{scanned}/{total_workers} workers" if scanned < total_workers else f"{total_workers} workers"
        alert_str = self._color("1;31" if alerting_count > 0 else "1;32", str(alerting_count))
        logger.info(
            f"Scan done: {scan_summary} scanned, {new_total} new terminal tasks, "
            f"{alert_str} workers with ≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures.",
        )
        return {
            "scanned": scanned,
            "total_workers": total_workers,
            "new_terminal": new_total,
            "alerting": alerting_count,
        }

    def run(self):
        signal.signal(signal.SIGINT, self._handle_interrupt)
        self._init_db()
        logger.info(f"Pool classifier starting: {self.provisioner}/{self.worker_type}")
        if self.results_dir:
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
                    self._backfill_worker_groups(workers)
                except Exception as e:
                    logger.warning(f"Failed to refresh worker list: {e}")

            summary = self.classify_cycle(workers)
            alert_str = self._color("1;31" if summary["alerting"] > 0 else "1;32", str(summary["alerting"]))
            logger.info(
                f"{alert_str} workers with ≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures. "
                f"{'Interrupted.' if self._interrupted else f'Sleeping {human_delta(self.poll_interval)}...'}",
            )

            for _ in range(self.poll_interval):
                if self._interrupted:
                    break
                time.sleep(1)

        logger.info("Interrupted — exiting.")
        self.storage.close()
        sys.exit(0)

    def _update_category(
        self,
        task_id: str,
        worker_id: str,
        run_state: str,
        reason_resolved: Optional[str],
        log_text: str,
    ) -> Optional[str]:
        """Classify log_text and update storage if not still unclassified. Returns new category or None."""
        category = self._classify(log_text, run_state, reason_resolved)
        if category == "unclassified":
            return None
        self.storage.update_task_category(task_id, worker_id, category)
        self.storage.update_worker_last_category(task_id, worker_id, category)
        self.storage.commit()
        return category

    def reclassify_unclassified(self, target_category: str = "unclassified", save_unmatched_logs: bool = False):
        """Re-run FAILURE_PATTERNS against saved logs and re-fetch logs for DB entries in target_category."""
        self._init_db()
        reclassified = 0
        refetch_total = 0

        unmatched_dir = self.results_dir / "reclassify_logs" / target_category if self.results_dir else None
        if save_unmatched_logs and unmatched_dir:
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
            for task_id, log_text, log_path in self.storage.list_unclassified_logs():
                saved_task_ids.add(task_id)
                row = self.storage.get_task_info(task_id)
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
                    if save_unmatched_logs and unmatched_dir:
                        (unmatched_dir / f"{task_id}.log").write_text(log_text)

        # Pass 2: DB entries with no saved log — try re-fetching from TC.
        for row in self.storage.db_rows_for_category(target_category):
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
                if save_unmatched_logs and unmatched_dir:
                    (unmatched_dir / f"{task_id}.log").write_text(log_text)

        logger.info(f"Reclassified {reclassified} tasks ({refetch_total} required re-fetch).")

    def _save_unclassified(self, task_id: str, run_id: int, worker_id: str, log_text: str):
        self.storage.save_unclassified_log(task_id, run_id, worker_id, log_text)
        logger.info(f"  saved unclassified log for task={task_id}")

    def _handle_interrupt(self, sig, frame):
        if self._interrupted:
            sys.exit(130)
        self._interrupted = True
        msg = _c("1;33", "[Ctrl-C] Will stop at next best time. Press again to exit immediately.", self.use_color)
        print(f"\n{msg}", file=sys.stderr)

    # --- reports ---

    def _query_workers(self) -> Dict[str, dict]:
        return self.storage.query_workers()

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
        return self.storage.top_offenders(category, n=n, since=since)

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
        return self.storage.query_windowed_sr()

    def _query_heatmap(self, since: str) -> Dict[str, Dict[int, dict]]:
        return self.storage.query_heatmap(since)

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

    def _update_quarantine_cache(self, quarantined: Dict[str, Optional[str]]) -> Dict[str, dict]:
        """Return enriched quarantine data, fetching GraphQL details only for changed/new entries."""
        now_iso = datetime.now(timezone.utc).isoformat()
        cache = self.storage.get_quarantine_cache()

        to_fetch = []
        for wid, until in quarantined.items():
            if wid not in cache or cache[wid]["quarantine_until"] != until:
                wg = self.storage.get_worker_group(wid)
                if wg:
                    to_fetch.append((wid, wg, until))
                else:
                    logger.debug(f"  {wid}: no worker_group, skipping quarantine detail fetch")

        if to_fetch:
            logger.info(f"  fetching quarantine details for {len(to_fetch)} worker(s)...")

            def fetch_one(args):
                wid, wg, until = args
                try:
                    details = quarantine_graphql.view_quarantined_worker_details(
                        provisionerId=self.provisioner,
                        workerType=self.worker_type,
                        workerGroup=wg,
                        workerId=wid,
                        workerPoolId=f"{self.provisioner}/{self.worker_type}",
                    )
                    if details:
                        latest = details[-1]
                        return wid, {
                            "quarantine_until": until,
                            "reason": latest.get("quarantineInfo", ""),
                            "set_at": latest.get("updatedAt", ""),
                            "client_id": latest.get("clientId", ""),
                        }
                except Exception as e:
                    logger.warning(f"  {wid}: failed to fetch quarantine details: {e}")
                return wid, None

            with ThreadPool(min(8, len(to_fetch))) as pool:
                for wid, data in pool.map(fetch_one, to_fetch):
                    if data:
                        self.storage.upsert_quarantine_entry(
                            wid,
                            data["quarantine_until"],
                            data["reason"],
                            data["set_at"],
                            data["client_id"],
                            now_iso,
                        )
                        cache[wid] = {**data, "fetched_at": now_iso}
            self.storage.commit()

        return {
            wid: {
                "quarantine_until": until,
                "reason": cache.get(wid, {}).get("reason", ""),
                "set_at": cache.get(wid, {}).get("set_at", ""),
                "client_id": cache.get(wid, {}).get("client_id", ""),
            }
            for wid, until in quarantined.items()
        }

    def update_report(self):
        """One-shot: init storage, fetch quarantine state, write reports, exit."""
        t0 = time.time()
        logger.info(f"update_report: starting at {datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]} UTC")
        self._init_db()
        self._update_reports()
        self.storage.close()
        elapsed = time.time() - t0
        logger.info(
            f"update_report: done at {datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]} UTC ({elapsed:.2f}s)",
        )

    def _backfill_worker_groups(self, live_workers: List[dict]):
        if not self.storage.count_workers_without_group():
            return
        self.storage.backfill_worker_groups(live_workers)

    def render_html(self) -> str:
        """Return the HTML dashboard string for this pool (does not write to disk)."""
        now = datetime.now(timezone.utc)
        since_1d = (now - timedelta(days=1)).isoformat()
        since_12h = (now - timedelta(hours=12)).isoformat()
        workers = self._query_workers()
        quarantined = self._list_quarantined_workers()
        quarantine_details = self._update_quarantine_cache(quarantined)
        windowed_sr = self._query_windowed_sr()
        heatmap = self._query_heatmap(since_12h)
        return self._write_html(workers, quarantined, windowed_sr, since_1d, heatmap, quarantine_details)

    def render_md(self) -> str:
        """Return the Markdown report string for this pool (does not write to disk)."""
        now = datetime.now(timezone.utc)
        since_1d = (now - timedelta(days=1)).isoformat()
        workers = self._query_workers()
        quarantined = self._list_quarantined_workers()
        windowed_sr = self._query_windowed_sr()
        return self._write_md(workers, quarantined, windowed_sr, since_1d)

    def _update_reports(self):
        def _timed(label, fn):
            t = time.time()
            result = fn()
            logger.info(f"  {label}: {time.time() - t:.2f}s")
            return result

        workers = _timed("query_workers", self._query_workers)
        quarantined = _timed("list_quarantined_workers", self._list_quarantined_workers)
        quarantine_details = _timed("update_quarantine_cache", lambda: self._update_quarantine_cache(quarantined))
        windowed_sr = _timed("query_windowed_sr", self._query_windowed_sr)
        now = datetime.now(timezone.utc)
        since_1d = (now - timedelta(days=1)).isoformat()
        since_12h = (now - timedelta(hours=12)).isoformat()
        heatmap = _timed("query_heatmap", lambda: self._query_heatmap(since_12h))
        md = _timed("write_md", lambda: self._write_md(workers, quarantined, windowed_sr, since_1d))
        html = _timed(
            "write_html",
            lambda: self._write_html(workers, quarantined, windowed_sr, since_1d, heatmap, quarantine_details),
        )
        if self.results_dir:
            self.results_dir.mkdir(parents=True, exist_ok=True)
            (self.results_dir / "OVERVIEW.md").write_text(md)
            (self.results_dir / "OVERVIEW.html").write_text(html)

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
            lines += [f"## Consecutive Failures (≥{CONSECUTIVE_FAILURE_ALERT})", ""]
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

        return "\n".join(lines) + "\n"

    def _write_html(
        self,
        workers: Dict[str, dict],
        quarantined: set = None,
        windowed_sr: Dict[str, dict] = None,
        since_1d: Optional[str] = None,
        heatmap: Dict[str, Dict[int, dict]] = None,
        quarantine_details: Dict[str, dict] = None,
    ):
        now = datetime.now(timezone.utc)
        total_failures = sum(w.get("failures", 0) for w in workers.values())
        total_successes = sum(w.get("successes", 0) for w in workers.values())
        oldest_ts = self.storage.oldest_classified_at()
        clipboard_svg = (
            '<svg aria-hidden="true" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 384 512">'
            '<path fill="currentColor" d="M280 64l40 0c35.3 0 64 28.7 64 64l0 320c0 35.3-28.7 64-64 64L64 512'
            "c-35.3 0-64-28.7-64-64L0 128C0 92.7 28.7 64 64 64l40 0 9.6 0C121 27.5 153.3 0 192 0s71 27.5 78.4"
            " 64l9.6 0zM64 112c-8.8 0-16 7.2-16 16l0 320c0 8.8 7.2 16 16 16l256 0c8.8 0 16-7.2 16-16l0-320"
            "c0-8.8-7.2-16-16-16l-16 0 0 24c0 13.3-10.7 24-24 24l-88 0-88 0c-13.3 0-24-10.7-24-24l0-24-16 0"
            'zm128-8a24 24 0 1 0 0-48 24 24 0 1 0 0 48z"></path></svg>'
        )

        def copy_btn(wid: str, label: str = None) -> str:
            link = tc_link(wid, label)
            btn = f'<span class="hm-copy" data-wid="{wid}" title="Copy hostname">{clipboard_svg}</span>'
            return f'<span style="white-space:nowrap">{btn}{link}</span>'

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

        def _humanize_future(iso: str) -> str:
            diff = (datetime.fromisoformat(iso.replace("Z", "+00:00")) - datetime.now(timezone.utc)).total_seconds()
            if diff <= 0:
                return "expired"
            if diff < 60:
                return f"in {int(diff)}s"
            if diff < 3600:
                return f"in {int(diff // 60)}m"
            if diff < 86400:
                return f"in {int(diff // 3600)}h"
            if diff < 604800:
                return f"in {int(diff // 86400)}d"
            if diff < 2592000:
                return f"in {int(diff // 604800)}w"
            return f"in {int(diff // 2592000)}mo"

        def fmt_expires(iso: Optional[str]) -> str:
            if not iso:
                return "—"
            return f'<span data-utc="{iso}" title="{iso[:19].replace("T", " ")} UTC">{_humanize_future(iso)}</span>'

        def tc_link(wid: str, label: str = None) -> str:
            wg = (workers.get(wid) or {}).get("worker_group")
            if not wg:
                return label or wid
            url = (
                f"https://firefox-ci-tc.services.mozilla.com/provisioners/{self.provisioner}"
                f"/worker-types/{self.worker_type}/workers/{wg}/{wid}?sortBy=started&sortDirection=desc"
            )
            return f'<a href="{url}" target="_blank">{label or wid}</a>'

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
            "  .tz-toggle { margin: 1rem 0 .75rem; font-size: .85em; color: #aaa; }",
            "  .tz-toggle label { margin-right: 1rem; cursor: pointer; }",
            "  .page-nav { display: flex; align-items: center; gap: 0; flex-wrap: wrap; width: fit-content; margin: 0 0 1.5rem; padding: .3rem .6rem; background: #1a1a1a; border-radius: 4px; font-size: .8em; }",
            "  .page-nav a { color: #58a6ff; padding: .2rem .6rem; border-radius: 3px; }",
            "  .page-nav a:hover { color: #a0c8ff; background: #2a2a2a; text-decoration: none; }",
            "  .page-nav a:visited { color: #58a6ff; }",
            "  .page-nav span.sep { color: #444; user-select: none; }",
            "  table { border-collapse: collapse; width: 100%; margin-bottom: 2rem; }",
            "  th { background: #222; color: #aaa; text-align: left; padding: .4rem .8rem; border-bottom: 1px solid #444; cursor: pointer; user-select: none; }",
            "  th:hover { color: #fff; }",
            "  th[data-sort='asc']::after { content: ' ▲'; color: #f90; }",
            "  th[data-sort='desc']::after { content: ' ▼'; color: #f90; }",
            "  td { padding: .35rem .8rem; border-bottom: 1px solid #2a2a2a; }",
            "  table:not(.hm-grid) tr:hover td { background: #1a1a1a; }",
            "  tr.alert td { background: #2a1a00; }",
            "  .hm-cell:hover { outline: 2px solid #fff; outline-offset: -2px; z-index: 1; position: relative; }",
            "  #hm-tip { position: fixed; background: #222; border: 1px solid #555; border-radius: 5px; padding: .5rem .8rem; font-size: .8em; color: #ccc; pointer-events: none; display: none; z-index: 200; line-height: 1.6; }",
            "  #hm-tip .tip-worker { color: #fff; font-weight: bold; margin-bottom: .2rem; }",
            "  #hm-tip .tip-period { color: #888; font-size: .85em; margin-bottom: .4rem; }",
            "  #hm-tip .tip-ok { color: #4c4; }",
            "  #hm-tip .tip-bad { color: #f44; }",
            "  #hm-tip .tip-warn { color: #f90; }",
            "  #hm-tip .tip-dim { color: #888; }",
            "  .ok { color: #4c4; }",
            "  .bad { color: #f44; }",
            "  .warn { color: #f90; }",
            "  ul { padding-left: 1.5rem; }",
            "  li.bad { color: #f44; margin-bottom: .3rem; }",
            "  .quarantine { color: #f90; font-size: .85em; margin-left: .4em; }",
            "  .reason-trunc { display: inline-block; max-width: 22rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: bottom; cursor: default; }",
            "  h3.cat-header { color: #ccc; font-size: .95em; margin: 1rem 0 .2rem; }",
            "  .cat-total { color: #666; font-weight: normal; }",
            "  ul.offenders { margin: 0 0 .6rem 1.2rem; padding: 0; list-style: none; font-size: .85em; color: #aaa; }",
            "  ul.offenders li { padding: .1rem 0; }",
            "  a { color: inherit; text-decoration: none; }",
            "  a:visited { color: #888; }",
            "  a:hover { text-decoration: underline; }",
            "  .hm-wrap { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem 2rem; margin-bottom: 2rem; }",
            "  .hm-block { overflow-x: auto; }",
            "  .hm-grid { border-collapse: collapse; width: auto; margin-bottom: 0; }",
            "  .hm-grid th { background: #1e1e1e; color: #666; padding: .25rem .4rem; font-size: .75em; text-align: center; cursor: default; user-select: none; border: none; }",
            "  .hm-grid th.hm-worker-hdr { text-align: left; color: #aaa; }",
            "  .hm-grid td.hm-worker { padding: .15rem .6rem .15rem 0; font-size: .82em; white-space: nowrap; border: none; }",
            "  .hm-cell { width: 2.2rem; min-width: 2.2rem; height: 1.5rem; padding: 0 !important; border: 2px solid #111 !important; border-radius: 3px; cursor: default; }",
            "  .hm-empty { background: #1c1c1c; }",
            "  .hm-ok { background: #1a4a20; }",
            "  .hm-bts { background: #7a4400; }",
            "  .hm-bdt { background: #7a1515; }",
            "  .hm-both { background: #8a2800; }",
            "  .hm-other { background: #2a2a4a; }",
            "  .hm-legend { display: flex; gap: 1.5rem; font-size: .8em; color: #aaa; margin: .5rem 0 1.2rem; align-items: center; flex-wrap: wrap; }",
            "  .hm-swatch { display: inline-block; width: .9rem; height: .9rem; margin-right: .35rem; vertical-align: middle; border-radius: 2px; border: 1px solid #333; }",
            "  .hm-copy { cursor: pointer; color: #555; margin-right: .35rem; vertical-align: middle; display: inline-block; line-height: 1; }",
            "  .hm-copy:hover { color: #bbb; }",
            "  .hm-copy.copied { color: #4c4; }",
            "  .hm-copy svg { width: .7rem; height: .7rem; }",
            "  .summary-grid { display: grid; grid-template-columns: max-content 1fr; gap: 0 3rem; }",
            "  .summary-grid > div { min-width: 0; }",
            "  .offenders-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: .25rem 2rem; }",
            "</style>",
            "</head>",
            "<body>",
            '<div style="display:flex;align-items:center;gap:1.5rem;margin:0 0 1.5rem">',
            '<pre style="color:#0ff;line-height:1;margin:0;font-size:1rem">',
            " ⣀⡀ ⢀⡀ ⢀⡀ ⡇   ⢀⣀ ⡇ ⢀⣀ ⢀⣀ ⢀⣀ ⠄ ⣰⡁ ⠄ ⢀⡀ ⡀⣀",
            " ⡧⠜ ⠣⠜ ⠣⠜ ⠣   ⠣⠤ ⠣ ⠣⠼ ⠭⠕ ⠭⠕ ⠇ ⢸  ⠇ ⠣⠭ ⠏ ",
            "</pre>",
            f'<span style="color:#ccc;font-size:1.1rem;letter-spacing:.02em;position:relative;top:4px"><a href="https://firefox-ci-tc.services.mozilla.com/provisioners/{self.provisioner}/worker-types/{self.worker_type}?sortBy=Last%20Active&sortDirection=desc" target="_blank">{self.provisioner}/{self.worker_type}</a></span>',
            "</div>",
            f'<p class="gen">Generated: <span class="utc-time" data-utc="{now.isoformat()}">{now.strftime("%Y-%m-%d %H:%M:%S UTC")}</span></p>',
        ]

        if workers:
            total_tasks = total_failures + total_successes
            sr_pct = f"{100 * total_successes / total_tasks:.1f}%" if total_tasks else "—"
            since_str = f" since {fmt_relative(oldest_ts)}" if oldest_ts else ""
            parts.append(
                f'<p class="gen">{total_failures} failures, {total_successes} successes '
                f"across {len(workers)} observed workers{since_str} — success rate: <strong>{sr_pct}</strong></p>",
            )

        parts += [
            '<div class="tz-toggle">',
            '  <label><input type="radio" name="tz" value="local" checked> Local time</label>',
            '  <label><input type="radio" name="tz" value="utc"> UTC</label>',
            '  <label style="margin-left:2rem"><input type="checkbox" id="autorefresh" checked> Auto-refresh (60s)</label>',
            "</div>",
            '<nav class="page-nav">',
            '  <a href="#s-categories">Failure Categories</a><span class="sep">|</span>',
            '  <a href="#s-attention">Consecutive Failures</a><span class="sep">|</span>',
            '  <a href="#s-quarantined">Quarantined</a><span class="sep">|</span>',
            '  <a href="#s-heatmap">Heatmap</a><span class="sep">|</span>',
            '  <a href="#s-all">All Workers</a><span class="sep">|</span>',
            '  <a href="#s-offenders">Top Offenders</a>',
            "</nav>",
        ]

        if category_totals or alerting:
            parts.append('<div class="summary-grid">')

        if category_totals:
            parts += ["<div>", '<h2 id="s-categories">Failure Categories</h2>', "<ul>"]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                parts.append(f"  <li>{cat}: <strong>{count}</strong></li>")
            parts += ["</ul>", "</div>"]

        if alerting:
            parts += [
                "<div>",
                f'<h2 id="s-attention">&#x26A0; Consecutive Failures (≥{CONSECUTIVE_FAILURE_ALERT})</h2>',
                "<ul>",
            ]
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
                    f'  <li class="bad"><strong>{tc_link(wid)}</strong>: {w["consecutive_failures"]} consecutive failures '
                    f"({w.get('last_failure_category', '?')}) — SR: {sr_display} — "
                    f"<span{last_style}>last: {fmt_relative(last_iso)}</span>{q_badge}</li>",
                )
            parts += ["</ul>", "</div>"]

        if category_totals or alerting:
            parts.append("</div>")

        if quarantine_details:
            parts += [
                f'<h2 id="s-quarantined">&#x1F512; Quarantined Workers ({len(quarantine_details)})</h2>',
                "<table>",
                "  <thead><tr>",
                "    <th>Worker</th><th>Reason</th><th>Set By</th><th>Set</th>"
                "<th>Expires</th><th>Remaining</th><th>Consec Fails</th><th>Top Category</th>",
                "  </tr></thead>",
                "  <tbody>",
            ]
            for wid, qd in sorted(quarantine_details.items()):
                w = workers.get(wid, {})
                consec = w.get("consecutive_failures", 0)
                consec_class = (
                    ' class="bad"' if consec >= CONSECUTIVE_FAILURE_ALERT else (' class="warn"' if consec > 0 else "")
                )
                reason = qd.get("reason", "")
                set_at = qd.get("set_at", "")
                client_id = qd.get("client_id", "")
                until = qd.get("quarantine_until", "")
                try:
                    set_by = client_id.split("|")[2] if client_id else "?"
                except IndexError:
                    set_by = client_id or "?"
                remaining = self._quarantine_duration(until)
                remaining_class = (
                    ' class="warn"'
                    if remaining and remaining != "expired"
                    else ' class="bad"'
                    if remaining == "expired"
                    else ""
                )
                remaining_inner = f'<span data-utc="{until}">{remaining}</span>' if until else (remaining or "—")
                parts.append(
                    f"  <tr>"
                    f"<td>{copy_btn(wid)}</td>"
                    f'<td><span class="reason-trunc" title="{reason}">{reason}</span></td>'
                    f"<td>{set_by}</td>"
                    f"<td>{fmt_relative(set_at) if set_at else '—'}</td>"
                    f"<td>{fmt_expires(until)}</td>"
                    f"<td{remaining_class}>{remaining_inner}</td>"
                    f"<td{consec_class}>{consec}</td>"
                    f"<td>{self._top_category(w)}</td>"
                    "</tr>",
                )
            parts += ["  </tbody>", "</table>"]

        if heatmap:
            hour_period = ["< 1h ago"] + [f"{i}–{i + 1}h ago" for i in range(1, 12)]

            def hm_cell(data: Optional[dict], h: int) -> str:
                period = hour_period[h]
                if not data:
                    return f'<td class="hm-cell hm-empty" data-info=\'{{"period":"{period}","ok":0,"bdt":0,"bts":0,"o":0}}\'></td>'
                s, bdt, bts, o = data["s"], data["bdt"], data["bts"], data["o"]
                if bdt and bts:
                    cls = "hm-both"
                elif bdt:
                    cls = "hm-bdt"
                elif bts:
                    cls = "hm-bts"
                elif o:
                    cls = "hm-other"
                else:
                    cls = "hm-ok"
                info = f'{{"period":"{period}","ok":{s},"bdt":{bdt},"bts":{bts},"o":{o}}}'
                return f"<td class=\"hm-cell {cls}\" data-info='{info}'></td>"

            # sort workers: most power-meter failures first, then alpha
            def hm_sort_key(wid):
                hours = heatmap[wid]
                bad = sum(h["bdt"] + h["bts"] for h in hours.values())
                return (-bad, wid)

            hour_labels = ["&lt;1h", "1h", "2h", "3h", "4h", "5h", "6h", "7h", "8h", "9h", "10h", "11h"]
            hm_header = "".join(f"<th>{hour_labels[i]}</th>" for i in range(12))

            sorted_wids = sorted(heatmap.keys(), key=hm_sort_key)
            mid = (len(sorted_wids) + 1) // 2
            halves = [sorted_wids[:mid], sorted_wids[mid:]]

            def hm_table(wids):
                rows = ""
                for wid in wids:
                    q_icon = ' <span class="quarantine">&#x1F512;</span>' if quarantined and wid in quarantined else ""
                    cells = "".join(hm_cell(heatmap[wid].get(h), h) for h in range(12))
                    rows += f'<tr data-wid="{wid}"><td class="hm-worker">{copy_btn(wid)}{q_icon}</td>{cells}</tr>'
                return (
                    f'<div class="hm-block"><table class="hm-grid">'
                    f'<thead><tr><th class="hm-worker-hdr">Worker</th>{hm_header}</tr></thead>'
                    f"<tbody>{rows}</tbody></table></div>"
                )

            parts += [
                '<h2 id="s-heatmap">12h Heatmap</h2>',
                '<p class="gen">Only hosts with activity in the last 12 hours are shown.</p>',
                '<div class="hm-legend">',
                '  <span><span class="hm-swatch" style="background:#1a4a20"></span>success</span>',
                '  <span><span class="hm-swatch" style="background:#7a1515"></span>device-timeout</span>',
                '  <span><span class="hm-swatch" style="background:#7a4400"></span>samples</span>',
                '  <span><span class="hm-swatch" style="background:#8a2800"></span>both</span>',
                '  <span><span class="hm-swatch" style="background:#2a2a4a"></span>other failure</span>',
                '  <span><span class="hm-swatch" style="background:#1c1c1c; border-color:#444"></span>no activity</span>',
                "</div>",
                '<div class="hm-wrap">',
                hm_table(halves[0]),
                hm_table(halves[1]),
                "</div>",
            ]

        total_w = len(workers)
        quarantined_w = sum(1 for wid in workers if wid in quarantined)
        active_w = total_w - quarantined_w
        active_pct = f"{100 * active_w / total_w:.1f}%" if total_w else "—"
        parts += [
            '<h2 id="s-all">All Workers</h2>',
            f'<p class="gen">{active_w} of {total_w} workers available ({active_pct}), {quarantined_w} quarantined.</p>',
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
            wid_cell = f"{tc_link(wid)}{q_cell}"
            parts.append(
                f"  <tr{row_class}>"
                f"<td>{wid_cell}</td>"
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
            parts += [
                "<h2 id=\"s-offenders\">Top Offenders by Category <span class='cat-total'>(last 1d)</span></h2>",
                '<div class="offenders-grid">',
            ]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                offenders = self._top_offenders(cat, since=since_1d)
                offender_items = ""
                for wid, n in offenders:
                    q_badge = ""
                    if quarantined and wid in quarantined:
                        dur = self._quarantine_duration(quarantined[wid])
                        dur_str = f" ({dur})" if dur and dur != "expired" else ""
                        q_badge = f' <span class="quarantine">&#x1F512;{dur_str}</span>'
                    offender_items += f"<li>{tc_link(wid)}{q_badge}: {n}</li>"
                parts.append(
                    f'<div><h3 class="cat-header">{cat} <span class="cat-total">({count} total all-time)</span></h3>'
                    f'<ul class="offenders">{offender_items}</ul></div>',
                )
            parts.append("</div>")

        parts += [
            '<div id="hm-tip"></div>',
            "<script>",
            "  // Heatmap hover card",
            "  const tip = document.getElementById('hm-tip');",
            "  document.querySelectorAll('.hm-cell').forEach(cell => {",
            "    cell.addEventListener('mouseenter', e => {",
            "      const d = JSON.parse(cell.dataset.info);",
            "      const wid = cell.closest('tr').dataset.wid;",
            "      const lines = [`<div class='tip-worker'>${wid}</div>`, `<div class='tip-period'>${d.period}</div>`];",
            "      if (d.ok)  lines.push(`<div class='tip-ok'>✓ ok: ${d.ok}</div>`);",
            "      if (d.bdt) lines.push(`<div class='tip-bad'>✗ device-timeout: ${d.bdt}</div>`);",
            "      if (d.bts) lines.push(`<div class='tip-warn'>⚠ samples: ${d.bts}</div>`);",
            "      if (d.o)   lines.push(`<div class='tip-dim'>• other: ${d.o}</div>`);",
            "      if (!d.ok && !d.bdt && !d.bts && !d.o) lines.push(`<div class='tip-dim'>no activity</div>`);",
            "      tip.innerHTML = lines.join('');",
            "      tip.style.display = 'block';",
            "    });",
            "    cell.addEventListener('mousemove', e => {",
            "      const x = e.clientX + 14, y = e.clientY + 14;",
            "      tip.style.left = (x + tip.offsetWidth > window.innerWidth ? e.clientX - tip.offsetWidth - 8 : x) + 'px';",
            "      tip.style.top  = (y + tip.offsetHeight > window.innerHeight ? e.clientY - tip.offsetHeight - 8 : y) + 'px';",
            "    });",
            "    cell.addEventListener('mouseleave', () => { tip.style.display = 'none'; });",
            "  });",
            "  // Heatmap clipboard copy",
            "  document.querySelectorAll('.hm-copy').forEach(btn => {",
            "    btn.addEventListener('click', e => {",
            "      e.preventDefault(); e.stopPropagation();",
            "      navigator.clipboard.writeText(btn.dataset.wid).then(() => {",
            "        btn.classList.add('copied');",
            "        setTimeout(() => btn.classList.remove('copied'), 1000);",
            "      });",
            "    });",
            "  });",
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
            "    const u = el.querySelector('[data-utc]');",
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
            "      const ad = Date.parse(av), bd = Date.parse(bv);",
            "      if (!isNaN(ad) && !isNaN(bd)) { const cmp = ad - bd; return asc ? cmp : -cmp; }",
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

        return "\n".join(parts)
