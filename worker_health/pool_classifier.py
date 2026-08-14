"""Pool failure classifier: monitors all workers in a TC pool and classifies task failures from logs."""

import collections
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import re
import signal
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import requests
import taskcluster

from worker_health.pool_classifier_web.patterns_registry import all_patterns, categories_by_severity
from worker_health.pool_classifier_web.job_sources import SourceMethod, classify_job_source
from worker_health.pool_classifier_web.registry import AVAILABILITY_MODES
from worker_health.pool_classifier_web.storage import SqliteStorage
from worker_health.utils import human_delta

TC_REQUEST_TIMEOUT = 30  # seconds; SDK has no built-in timeout


class _TimeoutSession(requests.Session):
    """requests.Session that enforces a default timeout on every request."""

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", TC_REQUEST_TIMEOUT)
        return super().request(*args, **kwargs)


try:
    from alive_progress import alive_bar as _alive_bar

    def alive_bar(*args, **kwargs):  # type: ignore[override]
        return _alive_bar(*args, **kwargs)

except ImportError:
    from contextlib import contextmanager

    @contextmanager  # type: ignore[no-redef]
    def alive_bar(total, **kwargs):
        yield lambda: None


TC_ROOT = os.environ.get("TC_ROOT_URL", "https://firefox-ci-tc.services.mozilla.com")
LOG_HEAD_BYTES = 20480  # 20 KB
LOG_TAIL_BYTES = 51200  # 50 KB
# Gzipped-artifact size at which we refuse to stream the log (GCS gunzips on the fly
# without honoring Range, so anything large means downloading the entire decompressed log).
LOG_MAX_GZIP_BYTES = 20 * 1024 * 1024  # 20 MB compressed (~100+ MB uncompressed)
LOG_FETCH_MAX_SECONDS = 30  # hard wall-clock cap for the streamed read
LOG_FETCH_MAX_BYTES = 5 * 1024 * 1024  # hard byte cap for the streamed read

DEFAULT_PROVISIONER = "proj-autophone"
DEFAULT_WORKER_TYPE = "gecko-t-lambda-perf-a55"
DEFAULT_POLL_INTERVAL = 900  # seconds (15 minutes)
WORKER_REFRESH_INTERVAL = 300  # seconds between re-listing workers
WORKER_THREAD_COUNT = 8
TASK_STATUS_THREAD_COUNT = 8
UNRESOLVED_TASK_RUN_BATCH_SIZE = 1000
CONSECUTIVE_FAILURE_ALERT = 2
DEFAULT_WORKER_CONTACT_THRESHOLD_SECONDS = 60 * 60


logger = logging.getLogger(__name__)


def _c(code: str, text: str, use_color: bool = True) -> str:
    return f"\033[{code}m{text}\033[0m" if use_color else text


def _natural_sort_key(value: str) -> Tuple[Tuple[int, object], ...]:
    """Sort embedded numeric fragments numerically, while ignoring case."""
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.lower())
        for part in re.split(r"(\d+)", value)
    )


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _recent_task_window_continuity(
    previous_window: Optional[dict],
    current_window: List[Tuple[str, Optional[int]]],
) -> Tuple[Optional[bool], int]:
    """Classify recent-task continuity without mistaking an idle worker for a gap.

    An empty prior window means the worker had no retained task references at
    the preceding observation.  A later nonempty window is therefore the
    normal idle-to-active transition, not evidence that a retained window was
    skipped.  Only disjoint *nonempty* windows are an unprovable gap.
    """
    if previous_window is None:
        return None, 0
    previous_runs = set(map(tuple, previous_window["recent_tasks"]))
    overlap_count = len(previous_runs.intersection(current_window))
    return (True if not previous_runs else bool(overlap_count)), overlap_count


class _RequestRateLimiter:
    """Thread-safe spacing for a bounded stream of Queue requests."""

    def __init__(self, requests_per_second: float):
        self._interval = 1 / requests_per_second
        self._next_at = 0.0
        self._lock = Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            scheduled_at = max(now, self._next_at)
            self._next_at = scheduled_at + self._interval
        delay = scheduled_at - now
        if delay > 0:
            time.sleep(delay)


class PoolClassifier:
    def __init__(
        self,
        provisioner: str,
        worker_type: str,
        results_dir: Optional[Path] = None,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        use_color: bool = True,
        storage=None,
        worker_contact_threshold_seconds: Optional[int] = None,
        coverage_max_gap_seconds: Optional[int] = None,
        availability_mode: str = "recent_contact",
    ):
        self.provisioner = provisioner
        self.worker_type = worker_type
        self.results_dir = results_dir
        self.poll_interval = poll_interval
        if availability_mode not in AVAILABILITY_MODES:
            allowed = ", ".join(sorted(AVAILABILITY_MODES))
            raise ValueError(f"availability_mode must be one of: {allowed}")
        self.availability_mode = availability_mode
        if worker_contact_threshold_seconds is None:
            worker_contact_threshold_seconds = int(
                os.environ.get(
                    "WORKER_CONTACT_THRESHOLD_SECONDS",
                    str(DEFAULT_WORKER_CONTACT_THRESHOLD_SECONDS),
                ),
            )
        if worker_contact_threshold_seconds <= 0:
            raise ValueError("worker contact threshold must be greater than zero")
        self.worker_contact_threshold = timedelta(seconds=worker_contact_threshold_seconds)
        if coverage_max_gap_seconds is None:
            coverage_max_gap_seconds = int(
                os.environ.get("COLLECTION_COVERAGE_MAX_GAP_SECONDS", str(poll_interval * 2)),
            )
        if coverage_max_gap_seconds <= 0:
            raise ValueError("collection coverage max gap must be greater than zero")
        self.coverage_max_gap_seconds = coverage_max_gap_seconds
        self.queue_base = f"{TC_ROOT}/api/queue/v1"
        self.seen_task_runs: Dict[str, set] = {}  # in-memory cache, reloaded from storage each cycle
        self._interrupted = False
        self.use_color = use_color
        self._cached_workers: List[dict] = []
        self._last_worker_refresh: float = 0.0
        self._cached_quarantined: Optional[Dict] = None
        self._cached_quarantine_details: Optional[Dict] = None
        self._last_quarantine_refresh: float = 0.0
        if storage is not None:
            self.storage = storage
        else:
            assert results_dir is not None, "results_dir required when no storage provided"
            pool_id = f"{provisioner}/{worker_type}"
            self.storage = SqliteStorage(pool_id=pool_id, results_dir=results_dir)
        self.tc_queue = None
        self.tc_worker_manager = None
        try:
            self._init_tc()
        except Exception as e:
            logger.warning("TC credentials unavailable at startup (%s). classify_cycle() will fail without them.", e)

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
        tc_options = {
            "rootUrl": TC_ROOT,
            "credentials": {"clientId": data["clientId"], "accessToken": data["accessToken"]},
        }
        self.tc_queue = taskcluster.Queue(tc_options, session=_TimeoutSession())
        self.tc_worker_manager = taskcluster.WorkerManager(tc_options, session=_TimeoutSession())

    def _ensure_tc(self):
        """Initialize TC client if not already done; raises if credentials are unavailable."""
        if self.tc_queue is None:
            self._init_tc()

    def _init_db(self):
        self.storage.init_schema()
        mode_reset = self.storage.ensure_worker_availability_mode(
            self.availability_mode,
            datetime.now(timezone.utc).isoformat(),
        )
        self.storage.commit()
        if mode_reset:
            logger.warning(
                "[%s/%s] Availability mode changed to %s; reset incompatible availability history and coverage",
                self.provisioner,
                self.worker_type,
                self.availability_mode,
            )
        self.seen_task_runs = self.storage.get_seen_task_runs()
        seen_count = sum(len(s) for s in self.seen_task_runs.values())
        logger.info(
            f"[{self.provisioner}/{self.worker_type}] Storage: {seen_count} "
            f"previously seen task runs across {len(self.seen_task_runs)} workers",
        )

    # --- TC API calls ---

    def _list_workers(self) -> List[dict]:
        self._ensure_tc()
        workers = []
        query: dict = {}
        page = 0
        while True:
            t = time.time()
            resp = self.tc_queue.listWorkers(self.provisioner, self.worker_type, query=query)
            logger.info(f"listWorkers page={page} {time.time() - t:.2f}s ({len(resp.get('workers', []))} workers)")
            workers.extend(resp.get("workers", []))
            token = resp.get("continuationToken")
            if not token:
                break
            query = {"continuationToken": token}
            page += 1
        return workers

    def _get_recent_tasks(self, worker_group: str, worker_id: str) -> List[dict]:
        self._ensure_tc()
        try:
            t = time.time()
            resp = self.tc_queue.getWorker(self.provisioner, self.worker_type, worker_group, worker_id)
            logger.info(f"getWorker {worker_id} {time.time() - t:.2f}s")
            return resp.get("recentTasks", [])
        except taskcluster.exceptions.TaskclusterRestFailure as e:
            if e.status_code == 404:
                return []
            raise

    def _get_task_status(self, task_id: str) -> Optional[dict]:
        self._ensure_tc()
        try:
            t = time.time()
            resp = self.tc_queue.status(task_id)
            logger.info(f"status {task_id} {time.time() - t:.2f}s")
            return resp
        except taskcluster.exceptions.TaskclusterRestFailure as e:
            if e.status_code == 404:
                return None
            raise

    def _record_job_source(self, task_id: str, classified_at: str) -> None:
        """Fetch and cache only the compact source classification for a task."""
        cached = self.storage.get_task_source(task_id)
        if cached and cached["source_method"] != int(SourceMethod.TASK_FETCH_FAILED):
            return
        try:
            self._ensure_tc()
            task = self.tc_queue.task(task_id)
        except Exception as exc:
            logger.warning("%s: task-definition fetch failed: %s", task_id, exc)
            task = None
        source = classify_job_source(task)
        self.storage.record_task_source(task_id, source.source, int(source.method), classified_at)

    def _get_task_definition_with_retry(
        self, task_id: str, retries: int, rate_limiter: _RequestRateLimiter,
    ) -> Optional[dict]:
        """Fetch one task definition with bounded retry/backoff."""
        for attempt in range(retries + 1):
            try:
                rate_limiter.wait()
                return self.tc_queue.task(task_id)
            except taskcluster.exceptions.TaskclusterRestFailure as exc:
                if exc.status_code in (400, 404):
                    logger.warning(
                        "%s: task definition is permanently unavailable (HTTP %s); recording unknown source",
                        task_id, exc.status_code,
                    )
                    return None
                if attempt == retries:
                    raise
                delay = 0.25 * (2**attempt)
                logger.warning("%s: task-definition fetch failed; retrying in %.2fs", task_id, delay)
                time.sleep(delay)
            except Exception:
                if attempt == retries:
                    raise
                delay = 0.25 * (2**attempt)
                logger.warning("%s: task-definition fetch failed; retrying in %.2fs", task_id, delay)
                time.sleep(delay)
        raise AssertionError("unreachable")

    def backfill_job_sources(
        self, batch_size: int = 500, concurrency: int = 5, retries: int = 2,
        requests_per_second: float = 5.0, should_stop: Optional[Callable[[], bool]] = None,
        not_before: Optional[str] = None,
    ) -> dict:
        """Classify one durable batch of task definitions missing source metadata."""
        if batch_size <= 0 or concurrency <= 0 or retries < 0 or requests_per_second <= 0:
            raise ValueError("batch_size, concurrency, and requests_per_second must be positive; retries must not be negative")
        self._ensure_tc()
        stats = {"selected_tasks": 0, "fetched": 0, "classified": 0, "unknown": 0, "errors": 0}
        task_ids = self.storage.list_task_ids_missing_source(batch_size, not_before=not_before)
        stats["selected_tasks"] = len(task_ids)
        if not task_ids:
            if should_stop is not None:
                stats["stop_requested"] = should_stop()
            return stats
        rate_limiter = _RequestRateLimiter(requests_per_second)
        fetched: Dict[str, Optional[dict]] = {}
        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="job-source-backfill") as executor:
            futures = {
                executor.submit(self._get_task_definition_with_retry, task_id, retries, rate_limiter): task_id
                for task_id in task_ids
            }
            for future in as_completed(futures):
                task_id = futures[future]
                try:
                    fetched[task_id] = future.result()
                    if fetched[task_id] is not None:
                        stats["fetched"] += 1
                except Exception:
                    stats["errors"] += 1
                    logger.exception("%s: task-definition fetch failed after retries", task_id)
        classified_at = datetime.now(timezone.utc).isoformat()
        for task_id, task in fetched.items():
            source = classify_job_source(task)
            self.storage.record_task_source(task_id, source.source, int(source.method), classified_at)
            stats["classified"] += 1
            if source.source == "unknown":
                stats["unknown"] += 1
        self.storage.commit()
        if should_stop is not None:
            stats["stop_requested"] = should_stop()
        logger.info("job-source backfill: %s", stats)
        return stats

    def _get_task_status_with_retry(
        self, task_id: str, retries: int, rate_limiter: _RequestRateLimiter,
    ) -> Optional[dict]:
        """Fetch Queue status with bounded retry/backoff for backfill work."""
        for attempt in range(retries + 1):
            try:
                rate_limiter.wait()
                return self._get_task_status(task_id)
            except Exception as exc:  # Queue clients expose several transient exception types.
                if attempt == retries:
                    raise
                delay = 0.25 * (2**attempt)
                logger.warning("%s: status fetch failed (%s); retrying in %.2fs", task_id, exc, delay)
                time.sleep(delay)
        raise AssertionError("unreachable")

    def backfill_start_lag(
        self, batch_size: int = 500, concurrency: int = 5, retries: int = 2, requests_per_second: float = 5.0,
        state_file: Optional[Path] = None, should_stop: Optional[Callable[[], bool]] = None,
        not_before: Optional[str] = None,
    ) -> dict:
        """Enrich one bounded batch of stored runs with Queue schedule metadata.

        This is intentionally separate from terminal-run collection: it updates
        only metadata that was absent when an existing result was first stored.
        Re-running it is safe because enriched rows no longer qualify.
        """
        if batch_size <= 0 or concurrency <= 0 or retries < 0 or requests_per_second <= 0:
            raise ValueError("batch_size, concurrency, and requests_per_second must be positive; retries must not be negative")
        self._ensure_tc()
        stats = {"selected_runs": 0, "selected_tasks": 0, "enriched_runs": 0, "expired_tasks": 0, "unmatched_runs": 0, "transient_failures": 0}
        with self.storage.classify_lock():
            state = self._load_backfill_state(state_file)
            pool_state = state.setdefault("pools", {}).setdefault(f"{self.provisioner}/{self.worker_type}", {})
            expired_task_ids = {item for item in pool_state.get("expired_task_ids", []) if isinstance(item, str)}
            unmatched_runs = {
                (item[0], item[1]) for item in pool_state.get("unmatched_runs", [])
                if isinstance(item, list) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], int)
            }
            backlog = self.storage.count_task_runs_missing_schedule(not_before=not_before)
            logger.info(
                "[%s/%s] Start-lag backfill backlog: %d runs across %d tasks missing Queue scheduled metadata; "
                "state file skips %d expired tasks and %d unmatched runs",
                self.provisioner, self.worker_type, backlog["runs"], backlog["tasks"],
                len(expired_task_ids), len(unmatched_runs),
            )
            rows = []
            offset = 0
            while len(rows) < batch_size:
                candidates = self.storage.list_task_runs_missing_schedule(batch_size, offset, not_before=not_before)
                if not candidates:
                    break
                offset += len(candidates)
                for row in candidates:
                    key = (row["task_id"], row["run_id"])
                    if row["task_id"] in expired_task_ids or key in unmatched_runs:
                        continue
                    else:
                        rows.append(row)
                        if len(rows) == batch_size:
                            break
                if len(candidates) < batch_size:
                    break
            by_task: Dict[str, List[int]] = {}
            for row in rows:
                by_task.setdefault(row["task_id"], []).append(row["run_id"])
            stats["selected_runs"] = len(rows)
            stats["selected_tasks"] = len(by_task)
            if not by_task:
                if should_stop is not None:
                    stats["stop_requested"] = should_stop()
                return stats

            statuses: Dict[str, Optional[dict]] = {}
            rate_limiter = _RequestRateLimiter(requests_per_second)
            with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="start-lag-backfill") as executor:
                futures = {
                    executor.submit(self._get_task_status_with_retry, task_id, retries, rate_limiter): task_id
                    for task_id in by_task
                }
                for future in as_completed(futures):
                    task_id = futures[future]
                    try:
                        statuses[task_id] = future.result()
                    except Exception:
                        logger.exception("%s: status fetch failed after retries", task_id)
                        stats["transient_failures"] += 1

            for task_id, run_ids in by_task.items():
                status_response = statuses.get(task_id)
                if status_response is None:
                    # `None` is a definitive Queue 404; a missing key was a transient failure.
                    if task_id in statuses:
                        stats["expired_tasks"] += 1
                        expired_task_ids.add(task_id)
                    continue
                runs = {run.get("runId"): run for run in status_response.get("status", {}).get("runs", [])}
                for run_id in run_ids:
                    run = runs.get(run_id)
                    if not run or not run.get("scheduled"):
                        stats["unmatched_runs"] += 1
                        unmatched_runs.add((task_id, run_id))
                        continue
                    if self.storage.enrich_task_run_queue_metadata(
                        task_id, run_id, run["scheduled"], run.get("reasonCreated"),
                    ):
                        stats["enriched_runs"] += 1
            self.storage.commit()
            pool_state["expired_task_ids"] = sorted(expired_task_ids)
            pool_state["unmatched_runs"] = [list(item) for item in sorted(unmatched_runs)]
            self._save_backfill_state(state_file, state)
            if should_stop is not None:
                # Check only after the whole batch is durable. Callers use this
                # to stop between batches without losing Queue results or writes.
                stats["stop_requested"] = should_stop()
        logger.info("start-lag backfill: %s", stats)
        return stats

    @staticmethod
    def _load_backfill_state(state_file: Optional[Path]) -> dict:
        if state_file is None or not state_file.exists():
            return {"version": 1, "pools": {}}
        try:
            state = json.loads(state_file.read_text())
            if isinstance(state, dict) and isinstance(state.get("pools"), dict):
                return state
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("ignoring unreadable start-lag backfill state %s: %s", state_file, exc)
        return {"version": 1, "pools": {}}

    @staticmethod
    def _save_backfill_state(state_file: Optional[Path], state: dict) -> None:
        if state_file is None:
            return
        state_file.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=state_file.parent, delete=False) as output:
            json.dump(state, output, indent=2, sort_keys=True)
            output.write("\n")
            temporary_path = Path(output.name)
        temporary_path.replace(state_file)

    def _fetch_log_tail(self, task_id: str, run_id: int) -> Tuple[str, str]:
        """Fetch head+tail of the task log with size, time, and byte caps.

        Returns (log_text, status):
          - "ok":         log fetched (possibly truncated by caps)
          - "too_large":  gzipped artifact exceeds LOG_MAX_GZIP_BYTES; skipped
          - "empty":      fetch failed or produced nothing
        """
        url = f"{self.queue_base}/task/{task_id}/runs/{run_id}/artifacts/public/logs/live_backing.log"

        # HEAD-first size gate. GCS transparently gunzips on egress for these artifacts and
        # silently ignores Range, so a large log means downloading the entire decompressed
        # stream. Check the stored (gzipped) size and bail out if it's huge.
        try:
            t = time.time()
            h = requests.head(url, allow_redirects=True, timeout=(10, 15))
            gz_len = h.headers.get("x-goog-stored-content-length")
            gz_enc = h.headers.get("x-goog-stored-content-encoding")
            logger.info(
                f"fetch_log {task_id}/{run_id} head_check {time.time() - t:.2f}s "
                f"status={h.status_code} stored_len={gz_len} stored_enc={gz_enc} url={h.url}",
            )
            if gz_len is not None:
                try:
                    if int(gz_len) > LOG_MAX_GZIP_BYTES:
                        logger.warning(
                            f"fetch_log {task_id}/{run_id} skipping: gzipped artifact "
                            f"{int(gz_len) / (1024 * 1024):.1f} MB > "
                            f"{LOG_MAX_GZIP_BYTES / (1024 * 1024):.0f} MB cap",
                        )
                        return "", "too_large"
                except ValueError:
                    pass
        except Exception as e:
            logger.warning(f"fetch_log {task_id}/{run_id} HEAD failed: {e}")
            # fall through and try the streamed fetch anyway

        # Streamed read with wall-clock + byte caps. Keep first LOG_HEAD_BYTES bytes and
        # the rolling last LOG_TAIL_BYTES bytes. If we hit a cap we still return what we have.
        head_buf = bytearray()
        tail_buf: "collections.deque[int]" = collections.deque(maxlen=LOG_TAIL_BYTES)
        total = 0
        start = time.monotonic()
        aborted = None
        try:
            with requests.get(url, stream=True, timeout=(10, 15)) as r:
                if r.status_code not in (200, 206):
                    logger.warning(
                        f"fetch_log {task_id}/{run_id} stream status={r.status_code}",
                    )
                    return "", "empty"
                for chunk in r.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if len(head_buf) < LOG_HEAD_BYTES:
                        head_buf.extend(chunk[: LOG_HEAD_BYTES - len(head_buf)])
                    tail_buf.extend(chunk)
                    if total >= LOG_FETCH_MAX_BYTES:
                        aborted = "byte_cap"
                        break
                    if time.monotonic() - start > LOG_FETCH_MAX_SECONDS:
                        aborted = "time_cap"
                        break
        except Exception as e:
            logger.warning(
                f"fetch_log {task_id}/{run_id} stream failed after {time.monotonic() - start:.2f}s, total={total}: {e}",
            )
            if total == 0:
                return "", "empty"

        elapsed = time.monotonic() - start
        logger.info(
            f"fetch_log {task_id}/{run_id} stream {elapsed:.2f}s total={total} "
            f"head={len(head_buf)} tail={len(tail_buf)} aborted={aborted}",
        )
        if total == 0:
            return "", "empty"
        head_str = bytes(head_buf).decode("utf-8", errors="replace")
        tail_str = bytes(tail_buf).decode("utf-8", errors="replace")
        return head_str + tail_str, "ok"

    def _classify(self, log_text: str, run_state: str, reason_resolved: Optional[str]) -> str:
        for pattern in all_patterns():
            if pattern.search(log_text):
                return pattern.name
        if run_state == "exception" and reason_resolved:
            return f"exception_{reason_resolved}"
        return "unclassified"

    # --- polling ---

    def _resolve_observed_task_runs(self, references: List[dict]) -> Tuple[Dict[str, List[Tuple]], bool]:
        """Resolve durable task-run references, returning terminal runs by worker."""
        by_task: Dict[str, Dict[str, set]] = {}
        for reference in references:
            by_task.setdefault(reference["task_id"], {}).setdefault(reference["worker_id"], set()).add(reference["run_id"])

        results: Dict[str, List[Tuple]] = {}
        complete = True
        for task_id, workers in by_task.items():
            checked_at = datetime.now(timezone.utc).isoformat()
            for run_ids in workers.values():
                for run_id in run_ids:
                    self.storage.record_task_run_check(task_id, run_id, checked_at)
            # Check attempts survive Queue failures and process interruptions.
            self.storage.commit()
            try:
                logger.debug("  fetching status for %s", task_id)
                status_resp = self._get_task_status(task_id)
            except Exception as exc:
                logger.warning("%s: status fetch error: %s", task_id, exc)
                complete = False
                continue
            if not status_resp:
                # Queue's `None` result is its definitive 404 signal.  Persist
                # an expiry marker so stale references are never retried.
                logger.info("task %s no longer has a status record; expiring observed references", task_id)
                for worker_id, run_ids in workers.items():
                    for run_id in run_ids:
                        if self.storage.expire_task_run(task_id, run_id, checked_at):
                            self.seen_task_runs.setdefault(worker_id, set()).add((task_id, run_id))
                self.storage.commit()
                continue

            runs = {run.get("runId"): run for run in status_resp.get("status", {}).get("runs", [])}
            for worker_id, run_ids in workers.items():
                for run_id in run_ids:
                    run = runs.get(run_id)
                    if not run or run.get("workerId") != worker_id:
                        complete = False
                        continue
                    run_state = run.get("state")
                    if run_state not in ("completed", "failed", "exception"):
                        logger.debug("  %s: task %s run %s still running (state=%s)", worker_id, task_id, run_id, run_state)
                        continue
                    self.seen_task_runs.setdefault(worker_id, set()).add((task_id, run_id))
                    results.setdefault(worker_id, []).append(
                        (
                            task_id, run_id, run_state, run.get("started"), run.get("resolved"),
                            run.get("reasonResolved"), run.get("scheduled"), run.get("reasonCreated"),
                        ),
                    )
        return results, complete

    def _retry_unresolved_task_runs(self) -> Tuple[Dict[str, List[Tuple]], bool]:
        """Retry a bounded durable backlog, including rows from prior processes."""
        return self._resolve_observed_task_runs(
            self.storage.list_unresolved_task_runs(UNRESOLVED_TASK_RUN_BATCH_SIZE),
        )

    def _prepare_observed_task_run_batch(
        self, fetched_windows: List[Tuple[str, str, List[dict]]],
    ) -> Tuple[Dict[str, List[dict]], Dict[str, Optional[bool]], bool]:
        """Durably prepare observed runs for Queue status I/O on this thread.

        ``getWorker`` calls happen in worker-pool threads, but every storage
        operation here deliberately happens on the classify thread.  The
        returned references are grouped for one Queue request per task and
        retain their worker and worker-group routing information for the
        eventual result-application phase.
        """
        continuity_by_worker: Dict[str, Optional[bool]] = {}
        references_by_key: Dict[Tuple[str, str, Optional[int]], dict] = {}
        window_observed = False
        observed_at = datetime.now(timezone.utc).isoformat()

        for worker_id, worker_group, recent in fetched_windows:
            window = [
                (task.get("taskId"), task.get("runId"))
                for task in recent
                if task.get("taskId")
            ]
            previous_window = self.storage.get_recent_task_window(worker_id)
            if not window:
                self.storage.record_recent_task_window(worker_id, worker_group, window, observed_at)
                continue
            window_observed = True
            continuity_by_worker[worker_id], overlap_count = _recent_task_window_continuity(
                previous_window, window,
            )
            if continuity_by_worker[worker_id] is False:
                self.storage.record_task_run_coverage_event(
                    observed_at, "recent_tasks_no_overlap", worker_id, worker_group,
                    previous_window, window, overlap_count,
                )
            self.storage.record_recent_task_window(worker_id, worker_group, window, observed_at)
            seen = self.seen_task_runs.setdefault(worker_id, set())
            for task_id, run_id in window:
                if (task_id, run_id) in seen:
                    continue
                self.storage.record_observed_task_run(task_id, worker_id, run_id, observed_at)
                references_by_key[(task_id, worker_id, run_id)] = {
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "worker_group": worker_group,
                    "run_id": run_id,
                }

        # Newly observed rows are now visible to the restart-safe backlog
        # query.  Merge that bounded backlog before marking attempts so an
        # interrupted previous process receives the same durable treatment.
        for reference in self.storage.list_unresolved_task_runs(UNRESOLVED_TASK_RUN_BATCH_SIZE):
            key = (reference["task_id"], reference["worker_id"], reference["run_id"])
            references_by_key.setdefault(key, {
                "task_id": reference["task_id"],
                "worker_id": reference["worker_id"],
                "worker_group": None,
                "run_id": reference["run_id"],
            })

        checked_at = datetime.now(timezone.utc).isoformat()
        by_task: Dict[str, List[dict]] = {}
        for reference in references_by_key.values():
            self.storage.record_task_run_check(
                reference["task_id"], reference["run_id"], checked_at,
            )
            by_task.setdefault(reference["task_id"], []).append(reference)

        # A crash after this commit leaves each reference retryable, with its
        # last attempted timestamp intact.  Queue I/O must start only after it.
        self.storage.commit()
        return by_task, continuity_by_worker, window_observed

    def _fetch_prepared_task_statuses(
        self, references_by_task: Dict[str, List[dict]],
    ) -> Dict[str, Tuple[str, Optional[dict]]]:
        """Fetch one Queue status per prepared task without touching state.

        Results are tagged as ``ok``, ``expired`` (the Queue's definitive
        404), or ``error`` (a retryable transport/API failure).  Applying those
        results remains a classify-thread responsibility.
        """
        if not references_by_task:
            return {}

        def fetch(task_id: str) -> Tuple[str, Optional[dict]]:
            try:
                response = self._get_task_status(task_id)
            except Exception as exc:
                logger.warning("%s: status fetch error: %s", task_id, exc)
                return "error", None
            return ("expired", None) if response is None else ("ok", response)

        results: Dict[str, Tuple[str, Optional[dict]]] = {}
        with ThreadPoolExecutor(
            max_workers=min(TASK_STATUS_THREAD_COUNT, len(references_by_task)),
            thread_name_prefix="queue-status",
        ) as executor:
            futures = {
                executor.submit(fetch, task_id): task_id
                for task_id in references_by_task
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        return results

    def _apply_prepared_task_statuses(
        self,
        references_by_task: Dict[str, List[dict]],
        status_results: Dict[str, Tuple[str, Optional[dict]]],
    ) -> Tuple[Dict[str, List[Tuple]], bool]:
        """Apply Queue outcomes on the classify thread and return terminals."""
        terminal_by_worker: Dict[str, List[Tuple]] = {}
        complete = True
        applied_at = datetime.now(timezone.utc).isoformat()

        for task_id, references in references_by_task.items():
            outcome, status_response = status_results.get(task_id, ("error", None))
            if outcome == "error":
                complete = False
                continue
            if outcome == "expired":
                logger.info("task %s no longer has a status record; expiring observed references", task_id)
                for reference in references:
                    worker_id = reference["worker_id"]
                    run_id = reference["run_id"]
                    if self.storage.expire_task_run(task_id, run_id, applied_at):
                        self.seen_task_runs.setdefault(worker_id, set()).add((task_id, run_id))
                continue

            assert status_response is not None
            runs = {
                run.get("runId"): run
                for run in status_response.get("status", {}).get("runs", [])
            }
            for reference in references:
                worker_id = reference["worker_id"]
                run_id = reference["run_id"]
                run = runs.get(run_id)
                if not run or run.get("workerId") != worker_id:
                    complete = False
                    continue
                run_state = run.get("state")
                if run_state not in ("completed", "failed", "exception"):
                    logger.debug(
                        "  %s: task %s run %s still running (state=%s)",
                        worker_id, task_id, run_id, run_state,
                    )
                    continue
                self.seen_task_runs.setdefault(worker_id, set()).add((task_id, run_id))
                terminal_by_worker.setdefault(worker_id, []).append(
                    (
                        task_id, run_id, run_state, run.get("started"), run.get("resolved"),
                        run.get("reasonResolved"), run.get("scheduled"), run.get("reasonCreated"),
                    ),
                )

        # Expiry markers must survive before the later log/classification work.
        self.storage.commit()
        return terminal_by_worker, complete

    def _process_recent_task_window(
        self, worker_id: str, worker_group: str, recent: List[dict],
    ) -> Tuple[List[Tuple], bool, Optional[bool], bool]:
        """Persist and resolve one worker's already-fetched recent-task window."""
        seen = self.seen_task_runs.setdefault(worker_id, set())
        window = [(task.get("taskId"), task.get("runId")) for task in recent if task.get("taskId")]
        references = []
        observed_at = datetime.now(timezone.utc).isoformat()
        previous_window = self.storage.get_recent_task_window(worker_id)
        if window:
            continuity, overlap_count = _recent_task_window_continuity(previous_window, window)
            if continuity is False:
                self.storage.record_task_run_coverage_event(
                    observed_at, "recent_tasks_no_overlap", worker_id, worker_group,
                    previous_window, window, overlap_count,
                )
        else:
            continuity = None
        self.storage.record_recent_task_window(worker_id, worker_group, window, observed_at)
        for task in recent:
            task_id = task.get("taskId")
            run_id = task.get("runId")
            if task_id and (task_id, run_id) not in seen:
                self.storage.record_observed_task_run(task_id, worker_id, run_id, observed_at)
                references.append({"task_id": task_id, "worker_id": worker_id, "run_id": run_id})
        # getWorker references must be durable before any Queue status request.
        self.storage.commit()
        if references:
            logger.debug("  %s: checking %d recent task run(s)", worker_id, len(references))
        resolved, complete = self._resolve_observed_task_runs(references)
        return resolved.get(worker_id, []), complete, continuity, bool(window)

    def _new_terminal_tasks_with_continuity(
        self, worker_id: str, worker_group: str,
    ) -> Tuple[List[Tuple], bool, Optional[bool], bool]:
        """Fetch, persist, and resolve recent references for direct callers."""
        try:
            recent = self._get_recent_tasks(worker_group, worker_id)
        except Exception as exc:
            logger.warning("%s: failed to fetch recent tasks: %s", worker_id, exc)
            return [], False, False, True
        return self._process_recent_task_window(worker_id, worker_group, recent)

    def _new_terminal_tasks(self, worker_id: str, worker_group: str) -> Tuple[List[Tuple], bool]:
        """Return newly terminal task runs, retaining the legacy private API."""
        tasks, complete, _continuity, _window_observed = self._new_terminal_tasks_with_continuity(worker_id, worker_group)
        return tasks, complete

    def _poll_one_worker(self, worker: dict) -> Tuple[str, str, Optional[List[dict]], Optional[str]]:
        """Fetch only: the main thread owns storage transactions."""
        worker_id = worker["workerId"]
        worker_group = worker["workerGroup"]
        logger.debug(f"  polling {worker_id}")
        try:
            return worker_id, worker_group, self._get_recent_tasks(worker_group, worker_id), None
        except Exception as exc:
            logger.warning("%s: failed to fetch recent tasks: %s", worker_id, exc)
            return worker_id, worker_group, None, type(exc).__name__

    def _record_collection_coverage(
        self,
        source: str,
        success: bool,
        observed_at: Optional[datetime] = None,
    ) -> None:
        observed_at = observed_at or datetime.now(timezone.utc)
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        self.storage.record_collection_coverage(
            source,
            observed_at.astimezone(timezone.utc).isoformat(),
            success,
            None if source == "task_runs" else self.coverage_max_gap_seconds,
        )
        self.storage.commit()

    def _record_worker_availability(
        self,
        workers: List[dict],
        observed_at: Optional[datetime] = None,
    ) -> int:
        """Persist availability state and transitions for one worker-list observation."""
        observed_at = observed_at or datetime.now(timezone.utc)
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        observed_at = observed_at.astimezone(timezone.utc)
        observed_iso = observed_at.isoformat()
        previous_states = self.storage.get_worker_availability_states()
        observed_workers = {worker["workerId"]: worker for worker in workers}
        worker_ids = set(previous_states) | set(observed_workers)
        transition_count = 0

        for worker_id in sorted(worker_ids):
            worker = observed_workers.get(worker_id)
            previous = previous_states.get(worker_id)
            previous_last_contact = _parse_datetime(previous.get("last_contact")) if previous else None
            if worker is not None:
                worker_group = worker.get("workerGroup")
                observed_last_contact = _parse_datetime(worker.get("lastDateActive"))
                last_contact = max(
                    (value for value in (previous_last_contact, observed_last_contact) if value is not None),
                    default=None,
                )
                quarantine_until_raw = worker.get("quarantineUntil")
            else:
                worker_group = previous.get("worker_group")
                last_contact = previous_last_contact
                quarantine_until_raw = previous.get("quarantine_until")

            quarantine_until = _parse_datetime(quarantine_until_raw)
            quarantined = quarantine_until is not None and quarantine_until > observed_at
            if self.availability_mode == "listed":
                available = worker is not None and not quarantined
            else:
                available = (
                    last_contact is not None
                    and last_contact + self.worker_contact_threshold > observed_at
                    and not quarantined
                )

            previous_available = bool(previous["available"]) if previous else None
            previous_quarantined = bool(previous["quarantined"]) if previous else None
            reason = None
            if previous is None:
                if quarantined:
                    reason = "quarantine"
                elif available:
                    reason = "listed" if self.availability_mode == "listed" else "online"
                else:
                    reason = "not_listed" if self.availability_mode == "listed" else "contact_timeout"
            elif previous_quarantined != quarantined:
                reason = "quarantine" if quarantined else "unquarantine"
            elif previous_available != available:
                if self.availability_mode == "listed":
                    reason = "listed" if available else "not_listed"
                else:
                    reason = "return" if available else "contact_timeout"

            if reason in ("listed", "not_listed"):
                effective_at = observed_at
            elif reason in ("online", "return") and last_contact is not None:
                effective_at = last_contact
            elif reason == "contact_timeout" and last_contact is not None:
                effective_at = last_contact + self.worker_contact_threshold
            elif reason is not None:
                effective_at = observed_at
            else:
                effective_at = _parse_datetime(previous["effective_at"]) or observed_at

            last_contact_iso = last_contact.isoformat() if last_contact else None
            quarantine_until_iso = quarantine_until.isoformat() if quarantine_until else None
            state_reason = reason or previous["reason"]
            effective_iso = effective_at.isoformat()

            if reason is not None:
                self.storage.record_worker_availability_transition(
                    worker_id,
                    worker_group,
                    available,
                    quarantined,
                    last_contact_iso,
                    quarantine_until_iso,
                    reason,
                    effective_iso,
                    observed_iso,
                )
                transition_count += 1

            self.storage.upsert_worker_availability_state(
                worker_id,
                worker_group,
                available,
                quarantined,
                last_contact_iso,
                quarantine_until_iso,
                state_reason,
                effective_iso,
                observed_iso,
            )

        self.storage.commit()
        return transition_count

    def _process_results(self, worker_id: str, terminal_tasks: List[Tuple], bar=None, worker_group: str = None):
        for task in terminal_tasks:
            task_id, run_id, run_state, run_started, run_resolved, reason_resolved, *queue_fields = task
            run_scheduled, reason_created = (queue_fields + [None, None])[:2]
            if self._interrupted:
                logger.info(f"  {worker_id}: interrupted, skipping remaining tasks")
                break
            if bar:
                bar()

            classified_at = datetime.now(timezone.utc).isoformat()
            self._record_job_source(task_id, classified_at)

            if run_state == "completed":
                category = None
                logger.info(f"  {worker_id}: {self._color('1;32', 'completed')} task={task_id} run={run_id}")
            else:
                log_text = ""
                fetch_status = "empty"
                if run_id is not None:
                    log_url = f"{self.queue_base}/task/{task_id}/runs/{run_id}/artifacts/public/logs/live_backing.log"
                    logger.info(f"  {worker_id}: {run_state} task={task_id} run={run_id} — fetching log tail {log_url}")
                    log_text, fetch_status = self._fetch_log_tail(task_id, run_id)
                    if log_text:
                        logger.info(f"  {worker_id}: task={task_id} log tail fetched ({len(log_text)} bytes)")
                    else:
                        logger.info(f"  {worker_id}: task={task_id} no log available ({fetch_status})")
                if fetch_status == "too_large":
                    category = "log_too_large"
                else:
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
                run_resolved,
                classified_at,
                run_scheduled=run_scheduled,
                reason_created=reason_created,
            )
            self.storage.upsert_worker(worker_id, worker_group)

            if run_state == "completed":
                self.storage.increment_success(worker_id, run_started)
            else:
                self.storage.increment_failure(worker_id, run_started, category)

            self.storage.commit()

    # --- main loop ---

    def classify_cycle(
        self,
        workers: Optional[List[dict]] = None,
        availability_collection_success: Optional[bool] = True,
    ) -> dict:
        """One classify pass: poll all workers, process results, write reports. Returns summary dict."""
        with self.storage.classify_lock():
            if workers is None:
                now = time.time()
                if now - self._last_worker_refresh > WORKER_REFRESH_INTERVAL or not self._cached_workers:
                    try:
                        self._cached_workers = self._list_workers()
                    except Exception:
                        self._record_collection_coverage("worker_availability", False)
                        raise
                    availability_collection_success = True
                    self._last_worker_refresh = now
                    self._backfill_worker_groups(self._cached_workers)
                    logger.info(f"Worker list refreshed: {len(self._cached_workers)} workers")
                else:
                    availability_collection_success = None
                workers = self._cached_workers

            availability_transitions = 0
            if availability_collection_success is not False:
                availability_transitions = self._record_worker_availability(workers)
            if availability_collection_success is not None:
                self._record_collection_coverage("worker_availability", availability_collection_success)

            total_workers = len(workers)
            logger.info(f"Scanning {total_workers} workers...")
            poll_results = []
            scanned = 0
            task_coverage_observed = False
            task_coverage_continuous = True
            task_window_fetch_failed = False
            fetched_windows = []
            thread_pool = ThreadPool(WORKER_THREAD_COUNT)
            terminated = False
            try:
                with alive_bar(total_workers, title="scanning workers", enrich_print=False) as bar:
                    for worker_id, worker_group, recent, error_type in thread_pool.imap_unordered(
                        self._poll_one_worker,
                        workers,
                    ):
                        scanned += 1
                        bar()
                        if recent is None:
                            # Keep coverage pending after a transient getWorker
                            # failure.  A later overlapping window bridges the
                            # missing poll; a later non-overlap proves a gap.
                            task_window_fetch_failed = True
                            self.storage.record_task_run_coverage_event(
                                datetime.now(timezone.utc).isoformat(), "get_worker_error", worker_id, worker_group,
                                self.storage.get_recent_task_window(worker_id), None, None, error_type,
                            )
                        else:
                            fetched_windows.append((worker_id, worker_group, recent))
                        if self._interrupted:
                            thread_pool.terminate()
                            terminated = True
                            break
            except Exception as e:
                logger.warning(f"Poll error: {e}")
                task_coverage_observed = True
                task_coverage_continuous = False
                thread_pool.terminate()
                terminated = True
            finally:
                if not terminated:
                    thread_pool.close()
                thread_pool.join()

            # getWorker requests are parallel, but all storage state changes
            # happen here on the classify thread.  PostgresStorage deliberately
            # owns one transaction connection per classifier instance.
            prepared_by_task, continuity_by_worker, window_observed = self._prepare_observed_task_run_batch(
                fetched_windows,
            )
            if window_observed:
                task_coverage_observed = True
                task_coverage_continuous = task_coverage_continuous and all(
                    continuity is not False for continuity in continuity_by_worker.values()
                )

            status_results = self._fetch_prepared_task_statuses(prepared_by_task)
            resolved, _complete = self._apply_prepared_task_statuses(
                prepared_by_task, status_results,
            )
            prepared_references = [
                reference for references in prepared_by_task.values() for reference in references
            ]
            worker_groups = {
                reference["worker_id"]: reference["worker_group"]
                for reference in prepared_references
                if reference["worker_group"] is not None
            }
            poll_results.extend(
                (worker_id, worker_groups.get(worker_id), terminal_tasks)
                for worker_id, terminal_tasks in resolved.items()
            )

            polls_complete = scanned == total_workers and not terminated and not self._interrupted
            # A fetch failure is recoverable only when no fetched worker has
            # already proved a discontinuity in this pass.
            if task_coverage_observed and (not task_window_fetch_failed or not task_coverage_continuous):
                self._record_collection_coverage(
                    "task_runs",
                    task_coverage_continuous and polls_complete,
                )

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
            scan_summary = (
                f"{scanned}/{total_workers} workers" if scanned < total_workers else f"{total_workers} workers"
            )
            alert_str = self._color("1;31" if alerting_count > 0 else "1;32", str(alerting_count))
            logger.info(
                f"Scan done: {scan_summary} scanned, {new_total} new terminal tasks, "
                f"{availability_transitions} availability transitions, "
                f"{alert_str} workers with ≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures.",
            )
            return {
                "scanned": scanned,
                "total_workers": total_workers,
                "new_terminal": new_total,
                "alerting": alerting_count,
                "availability_transitions": availability_transitions,
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
            availability_collection_success = None
            if now - last_worker_refresh > WORKER_REFRESH_INTERVAL or not workers:
                try:
                    workers = self._list_workers()
                    last_worker_refresh = now
                    availability_collection_success = True
                    logger.info(f"Worker list: {len(workers)} workers in pool")
                    self._backfill_worker_groups(workers)
                except Exception as e:
                    logger.warning(f"Failed to refresh worker list: {e}")
                    availability_collection_success = False

            summary = self.classify_cycle(
                workers,
                availability_collection_success=availability_collection_success,
            )
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
            log_text, _ = self._fetch_log_tail(task_id, run_id)
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

    def _top_offenders_by_category(self, since: str, n: int = 5) -> Dict[str, List[Tuple[str, int]]]:
        return self.storage.top_offenders_by_category(since, n=n)

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
        severity_map = {
            "critical": categories_by_severity("critical"),
            "high": categories_by_severity("high"),
            "low": categories_by_severity("low"),
        }
        return self.storage.query_heatmap(since, severity_map=severity_map)

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
        """Return enriched quarantine data, fetching details only for changed/new entries."""
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
                    resp = self.tc_worker_manager.getWorker(self.provisioner, self.worker_type, wg, wid)
                    details = resp.get("quarantineDetails", [])
                    if details:
                        latest = details[-1]
                        return wid, {
                            "quarantine_until": until,
                            "reason": latest.get("quarantineInfo", ""),
                            "set_at": latest.get("updatedAt", ""),
                            "client_id": latest.get("clientId", ""),
                        }
                except taskcluster.exceptions.TaskclusterRestFailure as e:
                    if e.status_code != 404:
                        logger.warning(f"  {wid}: failed to fetch quarantine details: {e}")
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

    def render_html(
        self,
        os_label: str = "",
        navigation_html: Optional[str] = None,
        navigation_styles: str = "",
    ) -> str:
        """Return the HTML dashboard string for this pool (does not write to disk).

        The standalone report keeps its own fallback header. Web callers can
        provide the shared Jinja navigation so every interactive page has the
        same menu markup and CSS.
        """
        now = datetime.now(timezone.utc)
        since_1d = (now - timedelta(days=1)).isoformat()
        since_12h = (now - timedelta(hours=12)).isoformat()
        workers = self._query_workers()
        quarantine_details = self.storage.get_current_quarantine_details()
        quarantined = {
            worker_id: details["quarantine_until"]
            for worker_id, details in quarantine_details.items()
        }
        windowed_sr = self._query_windowed_sr()
        heatmap = self._query_heatmap(since_12h)
        return self._write_html(
            workers,
            quarantined,
            windowed_sr,
            since_1d,
            heatmap,
            quarantine_details,
            os_label=os_label,
            navigation_html=navigation_html,
            navigation_styles=navigation_styles,
        )

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
        self._cached_quarantined = quarantined
        self._cached_quarantine_details = quarantine_details
        self._last_quarantine_refresh = time.time()
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
        top_offenders = self._top_offenders_by_category(since_1d) if category_totals else {}

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

        if self.availability_mode == "listed":
            lines += [
                "**Availability mode: listed.** Utilization treats every Taskcluster-listed, "
                "non-quarantined worker as eligible capacity. Listing does not confirm that a "
                "dormant or physically unhealthy device is live.",
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
            lines += ["## Consecutive Failures", ""]
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
            lines += ["## Top Offenders", "", "Workers with the most failures in the last day, grouped by category.", ""]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                lines.append(f"### {cat} ({count} total all-time)")
                lines.append("")
                for wid, n in top_offenders.get(cat, []):
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
        os_label: str = "",
        navigation_html: Optional[str] = None,
        navigation_styles: str = "",
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
        top_offenders = self._top_offenders_by_category(since_1d) if category_totals else {}

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

        detail_navigation = [navigation_html] if navigation_html else [
            '<header class="site-header">',
            '<a href="/" style="text-decoration:none"><pre style="color:#0ff;line-height:1;margin:0;font-size:1rem">',
            " ⣀⡀ ⢀⡀ ⢀⡀ ⡇   ⢀⣀ ⡇ ⢀⣀ ⢀⣀ ⢀⣀ ⠄ ⣰⡁ ⠄ ⢀⡀ ⡀⣀",
            " ⡧⠜ ⠣⠜ ⠣⠜ ⠣   ⠣⠤ ⠣ ⠣⠼ ⠭⠕ ⠭⠕ ⠇ ⢸  ⠇ ⠣⠭ ⠏ ",
            "</pre></a>",
            f'<span class="site-title"><a href="https://firefox-ci-tc.services.mozilla.com/provisioners/{self.provisioner}/worker-types/{self.worker_type}?sortBy=Last%20Active&sortDirection=desc" target="_blank">{self.provisioner}/{self.worker_type}</a></span>',
            '<details class="global-menu">',
            '  <summary aria-label="Open navigation" title="Navigation"><span class="menu-icon" aria-hidden="true"></span></summary>',
            '  <nav class="menu-popover" aria-label="Global navigation"><a href="/">Overview</a><a href="/patterns">Patterns</a><a href="/pool-discovery">Pool Discovery</a><a href="/api">API</a><a href="/about">About</a></nav>',
            "</details>",
            "</header>",
        ]

        parts = [
            "<!DOCTYPE html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<link rel="icon" href="/static/favicon.svg" type="image/svg+xml">',
            f"<title>Pool Classifier: {self.provisioner}/{self.worker_type}</title>",
            "<style>",
            "  body { font-family: monospace; background: #111; color: #ccc; padding: 1.5rem; }",
            "  h1 { color: #fff; }",
            "  h2 { color: #f90; margin-top: 2rem; }",
            "  p.gen { color: #666; font-size: .85em; margin-bottom: .5rem; }",
            "  .pool-summary-metrics { display:flex; flex-wrap:wrap; gap:.4rem 1.5rem; margin:.75rem 0 0; color:#aaa; font-size:.85em; } .pool-summary-metrics dt, .pool-summary-metrics dd { display:inline; margin:0; } .pool-summary-metrics dt { color:#666; } .pool-summary-metrics dd { color:#ccc; font-weight:bold; }",
            "  .footer { margin: 2rem 0 1rem; color: #555; font-size: .8em; text-align: center; }",
            "  .dashboard-toolbar { display:flex; align-items:center; gap:.75rem 1.5rem; flex-wrap:wrap; margin:1rem 0; }",
            "  .tz-toggle { display:flex; align-items:center; gap:.25rem; flex-wrap:wrap; margin:0 0 0 auto; font-size:.9em; }",
            "  .tz-toggle .label, .tz-toggle .interval { color:#666; } .tz-toggle .label { margin-right:.1rem; } .tz-toggle .interval { margin-left:.1rem; }",
            "  .tz-toggle button { padding:0; border:0; background:transparent; color:#777; cursor:pointer; font:inherit; }",
            "  .tz-toggle button:hover, .tz-toggle button.active { color:#f90; } .tz-toggle .separator { margin:0 .7rem; color:#444; user-select:none; }",
            "  .page-nav { display:flex; align-items:center; gap:0; flex-wrap:wrap; font-size:.9em; }",
            "  .page-nav a { color:#58a6ff; padding:.2rem .45rem; border-radius:3px; }",
            "  .page-nav a:first-child { padding-left:0; }",
            "  .page-nav a:hover { color:#a0c8ff; background:#2a2a2a; text-decoration:none; }",
            "  .page-nav a:visited { color: #58a6ff; }",
            "  .page-nav span.sep { color: #444; user-select: none; }",
            "  @media (max-width:54rem) { .tz-toggle { margin-left:0; } }",
            "  .site-header { display:flex; align-items:last baseline; gap:1.5rem; margin:0 0 1.5rem; }",
            "  .site-title { color:#ccc; font-size:1.1rem; letter-spacing:.02em; }",
            "  .global-menu { position:relative; margin-left:auto; }",
            "  .global-menu summary { display:flex; align-items:center; justify-content:center; width:2.25rem; height:2.25rem; box-sizing:border-box; color:#aaa; border:1px solid #333; border-radius:4px; cursor:pointer; list-style:none; }",
            "  .global-menu summary::-webkit-details-marker { display:none; }",
            "  .global-menu summary:hover, .global-menu[open] summary { color:#0ff; border-color:#0aa; background:#1a1a1a; }",
            "  .menu-icon, .menu-icon::before, .menu-icon::after { display:block; width:1rem; height:2px; background:currentColor; }",
            "  .menu-icon { position:relative; } .menu-icon::before, .menu-icon::after { content:''; position:absolute; left:0; } .menu-icon::before { top:-5px; } .menu-icon::after { top:5px; }",
            "  .menu-popover { position:absolute; z-index:1; top:calc(100% + .4rem); right:0; min-width:10rem; padding:.35rem; background:#1a1a1a; border:1px solid #333; border-radius:4px; box-shadow:0 .5rem 1.5rem #0008; }",
            "  .menu-popover a, .menu-popover .current { display:block; padding:.45rem .6rem; border-radius:3px; }",
            "  .menu-popover a { color:#58a6ff; } .menu-popover a:hover { color:#a0c8ff; background:#2a2a2a; text-decoration:none; } .menu-popover .current { color:#666; cursor:default; }",
            "  table { border-collapse: collapse; width: 100%; margin: 0; }",
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
            "  #hm-tip .tip-critical { color: #f44; }",
            "  #hm-tip .tip-high { color: #f90; }",
            "  #hm-tip .tip-low { color: #88a; }",
            "  #hm-tip .tip-dim { color: #888; }",
            "  .ok { color: #4c4; }",
            "  .bad { color: #f44; }",
            "  .warn { color: #f90; }",
            "  ul { margin:0; padding-left:1.5rem; }",
            "  li.bad { color: #f44; margin-bottom: .3rem; }",
            "  .quarantine { color: #f90; font-size: .85em; margin-left: .4em; }",
            "  .reason-trunc { display: inline-block; max-width: 22rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: bottom; cursor: default; }",
            "  h3.cat-header { color: #ccc; font-size: .95em; margin: 1rem 0 .2rem; }",
            "  .cat-total { color: #666; font-weight: normal; }",
            "  ul.offenders { margin:0 0 0 1.2rem; padding:0; list-style:none; font-size:.85em; color:#aaa; }",
            "  ul.offenders li { padding: .1rem 0; }",
            "  a { color: inherit; text-decoration: none; }",
            "  a:visited { color: inherit; }",
            "  a:hover { text-decoration: underline; }",
            "  .hm-wrap { display:grid; grid-template-columns:1fr 1fr; gap:1rem 2rem; margin:0; }",
            "  .hm-block { overflow-x: auto; }",
            "  .hm-grid { border-collapse: collapse; width: auto; margin-bottom: 0; }",
            "  .hm-grid th { background: #1e1e1e; color: #666; padding: .25rem .4rem; font-size: .75em; text-align: center; cursor: default; user-select: none; border: none; }",
            "  .hm-grid th.hm-worker-hdr { text-align: left; color: #aaa; }",
            "  .hm-grid td.hm-worker { padding: .15rem .6rem .15rem 0; font-size: .82em; white-space: nowrap; border: none; }",
            "  .hm-cell { width: 2.2rem; min-width: 2.2rem; height: 1.5rem; padding: 0 !important; border: 2px solid #111 !important; border-radius: 3px; cursor: default; }",
            "  .hm-empty { background: #1c1c1c; }",
            "  .hm-ok { background: #1a4a20; }",
            "  .hm-sev-critical { background: #7a1515; }",
            "  .hm-sev-high { background: #7a4400; }",
            "  .hm-sev-low { background: #2a2a4a; }",
            "  .hm-legend { display: flex; gap: 1.5rem; font-size: .8em; color: #aaa; margin: .5rem 0 1.2rem; align-items: center; flex-wrap: wrap; }",
            "  .hm-swatch { display: inline-block; width: .9rem; height: .9rem; margin-right: .35rem; vertical-align: middle; border-radius: 2px; border: 1px solid #333; }",
            "  .hm-copy { cursor: pointer; color: #555; margin-right: .35rem; vertical-align: middle; display: inline-block; line-height: 1; }",
            "  .hm-copy:hover { color: #bbb; }",
            "  .hm-copy.copied { color: #4c4; }",
            "  .hm-copy svg { width: .7rem; height: .7rem; }",
            "  .summary-grid { display: grid; grid-template-columns: max-content 1fr; gap: 0 3rem; }",
            "  .summary-grid > div { min-width: 0; }",
            "  .offenders-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: .25rem 2rem; }",
            "  .availability-note { max-width:80rem; margin:.75rem 0 0; color:#777; font-size:.85em; line-height:1.45; }",
            "  .footnote-ref, .footnote-ref:visited, .footnote-marker { color:#f90; } .footnote-ref { font-size:.75em; margin-left:.1em; vertical-align:super; }",
            "  .util-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:.8rem; max-width:80rem; }",
            "  .util-card { border:1px solid #444; border-radius:4px; background:#1a1a1a; padding:.8rem; min-height:7rem; }",
            "  .util-card.complete { border-color:#554070; box-shadow:inset 3px 0 #4c1d95; } .util-card.partial { border-color:#8a5a16; box-shadow:inset 3px 0 #d48718; } .util-card.incomplete { color:#999; } .util-card.incomplete h3 { color:#aaa; }",
            "  .util-card h3 { margin:0 0 .5rem; color:#ddd; } .util-card p { margin:.25rem 0; } .util-detail { color:#aaa; font-size:.85em; }",
            "  .util-timeline-controls { display:flex; align-items:center; gap:.45rem; margin:1.2rem 0 .45rem; font-size:.85em; color:#aaa; }",
            "  .util-timeline-controls button { border:0; background:transparent; color:#777; cursor:pointer; font:inherit; padding:0; } .util-timeline-controls button:hover, .util-timeline-controls button.active { color:#f90; }",
            "  .util-timeline-wrap { max-width:80rem; overflow-x:auto; } .util-timeline { display:grid; gap:3px; min-width:36rem; }",
            "  .util-hour { height:2.1rem; border:0; border-radius:2px; padding:0; cursor:default; outline:1px solid #111; } .util-hour:hover { filter:brightness(1.15); }",
            "  .util-hour-usage { background:#16111d; }",
            "  .util-hour-incomplete { background:repeating-linear-gradient(135deg,#303030 0,#303030 4px,#202020 4px,#202020 8px); } .util-hour-partial { outline:2px dashed #d48718; outline-offset:-2px; } .util-hour-unavailable { background:#6b1d1d; } .util-hour-error { background:#555; }",
            "  .util-timeline-legend { display:flex; gap:1rem; flex-wrap:wrap; margin:.45rem 0 1.2rem; color:#888; font-size:.8em; }",
            "  .lag-chart-wrap { width:100%; max-width:80rem; overflow-x:auto; border:1px solid #333; background:#161616; padding:.5rem; box-sizing:border-box; } .lag-chart { width:100%; min-width:42rem; height:12rem; display:block; } .lag-chart .lag-linked-hover { stroke:#fff; stroke-width:3; filter:drop-shadow(0 0 3px #fff); } .lag-chart rect.lag-linked-hover { fill:#ddd; } .lag-compact { color:#888; font-size:.85em; margin:.5rem 0; }",
            "  .lag-legend { display:flex; gap:1rem; flex-wrap:wrap; margin:.45rem 0; color:#888; font-size:.8em; } .lag-line { display:inline-block; width:1.5rem; border-top:2px solid; vertical-align:middle; margin-right:.3rem; }",
            "  .source-summary { display:flex; flex-wrap:wrap; align-items:baseline; gap:.35rem; } .source-controls { display:inline-flex; gap:.15rem; } .source-controls button { border:0; background:transparent; color:#777; cursor:pointer; font:inherit; padding:0 .25rem; } .source-controls button.active { color:#f90; } .source-chart { display:grid; grid-template-columns:3rem minmax(0,1fr); height:12rem; max-width:80rem; padding:.5rem; border:1px solid #333; background:#161616; } .source-axis, .source-plot { position:relative; min-height:0; } .source-axis-label { position:absolute; right:.45rem; color:#999; font-size:.75em; font-variant-numeric:tabular-nums; transform:translateY(50%); } .source-grid { position:absolute; inset:0; pointer-events:none; } .source-gridline { position:absolute; right:0; left:0; border-top:1px solid #303030; } .source-bars { display:flex; align-items:end; gap:.35rem; height:100%; } .source-day { flex:1; height:100%; display:flex; flex-direction:column-reverse; min-width:1rem; position:relative; z-index:1; } .source-segment { min-height:1px; cursor:default; outline:1px solid transparent; outline-offset:-1px; transition:filter .12s, outline-color .12s, box-shadow .12s; } .source-segment:hover, .source-segment:focus-visible { position:relative; z-index:1; outline:2px solid #fff; filter:brightness(1.25); box-shadow:inset 0 0 0 1px #161616, 0 0 4px #fff; } .source-tooltip { display:none; position:fixed; z-index:10; width:min(25rem,calc(100vw - 1rem)); max-height:calc(100vh - 1rem); overflow-y:auto; box-sizing:border-box; padding:.7rem .8rem; border:1px solid #777; border-radius:5px; background:#101216; color:#f3f4f6; box-shadow:0 8px 24px #000c; font-size:.84em; line-height:1.4; pointer-events:none; } .source-tooltip.visible { display:block; } .source-tooltip-heading { margin:0; padding-bottom:.4rem; border-bottom:1px solid #3d424a; color:#fff; font-weight:700; } .source-tooltip-current { margin:.45rem 0; color:#fff; } .source-tooltip-rows { display:grid; gap:.2rem; } .source-tooltip-row { display:grid; grid-template-columns:minmax(0,1fr) auto; align-items:baseline; column-gap:1rem; padding:.18rem .25rem; border-radius:2px; } .source-tooltip-row span:first-child { overflow-wrap:anywhere; color:#d9dde3; } .source-tooltip-row span:last-child { color:#c8ced8; font-variant-numeric:tabular-nums; white-space:nowrap; } .source-tooltip-row.active { background:#2a3545; box-shadow:inset 3px 0 #fff; font-weight:700; } .source-tooltip-row.active span { color:#fff; }",
            "  .lag-heatmap-wrap { overflow-x:auto; max-width:80rem; } .lag-heatmap { display:grid; grid-template-columns:1.75rem repeat(24, minmax(1.35rem, 1fr)); gap:3px; min-width:42rem; margin-top:.6rem; }",
            "  .lag-hm-label { color:#777; font-size:.72em; text-align:center; } .lag-hm-day { text-align:left; padding-right:.2rem; align-self:center; } .lag-hm-cell { height:1rem; border:0; border-radius:2px; cursor:default; outline:1px solid #111; } .lag-hm-cell.insufficient { background:repeating-linear-gradient(135deg,#333 0,#333 4px,#222 4px,#222 8px) !important; } .lag-hm-cell:hover { filter:brightness(1.25); outline-color:#aaa; } .lag-hm-cell.lag-linked-hover { box-shadow:inset 0 0 0 2px #fff; filter:brightness(1.35); position:relative; z-index:1; }",
            navigation_styles,
            "</style>",
            "</head>",
            "<body>",
            *detail_navigation,
        ]

        summary_url = f"/api/v1/pools/{self.provisioner}/{self.worker_type}/utilization/summary"
        timeline_url = f"/api/v1/pools/{self.provisioner}/{self.worker_type}/utilization"
        coverage_breaks_url = f"/api/v1/pools/{self.provisioner}/{self.worker_type}/coverage-breaks"
        start_lag_visualization_url = f"/api/v1/pools/{self.provisioner}/{self.worker_type}/observed-start-lag/visualization"
        job_sources_url = f"/api/v1/pools/{self.provisioner}/{self.worker_type}/job-sources"
        guide_url = f"/pools/{self.provisioner}/{self.worker_type}/utilization-api-guide"

        pool_summary = []
        if workers:
            total_tasks = total_failures + total_successes
            sr_pct = f"{100 * total_successes / total_tasks:.1f}%" if total_tasks else "—"
            window_str = f"Last {_humanize(oldest_ts).removesuffix(' ago')}" if oldest_ts else "Observed period"
            worker_os = {"android": "Android", "macos": "macOS", "windows": "Windows", "linux": "Linux"}.get(os_label, os_label)
            worker_label = f"{worker_os} workers" if worker_os else "workers"
            pool_summary.append(
                '<section class="pool-summary" aria-labelledby="summary-heading">'
                '<h2 id="summary-heading">Summary</h2>'
                '<dl class="pool-summary-metrics">'
                f'<div><dt>Period:</dt> <dd>{window_str}</dd></div>'
                f'<div><dt>Completed:</dt> <dd>{total_tasks:,}</dd></div>'
                f'<div><dt>Success rate:</dt> <dd>{sr_pct}</dd></div>'
                f'<div><dt>Observed workers:</dt> <dd>{len(workers):,} {worker_label}</dd></div>'
                '</dl>'
                '</section>',
            )

        parts += [
            '<div class="dashboard-toolbar">',
            '<nav class="page-nav">',
            '  <a href="#summary-heading">Summary</a><span class="sep">|</span>',
            '  <a href="#s-job-sources">Task Source</a><span class="sep">|</span>',
            '  <a href="#s-start-lag">Start Lag</a><span class="sep">|</span>',
            '  <a href="#s-utilization">Utilization</a><span class="sep">|</span>',
            '  <a href="#s-attention">Consecutive Failures</a><span class="sep">|</span>',
            '  <a href="#s-quarantined">Quarantined</a><span class="sep">|</span>',
            '  <a href="#s-categories">Failure Categories</a><span class="sep">|</span>',
            '  <a href="#s-heatmap">Worker Activity</a><span class="sep">|</span>',
            '  <a href="#s-offenders">Top Offenders</a><span class="sep">|</span>',
            '  <a href="#s-all">All Workers</a>',
            "</nav>",
            '<div class="tz-toggle" aria-label="Display controls">',
            '  <span class="label">Time:</span>',
            '  <button type="button" class="active" data-timezone="local" aria-pressed="true">[Local]</button>',
            '  <button type="button" data-timezone="utc" aria-pressed="false">[UTC]</button>',
            '  <span class="separator" aria-hidden="true">|</span>',
            '  <span class="label">Refresh:</span>',
            '  <button type="button" data-autorefresh="on" aria-pressed="true">[On]</button>',
            '  <button type="button" data-autorefresh="off" aria-pressed="false">[Off]</button>',
            '  <span class="interval">5m</span>',
            "</div>",
            "</div>",
        ]
        parts += pool_summary

        capacity_note_ref = (
            '<a class="footnote-ref" href="#utilization-capacity-note" aria-label="See capacity method note">*</a>'
            if self.availability_mode == "listed"
            else ""
        )
        parts += [
            '<h2 id="s-job-sources">Task Source</h2>',
            '<p class="gen">Terminal task runs grouped by the Taskcluster project tag or an explicit reviewed source mapping.</p>',
            '<p class="source-summary gen"><span id="source-freshness">Loading job sources…</span><span aria-hidden="true">·</span><span class="source-controls" role="group" aria-label="Task Source range"><button type="button" class="active" data-source-days="7">[7d]</button><button type="button" data-source-days="14">[14d]</button></span></p><div id="source-chart" class="source-chart" role="group" aria-label="Daily job volume by source"></div><div id="source-tooltip" class="source-tooltip" role="tooltip" aria-hidden="true"></div>',
            '<h2 id="s-start-lag">Start Lag</h2>',
            '<p class="gen">Observed scheduled-to-start time for terminal task runs. This excludes jobs that never started, so it is not a queue total, drop rate, or pool-health verdict.</p>',
            '<p id="lag-freshness" class="gen">Loading observed start lag…</p>',
            '<div class="lag-legend"><span><span class="lag-line" style="border-color:#5dd"></span>p50</span><span><span class="lag-line" style="border-color:#f90"></span>p95</span><span><span class="lag-line" style="border-color:#f44;border-top-style:dashed"></span>SLO</span><span>bars: sample count</span></div>',
            '<div id="lag-chart-wrap" class="lag-chart-wrap"><svg id="lag-chart" class="lag-chart" viewBox="0 0 960 240" role="img" aria-label="Hourly observed start lag p50 and p95 trend"></svg></div>',
            '<p class="gen">UTC weekday/hour p95. Striped cells have fewer than five observations.</p>',
            '<div id="lag-heatmap-wrap" class="lag-heatmap-wrap"><div id="lag-heatmap" class="lag-heatmap"></div></div>',
            '<h2 id="s-utilization">Utilization</h2>',
            f'<p class="gen">Duration-weighted task time versus available worker capacity{capacity_note_ref}. <a href="{guide_url}">API guide</a></p>',
            '<p id="util-freshness" class="gen">Loading utilization…</p>',
            '<div id="util-cards" class="util-grid"></div>',
            '<div class="util-timeline-controls"><span>Hourly timeline:</span><button type="button" data-hours="24">[24h]</button><button type="button" data-hours="48">[48h]</button></div>',
            '<div class="util-timeline-legend"><span><span class="hm-swatch" style="width:3rem; background:linear-gradient(90deg,#16111d,#4c1d95)"></span>purple: 0% to 100% utilization</span><span><span class="hm-swatch" style="outline:2px dashed #d48718; outline-offset:-2px"></span>orange outline: partial coverage</span><span><span class="hm-swatch" style="background:repeating-linear-gradient(135deg,#303030 0,#303030 4px,#202020 4px,#202020 8px)"></span>striped: no data</span><span><span class="hm-swatch" style="background:#6b1d1d"></span>dark red: no available capacity</span></div>',
            '<div class="util-timeline-wrap"><div id="util-timeline" class="util-timeline"></div></div>',
            *(
                ['<p id="utilization-capacity-note" class="availability-note"><span class="footnote-marker" aria-hidden="true">*</span> <strong>Availability mode: listed.</strong> Rather than requiring a worker to have contacted Taskcluster recently, utilization treats every non-quarantined worker still listed by Taskcluster as eligible capacity. The Taskcluster listing does not confirm that the device is live or ready.</p>']
                if self.availability_mode == "listed"
                else []
            ),
        ]

        if alerting:
            parts.append('<div class="summary-grid">')

        if alerting:
            parts += ["<div>", '<h2 id="s-attention">Consecutive Failures</h2>', "<ul>"]
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

        if alerting:
            parts.append("</div>")

        if quarantine_details:
            quarantine_count = len(quarantine_details)
            quarantine_label = "worker" if quarantine_count == 1 else "workers"
            snapshot_at = max(
                (details.get("observed_at") for details in quarantine_details.values() if details.get("observed_at")),
                default=None,
            )
            snapshot_note = f" Snapshot: {fmt(snapshot_at)}." if snapshot_at else ""
            parts += [
                '<h2 id="s-quarantined">Quarantined Workers</h2>',
                f'<p class="gen">{quarantine_count} {quarantine_label} currently quarantined.{snapshot_note}</p>',
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

        if category_totals:
            parts += ["<div>", '<h2 id="s-categories">Failure Categories</h2>', "<ul>"]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                parts.append(f"  <li>{cat}: <strong>{count}</strong></li>")
            parts += ["</ul>", "</div>"]

        if heatmap:
            hour_period = ["< 1h ago"] + [f"{i}–{i + 1}h ago" for i in range(1, 12)]

            def hm_cell(data: Optional[dict], h: int) -> str:
                period = hour_period[h]
                if not data:
                    info = json.dumps({"period": period, "ok": 0, "critical": 0, "high": 0, "low": 0, "cats": {}})
                    return f"<td class=\"hm-cell hm-empty\" data-info='{info}'></td>"
                s, critical, high, low = data["s"], data["critical"], data["high"], data["low"]
                if critical:
                    cls = "hm-sev-critical"
                elif high:
                    cls = "hm-sev-high"
                elif low:
                    cls = "hm-sev-low"
                else:
                    cls = "hm-ok"
                info = json.dumps(
                    {
                        "period": period,
                        "ok": s,
                        "critical": critical,
                        "high": high,
                        "low": low,
                        "cats": data.get("cats", {}),
                    },
                )
                return f"<td class=\"hm-cell {cls}\" data-info='{info}'></td>"

            # sort workers: highest-severity failures first, then alpha
            def hm_sort_key(wid):
                hours = heatmap[wid]
                bad = sum(h["critical"] * 2 + h["high"] for h in hours.values())
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
                    f'<div class="hm-block"><table class="hm-grid not-sortable">'
                    f'<thead><tr><th class="hm-worker-hdr">Worker</th>{hm_header}</tr></thead>'
                    f"<tbody>{rows}</tbody></table></div>"
                )

            parts += [
                '<h2 id="s-heatmap">Worker Activity</h2>',
                '<p class="gen">Only hosts with activity in the last 12 hours are shown. Workers are ordered by recent failure severity (critical counts twice), then hostname.</p>',
                '<div class="hm-legend">',
                '  <span><span class="hm-swatch" style="background:#1a4a20"></span>success</span>',
                '  <span><span class="hm-swatch" style="background:#7a1515"></span>critical</span>',
                '  <span><span class="hm-swatch" style="background:#7a4400"></span>high</span>',
                '  <span><span class="hm-swatch" style="background:#2a2a4a"></span>low</span>',
                '  <span><span class="hm-swatch" style="background:#1c1c1c; border-color:#444"></span>no activity</span>',
                "</div>",
                '<div class="hm-wrap">',
                hm_table(halves[0]),
                hm_table(halves[1]),
                "</div>",
            ]

        if category_totals:
            parts += [
                "<h2 id=\"s-offenders\">Top Offenders</h2>",
                '<p class="gen">Workers with the most failures in the last day, grouped by category.</p>',
                '<div class="offenders-grid">',
            ]
            for cat, count in sorted(category_totals.items(), key=lambda x: -x[1]):
                offenders = top_offenders.get(cat, [])
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

        total_w = len(workers)
        quarantined_w = len(quarantined or {})
        parts += [
            '<h2 id="s-all">All Workers</h2>',
            f'<p class="gen">{total_w} tracked workers &middot; {quarantined_w} currently quarantined. '
            'Tracked workers have recorded task history; this is not a liveness or readiness check.</p>',
            "<table>",
            "  <thead><tr>",
            "    <th>Worker</th><th>SR (1d)</th><th>SR (3d)</th><th>SR (7d)</th><th>SR (all)</th>"
            "<th>Successes</th><th>Failures</th><th>Top Category</th><th>Consec Fails</th><th>Last Active</th>",
            "  </tr></thead>",
            "  <tbody>",
        ]

        for wid, w in sorted(workers.items(), key=lambda item: _natural_sort_key(item[0])):
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

        hm_sev_map = {cat: sev for sev in ("critical", "high", "low") for cat in categories_by_severity(sev)}
        parts += [
            '<div id="hm-tip"></div>',
            "<script>",
            f"  const HM_SEV = {json.dumps(hm_sev_map)};",
            "  const SEV_ORDER = {critical: 0, high: 1, low: 2};",
            "  const SEV_CLASS = {critical: 'tip-critical', high: 'tip-high', low: 'tip-low'};",
            "  const SEV_ICON  = {critical: '✗', high: '⚠', low: '•'};",
            "  // Heatmap hover card",
            f"  const UTIL_SUMMARY_URL = {json.dumps(summary_url)};",
            f"  const UTIL_TIMELINE_URL = {json.dumps(timeline_url)};",
            f"  const COVERAGE_BREAKS_URL = {json.dumps(coverage_breaks_url)};",
            f"  const START_LAG_VISUALIZATION_URL = {json.dumps(start_lag_visualization_url)};",
            f"  const JOB_SOURCES_URL = {json.dumps(job_sources_url)};",
            "  const utilCards = document.getElementById('util-cards');",
            "  const utilFreshness = document.getElementById('util-freshness');",
            "  const utilTimeline = document.getElementById('util-timeline');",
            "  const utilTimelineButtons = [...document.querySelectorAll('.util-timeline-controls button')];",
            "  const UTIL_TIMELINE_KEY = 'pc-utilization-timeline-hours'; let utilTimelineHours = 24; let utilDataThrough = null; let timezoneMode = 'local';",
            "  try { const saved = Number(localStorage.getItem(UTIL_TIMELINE_KEY)); if (saved === 24 || saved === 48) utilTimelineHours = saved; } catch (_) {}",
            "  const esc = value => String(value).replace(/[&<>\"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));",
            "  const lagFreshness = document.getElementById('lag-freshness'), lagChart = document.getElementById('lag-chart'), lagChartWrap = document.getElementById('lag-chart-wrap'), lagHeatmap = document.getElementById('lag-heatmap'), lagHeatmapWrap = document.getElementById('lag-heatmap-wrap');",
            "  const sourceChart=document.getElementById('source-chart'), sourceFreshness=document.getElementById('source-freshness'), sourceTooltip=document.getElementById('source-tooltip'), sourceButtons=[...document.querySelectorAll('[data-source-days]')]; const SOURCE_DAYS_KEY='pc-task-source-days'; let sourceDays=7; try { const saved=Number(localStorage.getItem(SOURCE_DAYS_KEY)); if(saved===7||saved===14)sourceDays=saved; } catch (_) {} sourceButtons.forEach(b=>b.classList.toggle('active',Number(b.dataset.sourceDays)===sourceDays)); const SOURCE_COLORS=['#56b4e9','#e69f00','#009e73','#f0e442','#0072b2','#d55e00','#cc79a7','#88ccee','#aa4499','#44aa99','#ee7733','#332288']; const sourceColor=(source,index)=>source==='unknown'?'#777':SOURCE_COLORS[index%SOURCE_COLORS.length], fmtSourcePct=(tasks,total)=>total?`${(100*tasks/total).toFixed(1)}%`:'0.0%', hideSourceTooltip=()=>{sourceTooltip.classList.remove('visible');sourceTooltip.setAttribute('aria-hidden','true');}, showSourceTooltip=(segment,details,event)=>{const pct=fmtSourcePct(details.tasks,details.total), rows=details.rows.map(row=>`<div class='source-tooltip-row ${row.source===details.source?'active':''}'><span>${esc(row.source)}</span><span>${row.tasks} · ${fmtSourcePct(row.tasks,details.total)}</span></div>`).join('');sourceTooltip.innerHTML=`<p class='source-tooltip-heading'>${esc(details.day)} · ${details.total} tasks</p><div class='source-tooltip-current'><strong>${esc(details.source)} — ${details.tasks} tasks (${pct})</strong></div><div class='source-tooltip-rows'>${rows}</div>`;sourceTooltip.classList.add('visible');sourceTooltip.setAttribute('aria-hidden','false');const rect=segment.getBoundingClientRect(), x=event?.clientX??rect.left+rect.width/2, y=event?.clientY??rect.top, tip=sourceTooltip.getBoundingClientRect(), left=Math.max(8,Math.min(x+12,window.innerWidth-tip.width-8)), top=y+12+tip.height<=window.innerHeight-8?y+12:Math.max(8,rect.top-tip.height-8);sourceTooltip.style.left=`${left}px`;sourceTooltip.style.top=`${top}px`;}; function loadSources(){fetch(`${JOB_SOURCES_URL}?days=${sourceDays}`).then(r=>r.ok?r.json():Promise.reject(new Error(`HTTP ${r.status}`))).then(data=>{const sources=[...new Set(data.buckets.map(b=>b.source))].sort(), sourceColors=new Map(sources.map((s,index)=>[s,sourceColor(s,index)])), sourceTotals=new Map(sources.map(s=>[s,0])), days=[...new Set(data.buckets.map(b=>b.day))], byDay=new Map(days.map(d=>[d,[]])); data.buckets.forEach(b=>{byDay.get(b.day).push(b);sourceTotals.set(b.source,sourceTotals.get(b.source)+b.tasks);}); const displaySources=[...sources].sort((a,b)=>sourceTotals.get(b)-sourceTotals.get(a)||a.localeCompare(b)), max=Math.max(...days.map(d=>byDay.get(d).reduce((n,b)=>n+b.tasks,0)),1), tooltipDetails=new Map; sourceChart.innerHTML=days.map(d=>{const rowBySource=new Map(byDay.get(d).map(b=>[b.source,b])), rows=displaySources.map(s=>rowBySource.get(s)).filter(Boolean), total=rows.reduce((n,b)=>n+b.tasks,0); return `<div class='source-day'>${rows.map(b=>{const key=encodeURIComponent(`${d}:${b.source}`);tooltipDetails.set(key,{day:d,source:b.source,tasks:b.tasks,total,rows});return `<div class='source-segment' tabindex='0' data-source-key='${esc(key)}' style='height:${100*b.tasks/max}%;background:${sourceColors.get(b.source)}' aria-label='${esc(`${d}, ${b.source}: ${b.tasks} tasks (${fmtSourcePct(b.tasks,total)})`)}' aria-describedby='source-tooltip'></div>`;}).join('')}</div>`}).join(''); sourceChart.querySelectorAll('.source-segment').forEach(segment=>{const details=tooltipDetails.get(segment.dataset.sourceKey), show=event=>showSourceTooltip(segment,details,event);segment.addEventListener('mouseenter',show);segment.addEventListener('focus',show);segment.addEventListener('mouseleave',hideSourceTooltip);segment.addEventListener('blur',hideSourceTooltip);}); sourceFreshness.textContent=`Last ${sourceDays} days · ${data.buckets.reduce((n,b)=>n+b.tasks,0)} terminal runs`;}).catch(e=>{sourceFreshness.textContent=`Job sources unavailable: ${e.message}`;});} document.addEventListener('keydown',event=>{if(event.key==='Escape')hideSourceTooltip();}); sourceButtons.forEach(b=>b.addEventListener('click',()=>{sourceDays=Number(b.dataset.sourceDays);try { localStorage.setItem(SOURCE_DAYS_KEY,String(sourceDays)); } catch (_) {}sourceButtons.forEach(x=>x.classList.toggle('active',x===b));hideSourceTooltip();loadSources();})); loadSources();",
            "  const sourceTaskCount=segment=>Number(segment.getAttribute('aria-label').match(/: (\\d+) tasks/)[1]), fmtSourceAxis=value=>value>=1000?`${value/1000}k`:String(value), sourceAxis=max=>{const rough=max/3, power=10**Math.floor(Math.log10(rough)), step=[1,2,2.5,5,10].map(n=>n*power).find(n=>{const ticks=Math.ceil(max/n);return ticks>=2&&ticks<=4;})||max, ticks=Math.ceil(max/step);return {max:step*ticks,step,ticks};}; function addSourceAxis(){if(sourceChart.querySelector('.source-bars'))return;const days=[...sourceChart.children];if(!days.length)return;const totals=days.map(day=>[...day.children].reduce((total,segment)=>total+sourceTaskCount(segment),0)), max=Math.max(...totals,1), axis=sourceAxis(max), values=Array.from({length:axis.ticks+1},(_,index)=>index*axis.step), labels=values.map(value=>`<span class='source-axis-label' style='bottom:${100*value/axis.max}%'>${fmtSourceAxis(value)}</span>`).join(''), gridlines=values.slice(1).map(value=>`<span class='source-gridline' style='bottom:${100*value/axis.max}%'></span>`).join('');sourceChart.innerHTML=`<div class='source-axis' aria-label='Task count axis'>${labels}</div><div class='source-plot'><div class='source-grid' aria-hidden='true'>${gridlines}</div><div class='source-bars'></div></div>`;const bars=sourceChart.querySelector('.source-bars');days.forEach(day=>{[...day.children].forEach(segment=>{segment.style.height=`${100*sourceTaskCount(segment)/axis.max}%`;});bars.append(day);});} new MutationObserver(addSourceAxis).observe(sourceChart,{childList:true});",
            "  const fmtLag = value => value == null ? '—' : value >= 60 ? `${(value / 60).toFixed(value % 60 ? 1 : 0)}m` : `${value.toFixed(0)}s`;",
            "  const lagKey = startAt => { const date = new Date(startAt); return `${(date.getUTCDay()+6)%7}-${date.getUTCHours()}`; }; const setLagHover = key => document.querySelectorAll(`[data-lag-key=\"${key}\"]`).forEach(element => element.classList.add('lag-linked-hover')); const clearLagHover = () => document.querySelectorAll('.lag-linked-hover').forEach(element => element.classList.remove('lag-linked-hover')); const bindLagHover = () => document.querySelectorAll('[data-lag-key]').forEach(element => { element.addEventListener('mouseenter', () => setLagHover(element.dataset.lagKey)); element.addEventListener('mouseleave', clearLagHover); });",
            "  function renderLag(data) { const usable = data.buckets.filter(b => b.sufficient_samples); if (!usable.length) { lagChart.innerHTML = `<text x='24' y='35' fill='#888'>Not enough observed terminal runs yet (need ${data.min_samples} per hour).</text>`; return; } const width=lagChart.clientWidth||960, height=lagChart.clientHeight||240, left=82, right=width-32, top=18, bottom=height-42; lagChart.setAttribute('viewBox',`0 0 ${width} ${height}`); const first=data.buckets.indexOf(usable[0]), last=data.buckets.indexOf(usable.at(-1)), span=data.buckets.slice(first,last+1), rawMax=Math.max(data.slo_seconds,...usable.map(b=>b.p95_seconds)), niceSteps=[60,120,300,600,900,1800,3600,7200,14400,21600,43200,86400], axisStep=niceSteps.find(step=>Math.ceil(rawMax/step)<=10)||niceSteps.at(-1), axisMax=Math.ceil(rawMax/axisStep)*axisStep, tickCount=axisMax/axisStep, labelEvery=Math.ceil(tickCount/5), maxCount=Math.max(...span.map(b=>b.sample_count),1), x=i=>span.length===1?(left+right)/2:left+i*(right-left)/(span.length-1), y=v=>bottom-v/axisMax*(bottom-top), axisLabel=v=>v>=3600?`${v/3600}h`:fmtLag(v), points=key=>usable.map(b=>`${x(span.indexOf(b)).toFixed(1)},${y(b[key])}`).join(' '); const bars=span.map((b,i)=>`<rect class='lag-point' data-lag-key='${lagKey(b.start_at)}' x='${x(i)-2}' y='${height-20-b.sample_count/maxCount*22}' width='4' height='${b.sample_count/maxCount*22}' fill='#555'><title>${b.start_at}\\n${b.sample_count} samples\\np50 ${fmtLag(b.p50_seconds)}, p95 ${fmtLag(b.p95_seconds)}</title></rect>`).join(''); const dots=usable.flatMap(b=>[['p50_seconds','#5dd'],['p95_seconds','#f90']].map(([key,color])=>`<circle class='lag-point' data-lag-key='${lagKey(b.start_at)}' cx='${x(span.indexOf(b))}' cy='${y(b[key])}' r='4.5' fill='${color}'><title>${b.start_at}\\n${key === 'p50_seconds' ? 'p50' : 'p95'} ${fmtLag(b[key])}\\n${b.sample_count} samples</title></circle>`)).join(''); const lines=usable.length>=4?`<polyline points='${points('p50_seconds')}' fill='none' stroke='#5dd' stroke-width='2.5'/><polyline points='${points('p95_seconds')}' fill='none' stroke='#f90' stroke-width='2.5'/>`:''; const ticks=Array.from({length:tickCount+1},(_,i)=>i*axisStep).map((v,i)=>`<line x1='${left}' y1='${y(v)}' x2='${right}' y2='${y(v)}' stroke='#2d2d2d'/><line x1='${left-4}' y1='${y(v)}' x2='${left}' y2='${y(v)}' stroke='#666'/>${(i%labelEvery===0||v===axisMax)?`<text x='${left-8}' y='${y(v)+5}' text-anchor='end' fill='#aaa' font-size='14'>${axisLabel(v)}</text>`:''}`).join(''); lagChart.innerHTML=`${ticks}<line x1='${left}' y1='${y(data.slo_seconds)}' x2='${right}' y2='${y(data.slo_seconds)}' stroke='#f44' stroke-dasharray='5 4'/><text x='${left+4}' y='${y(data.slo_seconds)-5}' fill='#f77' font-size='14'>SLO ${fmtLag(data.slo_seconds)}</text><text x='${left}' y='${height-4}' fill='#999' font-size='14'>${span[0].start_at.slice(5,16)} UTC</text><text x='${right}' y='${height-4}' text-anchor='end' fill='#999' font-size='14'>${span.at(-1).start_at.slice(5,16)} UTC</text>${bars}${lines}${dots}`; }",
            "  function renderLagHeatmap(data) { const names=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'], cells=new Map(data.heatmap.map(c=>[`${c.weekday}-${c.hour}`,c])), max=Math.max(data.slo_seconds,...data.heatmap.filter(c=>c.sufficient_samples).map(c=>c.p95_seconds||0)); let html=`<span></span>${[...Array(24).keys()].map(h=>`<span class='lag-hm-label'>${h}</span>`).join('')}`; for(let d=0;d<7;d++){html+=`<span class='lag-hm-label lag-hm-day'>${names[d]}</span>`;for(let h=0;h<24;h++){const c=cells.get(`${d}-${h}`), sparse=!c.sufficient_samples, light=c.p95_seconds==null?12:16+Math.min(1,c.p95_seconds/max)*34, title=`${names[d]} ${h}:00 UTC\\n${c.sample_count} samples\\np95: ${fmtLag(c.p95_seconds)}\\n${sparse?`Insufficient samples (need ${data.min_samples})`:`${c.started_within_slo_pct}% within SLO`}`;html+=`<div class='lag-hm-cell ${sparse?'insufficient':''}' data-lag-key='${d}-${h}' style='background:hsl(18 75% ${light}%)' title='${esc(title)}'></div>`;}}lagHeatmap.innerHTML=html; }",
            "  function loadStartLag() { fetch(START_LAG_VISUALIZATION_URL).then(r=>r.ok?r.json():Promise.reject(new Error(`HTTP ${r.status}`))).then(data=>{const hourly=data.buckets.filter(b=>b.sufficient_samples).length, heatmap=data.heatmap.filter(c=>c.sample_count).length; lagFreshness.textContent=`Last 7 days · SLO ${fmtLag(data.slo_seconds)} · ${hourly} hourly bucket${hourly===1?'':'s'} at ${data.min_samples}+ samples · ${heatmap} populated UTC heatmap cell${heatmap===1?'':'s'}`; renderLag(data); renderLagHeatmap(data); bindLagHover();}).catch(error=>{lagFreshness.textContent='Observed start lag could not be loaded.';lagChart.innerHTML=`<text x='24' y='35' fill='#f44'>${esc(error.message)}</text>`;}); }",
            "  loadStartLag();",
            "  const utilCard = (label, data) => {",
            "    if (data.status === 'error') return `<article class='util-card incomplete'><h3>${label}</h3><p class='bad'>Request error</p><p class='util-detail'>${esc(data.error || 'Unknown error')}</p></article>`;",
            "    const u = data.utilization;",
            "    if (!u || u.status === 'incomplete') return `<article class='util-card incomplete'><h3>${label}</h3><p>Collecting data</p><p class='util-detail'>Coverage: ${(u?.coverage_pct ?? 0).toFixed(1)}%</p></article>`;",
            "    if (u.status === 'partial') { const quality = u.utilization_pct > 100 ? `<p class='warn'>Over 100% — possible data-quality issue</p>` : ''; const capacity = u.available_worker_hours > 0 ? `Available: ${u.available_worker_hours.toFixed(2)} worker-hours` : 'No available capacity observed'; const utilization = u.utilization_pct === null ? '—' : `<strong>${u.utilization_pct.toFixed(1)}%</strong> utilization`; return `<article class='util-card partial'><h3>${label}</h3><p>${utilization}</p><p class='util-detail'>Partial coverage: ${u.coverage_pct.toFixed(1)}% — measured values cover only observed time.<br>Busy: ${u.busy_worker_hours.toFixed(2)} worker-hours<br>${capacity}</p>${quality}</article>`; }",
            "    if (u.status === 'unavailable') return `<article class='util-card complete'><h3>${label}</h3><p>No available capacity</p><p class='util-detail'>Busy: ${u.busy_worker_hours.toFixed(2)} worker-hours</p></article>`;",
            "    const quality = u.utilization_pct > 100 ? `<p class='warn'>Over 100% — possible data-quality issue</p>` : '';",
            "    return `<article class='util-card complete'><h3>${label}</h3><p><strong>${u.utilization_pct.toFixed(1)}%</strong> utilization</p><p class='util-detail'>Busy: ${u.busy_worker_hours.toFixed(2)} worker-hours<br>Available: ${u.available_worker_hours.toFixed(2)} worker-hours<br>Coverage: 100%</p>${quality}</article>`;",
            "  };",
            "  const coverageReason = reason => ({get_worker_error:'Worker lookup failed', recent_tasks_no_overlap:'Recent task windows did not overlap', incomplete_poll:'Task poll was incomplete'})[reason] || reason;",
            "  function coverageEventsForBucket(events, bucket) { const bucketStart = new Date(bucket.start_at), bucketEnd = new Date(bucket.end_at); return events.filter(event => { const observed = new Date(event.observed_at), previous = event.previous_observed_at ? new Date(event.previous_observed_at) : observed; return previous < bucketEnd && observed > bucketStart; }); }",
            "  function coverageBreakDetail(events, diagnosticsAvailable) { if (!diagnosticsAvailable) return 'Coverage-break diagnostics could not be loaded.'; if (!events.length) return 'No retained coverage-break event explains this gap.'; const groups = new Map(); events.forEach(event => { const key = `${event.observed_at}\\u0000${event.reason}`; groups.set(key, [...(groups.get(key) || []), event]); }); return [...groups.values()].flatMap(group => { if (group.length >= 5) { const event = group[0], previous = event.previous_window_count ?? '—', current = event.current_window_count ?? '—', overlap = event.overlap_count ?? '—'; return `${coverageReason(event.reason)}: ${group.length} workers; windows: ${previous} → ${current}; overlap: ${overlap}`; } return group.map(event => { const workers = event.worker_id || 'unknown worker'; const previous = event.previous_window_count ?? '—', current = event.current_window_count ?? '—', overlap = event.overlap_count ?? '—'; return `${coverageReason(event.reason)}; worker: ${workers}; windows: ${previous} → ${current}; overlap: ${overlap}`; }); }).join('\\n'); }",
            "  function loadUtilizationTimeline() { if (!utilDataThrough) return; const end = new Date(utilDataThrough); const start = new Date(end.getTime() - utilTimelineHours * 3600000); const query = new URLSearchParams({start:start.toISOString(), end:end.toISOString(), bucket_seconds:'3600'}); const coverageQuery = new URLSearchParams({start:start.toISOString(), end:end.toISOString()}); utilTimeline.textContent = 'Loading hourly timeline…'; Promise.all([fetch(`${UTIL_TIMELINE_URL}?${query}`).then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))), fetch(`${COVERAGE_BREAKS_URL}?${coverageQuery}`).then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))).catch(() => null)]).then(([data, coverageBreaks]) => { const events = coverageBreaks?.events || []; utilTimeline.style.gridTemplateColumns = `repeat(${data.buckets.length}, minmax(1rem, 1fr))`; utilTimeline.textContent = ''; [...data.buckets].reverse().forEach(bucket => { let klass = 'util-hour-error', status = 'Utilization unavailable', usage = null; if (bucket.status === 'incomplete') [klass, status] = ['util-hour-incomplete', `No shared coverage (${bucket.coverage_pct.toFixed(1)}%)`]; else if (bucket.status === 'unavailable') [klass, status] = ['util-hour-unavailable', 'No available capacity']; else if ((bucket.status === 'available' || bucket.status === 'partial') && bucket.utilization_pct !== null) { const value = Math.max(0, Math.min(100, bucket.utilization_pct)); klass = bucket.status === 'partial' ? 'util-hour-usage util-hour-partial' : 'util-hour-usage'; usage = value; status = `${value.toFixed(1)}% utilization${bucket.status === 'partial' ? ` (${bucket.coverage_pct.toFixed(1)}% coverage)` : ''}`; } const cell = document.createElement('div'); cell.className = `util-hour ${klass}`; if (usage !== null) cell.style.backgroundColor = `hsl(270 55% ${8 + usage * .29}%)`; const breakDetail = bucket.complete ? '' : `\\n${coverageBreakDetail(coverageEventsForBucket(events, bucket), coverageBreaks !== null)}`; cell.title = `${bucket.start_at} – ${bucket.end_at}\\n${status}\\nBusy: ${bucket.busy_worker_hours ?? '—'} worker-hours\\nAvailable: ${bucket.available_worker_hours ?? '—'} worker-hours\\nCoverage: ${bucket.coverage_pct.toFixed(1)}%${breakDetail}`; utilTimeline.appendChild(cell); }); }).catch(error => { utilTimeline.style.gridTemplateColumns = '1fr'; utilTimeline.innerHTML = `<span class='bad'>Hourly timeline unavailable: ${esc(error.message)}</span>`; }); }",
            "  function setUtilizationTimeline(hours) { utilTimelineHours = hours; utilTimelineButtons.forEach(button => button.classList.toggle('active', Number(button.dataset.hours) === hours)); try { localStorage.setItem(UTIL_TIMELINE_KEY, String(hours)); } catch (_) {} loadUtilizationTimeline(); }",
            "  utilTimelineButtons.forEach(button => button.addEventListener('click', () => setUtilizationTimeline(Number(button.dataset.hours))));",
            "  fetch(UTIL_SUMMARY_URL).then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))).then(data => {",
            "    const through = data.data_through; const age = through ? Math.max(0, Math.round((Date.now() - new Date(through)) / 60000)) : null;",
            "    utilFreshness.textContent = through ? `Data through ${formatTime(through, timezoneMode)} (${age < 60 ? age + 'm' : Math.floor(age / 60) + 'h'} old)` : 'Collecting data: no common coverage boundary yet.';",
            "    utilCards.innerHTML = ['1h','24h','7d','30d'].map(label => utilCard(label, data.windows[label] || {status:'ok', utilization:null})).join('');",
            "    utilDataThrough = through; if (through) setUtilizationTimeline(utilTimelineHours); else utilTimeline.textContent = 'Collecting data: no complete hourly timeline yet.';",
            "  }).catch(error => { utilFreshness.textContent = 'Utilization could not be loaded.'; utilCards.innerHTML = ['1h','24h','7d','30d'].map(label => utilCard(label, {status:'error', error:error.message})).join(''); utilTimeline.innerHTML = `<span class='bad'>Hourly timeline unavailable: ${esc(error.message)}</span>`; });",
            "  const tip = document.getElementById('hm-tip');",
            "  document.querySelectorAll('.hm-cell').forEach(cell => {",
            "    cell.addEventListener('mouseenter', e => {",
            "      const d = JSON.parse(cell.dataset.info);",
            "      const wid = cell.closest('tr').dataset.wid;",
            "      const lines = [`<div class='tip-worker'>${wid}</div>`, `<div class='tip-period'>${d.period}</div>`];",
            "      if (d.ok) lines.push(`<div class='tip-ok'>✓ ok: ${d.ok}</div>`);",
            "      const cats = Object.entries(d.cats || {});",
            "      cats.sort((a, b) => {",
            "        const sa = SEV_ORDER[HM_SEV[a[0]]] ?? 3, sb = SEV_ORDER[HM_SEV[b[0]]] ?? 3;",
            "        return sa !== sb ? sa - sb : b[1] - a[1];",
            "      });",
            "      for (const [cat, cnt] of cats) {",
            "        const sev = HM_SEV[cat] || 'low';",
            "        lines.push(`<div class='${SEV_CLASS[sev]}'>${SEV_ICON[sev]} ${cat}: ${cnt}</div>`);",
            "      }",
            "      if (!d.ok && cats.length === 0) lines.push(`<div class='tip-dim'>no activity</div>`);",
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
            "  const arButtons = [...document.querySelectorAll('[data-autorefresh]')];",
            "  let arTimer = null;",
            "  function setAutoRefresh(enabled) {",
            "    localStorage.setItem('autorefresh', enabled ? 'on' : 'off');",
            "    arButtons.forEach(button => { const active = (button.dataset.autorefresh === 'on') === enabled; button.classList.toggle('active', active); button.setAttribute('aria-pressed', String(active)); });",
            "    clearTimeout(arTimer);",
            "    arTimer = enabled ? setTimeout(() => location.reload(), 300000) : null;",
            "  }",
            "  arButtons.forEach(button => button.addEventListener('click', () => setAutoRefresh(button.dataset.autorefresh === 'on')));",
            "  setAutoRefresh(localStorage.getItem('autorefresh') !== 'off');",
            "  function formatTime(iso, mode) {",
            "    const d = new Date(iso);",
            "    if (mode === 'utc') return iso.slice(0,19).replace('T',' ') + ' UTC';",
            "    return d.toLocaleString(undefined, {year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'});",
            "  }",
            "  const timezoneButtons = [...document.querySelectorAll('[data-timezone]')];",
            "  function updateTimes(mode) {",
            "    document.querySelectorAll('.utc-time').forEach(el => el.textContent = formatTime(el.dataset.utc, mode));",
            "  }",
            "  function setTimezone(mode) {",
            "    timezoneMode = mode;",
            "    timezoneButtons.forEach(button => { const active = button.dataset.timezone === mode; button.classList.toggle('active', active); button.setAttribute('aria-pressed', String(active)); });",
            "    updateTimes(mode);",
            "  }",
            "  timezoneButtons.forEach(button => button.addEventListener('click', () => setTimezone(button.dataset.timezone)));",
            "  setTimezone('local');",
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
            "      const cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv, undefined, {numeric:true, sensitivity:'base'});",
            "      return asc ? cmp : -cmp;",
            "    });",
            "    rows.forEach(r => tbody.appendChild(r));",
            "  }",
            "  document.querySelectorAll('table:not(.not-sortable) th').forEach(th => th.addEventListener('click', () => sortTable(th)));",
            "</script>",
            f'<p class="footer">generated on <span class="utc-time" data-utc="{now.isoformat()}">{now.strftime("%Y-%m-%d %H:%M:%S UTC")}</span></p>',
            "</body>",
            "</html>",
            "",
        ]

        return "\n".join(parts)
