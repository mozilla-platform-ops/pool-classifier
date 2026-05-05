"""Pool failure classifier: monitors all workers in a TC pool and classifies task failures from logs."""

import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import taskcluster
from alive_progress import alive_bar

TC_ROOT = "https://firefox-ci-tc.services.mozilla.com"
LOG_TAIL_BYTES = 51200  # 50 KB

DEFAULT_PROVISIONER = "proj-autophone"
DEFAULT_WORKER_TYPE = "gecko-t-lambda-perf-a55"
DEFAULT_POLL_INTERVAL = 900  # seconds (15 minutes)
WORKER_REFRESH_INTERVAL = 300  # seconds between re-listing workers
WORKER_THREAD_COUNT = 8
CONSECUTIVE_FAILURE_ALERT = 2
SEEN_TASKS_CAP = 30  # max seen task IDs to persist per worker (recentTasks is ~20)

# Patterns are checked in order; first match wins.
# Edit these to match the failure modes you care about.
FAILURE_PATTERNS = [
    (r"TypeError: Cannot read properties of undefined \(reading 'samples'\)", "browsertime_samples"),
    (r"ADB server didn't ACK", "adb_no_ack"),
    (r"DEVICE_UNAVAILABLE", "device_unavailable"),
    (r"mozdevice\.DeviceError", "device_error"),
    (r"error: device .* not found", "device_not_found"),
    (r"DeviceDisconnectedError", "device_disconnected"),
    (r"[Tt]imeout\b", "timeout"),
]

logger = logging.getLogger(__name__)


class PoolClassifier:
    def __init__(
        self,
        provisioner: str,
        worker_type: str,
        results_dir: Path,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ):
        self.provisioner = provisioner
        self.worker_type = worker_type
        self.results_dir = results_dir
        self.poll_interval = poll_interval
        self.queue_base = f"{TC_ROOT}/api/queue/v1"
        self.seen_tasks: Dict[str, set] = {}
        self._interrupted = False
        self._init_tc()

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
            r = requests.get(url, headers={"Range": f"bytes=-{LOG_TAIL_BYTES}"}, timeout=60)
            if r.status_code in (200, 206):
                return r.text
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

    # --- state ---

    @property
    def _state_file(self) -> Path:
        return self.results_dir / "state.json"

    def _load_state(self) -> dict:
        if self._state_file.exists():
            try:
                state = json.loads(self._state_file.read_text())
                # restore seen_tasks into memory
                for worker_id, w in state.get("workers", {}).items():
                    self.seen_tasks[worker_id] = set(w.get("seen_task_ids", []))
                return state
            except Exception as e:
                logger.warning(f"Could not parse state file, starting fresh: {e}")
        return {"workers": {}}

    def _save_state(self, state: dict):
        self.results_dir.mkdir(parents=True, exist_ok=True)
        state["last_updated"] = datetime.now(timezone.utc).isoformat()
        # persist seen task IDs (capped) so restarts don't reprocess old tasks
        for worker_id, seen in self.seen_tasks.items():
            if worker_id in state["workers"]:
                state["workers"][worker_id]["seen_task_ids"] = list(seen)[-SEEN_TASKS_CAP:]
        tmp = self._state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2) + "\n")
        tmp.replace(self._state_file)

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

    def _process_results(self, worker_id: str, terminal_tasks: List[Tuple], state: dict, bar=None):
        w = state["workers"].setdefault(
            worker_id,
            {
                "successes": 0,
                "failures": 0,
                "failures_by_category": {},
                "consecutive_failures": 0,
                "last_active": None,
                "last_success": None,
                "last_failure": None,
                "last_failure_category": None,
            },
        )

        for task_id, run_id, run_state, run_started, reason_resolved in terminal_tasks:
            if self._interrupted:
                logger.info(f"  {worker_id}: interrupted, skipping remaining tasks")
                break
            if bar:
                bar()
            if run_started:
                if w["last_active"] is None or run_started > w["last_active"]:
                    w["last_active"] = run_started

            if run_state == "completed":
                w["successes"] += 1
                w["consecutive_failures"] = 0
                w["last_success"] = run_started or w["last_success"]
                logger.info(f"  {worker_id}: completed task={task_id} run={run_id}")
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
                w["failures"] += 1
                w["failures_by_category"][category] = w["failures_by_category"].get(category, 0) + 1
                w["consecutive_failures"] += 1
                w["last_failure"] = run_started or w["last_failure"]
                w["last_failure_category"] = category
                logger.info(f"  {worker_id}: {run_state} task={task_id} run={run_id} → {category}")
                if category == "unclassified" and log_text:
                    self._save_unclassified(task_id, run_id, worker_id, log_text)

    # --- main loop ---

    def run(self):
        signal.signal(signal.SIGINT, self._handle_interrupt)
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

            state = self._load_state()

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
                            self._process_results(worker_id, terminal_tasks, state, bar)
            else:
                for worker_id, terminal_tasks in poll_results:
                    if self._interrupted:
                        break
                    if terminal_tasks:
                        self._process_results(worker_id, terminal_tasks, state)

            self._save_state(state)
            self._update_reports(state)

            alerting_count = sum(
                1 for w in state["workers"].values() if w.get("consecutive_failures", 0) >= CONSECUTIVE_FAILURE_ALERT
            )
            scan_summary = (
                f"{scanned}/{total_workers} workers" if scanned < total_workers else f"{total_workers} workers"
            )
            logger.info(
                f"Scan done: {scan_summary} scanned, {new_total} new terminal tasks, "
                f"{alerting_count} workers with ≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures. "
                f"{'Interrupted.' if self._interrupted else f'Sleeping {self.poll_interval}s...'}",
            )

            for _ in range(self.poll_interval):
                if self._interrupted:
                    break
                time.sleep(1)

        logger.info("Interrupted — exiting.")
        sys.exit(0)

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
        print("\n[Ctrl-C] Will stop at next best time. Press again to exit immediately.", file=sys.stderr)

    # --- report helpers ---

    def _fmt_dt(self, iso: Optional[str]) -> str:
        if not iso:
            return ""
        return iso[:19].replace("T", " ") + " UTC"

    def _top_category(self, worker_state: dict) -> str:
        cats = worker_state.get("failures_by_category", {})
        if not cats:
            return ""
        return max(cats, key=lambda k: cats[k])

    def _sr_pct(self, worker_state: dict) -> Optional[float]:
        s = worker_state.get("successes", 0)
        f = worker_state.get("failures", 0)
        total = s + f
        if total == 0:
            return None
        return s / total

    def _update_reports(self, state: dict):
        self._write_md(state)
        self._write_html(state)

    def _write_md(self, state: dict):
        now = datetime.now(timezone.utc)
        workers = state.get("workers", {})
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
                lines.append(f"- {cat}: {count}")
            lines.append("")

        if alerting:
            lines += [f"## Needs Attention (≥{CONSECUTIVE_FAILURE_ALERT} consecutive failures)", ""]
            for wid, w in sorted(alerting.items(), key=lambda x: -x[1].get("consecutive_failures", 0)):
                lines.append(
                    f"- **{wid}**: {w['consecutive_failures']} consecutive failures "
                    f"({w.get('last_failure_category', '?')}), "
                    f"last: {self._fmt_dt(w.get('last_failure'))}",
                )
            lines.append("")

        if workers:
            lines += [
                "## All Workers",
                "",
                "| Worker | SR% | Successes | Failures | Top Category | Consec Fails | Last Active |",
                "|--------|-----|-----------|----------|--------------|--------------|-------------|",
            ]
            for wid, w in sorted(workers.items()):
                sr = self._sr_pct(w)
                sr_str = f"{sr:.0%}" if sr is not None else "—"
                lines.append(
                    f"| {wid} | {sr_str} | {w.get('successes', 0)} | {w.get('failures', 0)} | "
                    f"{self._top_category(w)} | {w.get('consecutive_failures', 0)} | "
                    f"{self._fmt_dt(w.get('last_active'))} |",
                )
            lines.append("")

        self.results_dir.mkdir(parents=True, exist_ok=True)
        (self.results_dir / "OVERVIEW.md").write_text("\n".join(lines) + "\n")

    def _write_html(self, state: dict):
        now = datetime.now(timezone.utc)
        workers = state.get("workers", {})
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
            '<meta http-equiv="refresh" content="60">',
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
                parts.append(
                    f'  <li class="bad"><strong>{wid}</strong>: {w["consecutive_failures"]} consecutive failures '
                    f"({w.get('last_failure_category', '?')}) — last: {fmt(w.get('last_failure'))}</li>",
                )
            parts.append("</ul>")

        parts += [
            "<h2>All Workers</h2>",
            "<table>",
            "  <thead><tr>",
            "    <th>Worker</th><th>SR%</th><th>Successes</th><th>Failures</th>"
            "<th>Top Category</th><th>Consec Fails</th><th>Last Active</th>",
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
            parts.append(
                f"  <tr{row_class}>"
                f"<td>{wid}</td>"
                f'<td class="{sr_class(w)}">{sr_str(w)}</td>'
                f'<td class="ok">{w.get("successes", 0)}</td>'
                f"<td{fail_class}>{failures}</td>"
                f"<td>{self._top_category(w)}</td>"
                f"<td{consec_class}>{consec}</td>"
                f"<td>{fmt(w.get('last_active'))}</td>"
                "</tr>",
            )

        parts += ["  </tbody>", "</table>"]

        parts += [
            "<script>",
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
