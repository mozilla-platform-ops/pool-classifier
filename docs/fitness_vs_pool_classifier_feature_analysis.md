# Feature Analysis: fitness.py vs pool_classifier.py

## Overview

| Dimension | fitness.py | pool_classifier.py |
|-----------|------------|-------------------|
| **Primary Goal** | Real-time worker health snapshot | Continuous failure classification & trend tracking |
| **Data Source** | TC API (task state only) | TC API + log artifacts |
| **Storage** | Memory (per-run) | SQLite / Postgres (persistent) |
| **Report Types** | Console table | HTML dashboard + Markdown |
| **Metrics** | SR, last_started, exception count | SR (windowed 1d/3d/7d), categories, heatmap |
| **Alerting** | Low SR, no work, consecutive fails | Consecutive fails, quarantine status |
| **Classification** | State-based only | Log-pattern-based (patterns.yaml) |
| **Execution** | One-shot CLI | Daemon loop (with one-shot mode) |
| **Failure Root Cause** | Not attempted | Pattern-matched from logs |
| **Historical Trending** | No | Yes |
| **Network Testing** | Yes (ping) | No |
| **Pool Reconciliation** | Yes (Moonshot, packet, R8) | No |
| **Interactive UI** | No | Yes (HTML dashboard) |

---

## fitness.py — Feature Inventory

**File:** `worker_health/fitness.py` / launcher: `fitness_check.py`

### Data Collection
- Recent tasks per worker from TC Queue API (`get_worker_jobs()`)
- Individual task status from TC (`get_task_status()`)
- Pending task counts per worker type (`get_pending_tasks()`)
- Worker listings with groups from TC (`get_workers()`)
- Quarantine state from TC Queue API

### Metrics
- Success ratio: `successes / (successes + failures)`
- Success count, completion count, exception count, running count
- Last-started timestamp
- Consecutive failures from end of history (`consecutive_non_ones_from_end()`)

### Alerting Thresholds
- SR below `--alert-percent` (default 85%)
- ≥2 consecutive failures
- No work in last `--alert-time` minutes (default 60)
- ≥3 exceptions
- No work done at all
- Worker quarantined (with duration countdown)

### Output
- Colored console table with `graph_percentage()` ASCII bars
- Human-readable time deltas
- Optional humanized worker IDs via `humanhash` (`--humanize-hashes`)

### Actions
- Read-only; no direct quarantine writes

### CLI Flags
| Flag | Default | Description |
|------|---------|-------------|
| `-s, --success_rate` | — | Sort by SR instead of worker ID |
| `-a, --alert-percent` | 0.85 | Low-health SR threshold |
| `-t, --alert-time` | 60 | Minutes before "no work" alert |
| `-o, --only-show-alerting` | — | Hide healthy workers |
| `-p, --provisioner` | — | TC provisioner ID |
| `-hh, --humanize-hashes` | — | 3-word hash aliases for worker IDs |
| `--ping` | — | Ping workers to verify reachability |
| `--ping-domain` | — | Domain suffix for ping targets |
| `--ping-host` | — | SSH host to ping from |

### Invocation Modes
- **Provisioner mode** (no args): report all worker types
- **Queue mode** (one arg): report all workers in a worker type
- **Host mode** (two args): report single worker via `worker_type.worker_id`

### Pool Reconciliation
- `moonshot_worker_report()`: generates expected worker list, flags missing
- `r8_worker_report()`: reconciles YAML puppet inventory against TC state
- `simple_worker_report()`: numbered pool expectations (e.g., `packet-0..59`)

---

## pool_classifier.py — Feature Inventory

**File:** `worker_health/pool_classifier.py` / launcher: `pool_classifier.py`

### Data Collection
- Recent tasks per worker from TC Queue API
- Individual task status from TC (with run details)
- Task log artifacts — HEAD (20 KB) + TAIL (50 KB) via range requests
- Full worker list with continuation-token pagination
- Quarantine state + enriched details via TC GraphQL

### Metrics
- Per-worker: successes, failures, consecutive_failures, last_active, last_success, last_failure, last_failure_category, failures_by_category
- Windowed SR: 1-day, 3-day, 7-day
- 12-hour heatmap: per-worker × per-hour failure counts bucketed by severity (critical/high/low)
- Pool-wide: total successes/failures, failure category totals, alerting worker count

### Alerting
- `CONSECUTIVE_FAILURE_ALERT = 2` — highlighted in reports and console
- Quarantine expiry display (time remaining / expired badge)

### Output
- **HTML dashboard** (`OVERVIEW.html`): interactive heatmap with hover tooltips, sortable worker table, windowed SR columns, top offenders, quarantine table, timezone toggle, auto-refresh
- **Markdown report** (`OVERVIEW.md`): pool summary, category breakdown, per-worker table, top offenders
- **SQLite DB** (`pool_classifier.db`): full persistent task/worker/quarantine history
- **Unclassified logs** (`unclassified/<task_id>.log`): saved for manual inspection

### Classification System
- `patterns.yaml`: YAML-defined regex patterns with severity (critical/high/low), tags, description, enabled toggle
- `patterns_registry.py`: loader with validation, `all_patterns()`, `severity_of()`, `categories_by_severity()`
- First-match-wins; exception tasks sub-classified via TC `reasonResolved` field
- `--reclassify` workflow: re-runs patterns against saved/re-fetched logs, updates DB

### Actions
- Read-only on TC (no quarantine writes)
- Saves unclassified logs locally
- Reclassifies DB entries when patterns change

### CLI Flags
| Flag | Default | Description |
|------|---------|-------------|
| `-p, --provisioner` | `proj-autophone` | TC provisioner |
| `-w, --worker-type` | `gecko-t-lambda-perf-a55` | TC worker type |
| `--poll-interval` | 900 | Seconds between scans |
| `--results-dir` | `pool_classifier_results/` | Output dir |
| `-u, --update-only` | — | One-shot report, then exit |
| `--reclassify` | — | Re-run patterns on stored logs |
| `--reclassify-category` | `unclassified` | Target category for reclassify |
| `--save-unmatched-logs` | — | Save logs still unmatched after reclassify |

---

## Gap Analysis: What fitness.py Has That pool_classifier Lacks

| Feature | fitness.py Location | Notes |
|---------|-------------------|-------|
| **Job queue depth** | `get_pending_tasks()` | Knows if there's no work available; can distinguish idle pool from broken pool |
| **Network reachability** | `device_fitness_report()` ping block | ICMP ping (local or via SSH hop) to verify workers are reachable |
| **Single-worker drill-down** | `fitness_report_single_host()` | `worker_type.worker_id` mode for focused per-host investigation |
| **Pool reconciliation** | `moonshot_worker_report()`, `r8_worker_report()`, `simple_worker_report()` | Expected vs. actual worker count; flags missing hosts |
| **`--only-show-alerting` filter** | CLI flag | Hide healthy workers, show only problems |
| **Humanized worker IDs** | `--humanize-hashes` | 3-word `humanhash` aliases for AWS instance IDs |
| **Sort by success rate** | `-s` flag | CLI control over sort order |

---

## Recommendations for pool_classifier (Ranked by Value)

### 1. Queue depth integration
Fetch pending task count for the pool and surface it as a pool-level note in the HTML dashboard. Helps distinguish "all workers failing" from "pool is idle." Low effort — one TC API call, one line of context in the report header.

### 2. Single-worker drill-down page
Add a `/workers/<provisioner>/<worker_type>/<worker_id>` Flask route showing full task history, category breakdown over time, and recent log snippets for one worker. The DB already has all the data; it just needs a view.

### 3. Expected vs. actual worker count
Add `expected_workers: N` field to `pools.yaml`. Flag in the dashboard when the live count is significantly below expected. Catches silent worker dropouts before they affect SR.

### 4. "Show alerting only" toggle
A client-side filter button on the HTML dashboard to collapse healthy workers and focus on those with consecutive failures or quarantine status.

### 5. Network reachability
Lower priority unless silent dropouts (workers TC still lists but physically unreachable) are a known problem in the pools being monitored.
