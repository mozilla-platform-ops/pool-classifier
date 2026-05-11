# Pool Classifier → Cloud Run Migration

## Goal

Turn the single-pool, long-running `pool_classifier.py` CLI into a multi-pool web app on Google Cloud Run that monitors multiple Taskcluster worker pools behind a shared dashboard.

## Architecture

```
Cloud Scheduler ──POST /classify/<pool_id>──┐
  (one job per pool, every 15 min, OIDC)    │
                                            ▼
        ┌────────────────────────────────────────────┐
Browser │ HTTPS LB + IAP (mozilla.com) ──► Cloud Run │──► Cloud SQL Postgres (private IP)
        │   GET / , /pools/<id>, /healthz             │──► Secret Manager (TC token)
        │   POST /classify/<id> (Scheduler bypass)    │──► Taskcluster API
        └────────────────────────────────────────────┘
        + Cloud Armor (rate limit + OWASP rules)
        + VPC + Serverless VPC Access connector
```

**Key decisions:**
- One Cloud Run service, N pools — each pool registered in `pools.yaml`
- Cloud Scheduler triggers per-pool classify cycles (not APScheduler in-process)
- Cloud SQL Postgres replaces local SQLite; multi-tenanted via `pool_id` column
- IAP at the LB backend (mozilla.com domain access); Scheduler bypasses IAP on `/classify/*` via a separate LB path-matcher
- Deployment mirrors `~/git/hangar/` terraform layout

**Why Cloud Scheduler over APScheduler:** N pools with different cadences map naturally to N Scheduler jobs. Missed ticks on CI monitoring are an incident. Two Cloud Run instances would double-fire an in-process scheduler. Scheduler is auditable in `gcloud scheduler jobs list`.

---

## Phase Checklist

### ✅ Phase 1 — Refactor in place (DONE)

Extract a `Storage` protocol so SQLite can be swapped for Postgres without touching classify logic.

**Changes made:**

- **`worker_health/pool_classifier_web/__init__.py`** — new (empty package marker)
- **`worker_health/pool_classifier_web/storage.py`** — new
  - `SqliteStorage` class: all DB read/write operations extracted from `PoolClassifier`
  - Implements: `init_schema`, `get_seen_tasks`, `record_task_result`, `upsert_worker`, `increment_success`, `increment_failure`, `update_task_category`, `update_worker_last_category`, `count_alerting`, `backfill_worker_groups`, `get_quarantine_cache`, `upsert_quarantine_entry`, `query_workers`, `query_windowed_sr`, `query_heatmap`, `top_offenders`, `oldest_classified_at`, `save_unclassified_log`, `list_unclassified_logs`, `get_task_info`, `db_rows_for_category`
- **`worker_health/pool_classifier.py`** (core module) — refactored:
  - Constructor now accepts `storage=` kwarg; creates `SqliteStorage` by default; `results_dir` is now `Optional[Path]`
  - `_init_tc()` prefers `TC_TOKEN_JSON` env var (for Cloud Run), falls back to `~/.tc_token` file
  - All ~25 `self.db.execute(...)` calls replaced with `self.storage.<method>()`
  - `classify_cycle()` extracted from `run()` body — runs one poll+classify pass, returns `{"scanned", "total_workers", "new_terminal", "alerting"}`; `run()` calls it in a loop as before
  - `_write_md()` and `_write_html()` now **return strings**; `_update_reports()` writes files only when `results_dir` is set
  - `render_html()` and `render_md()` added — web routes call these
  - `alive_progress` import made optional (graceful no-op fallback for the container image)
  - `sqlite3` import and `DB_SCHEMA` removed from this file

**CLI entrypoint (`pool_classifier.py`) and all 18 existing tests pass unchanged.**

---

### ⬜ Phase 2 — Postgres adapter + migrations

Add `PostgresStorage` implementing the same interface as `SqliteStorage`, and a simple migration runner.

**To do:**

- **`worker_health/pool_classifier_web/storage.py`** — add `PostgresStorage`:
  - `psycopg3` (`psycopg[binary]`), `?` → `%s`, `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING`, `INSERT OR REPLACE` → `ON CONFLICT ... DO UPDATE`
  - Every WHERE gains `AND pool_id = %s`; every INSERT gains `pool_id`
  - `query_heatmap`: replace SQLite `strftime('%s', ...)` with `EXTRACT(EPOCH FROM ...)` for Postgres
  - Timestamps stored as `TIMESTAMPTZ`; storage layer always passes/returns ISO strings to callers
- **`worker_health/pool_classifier_web/migrations/001_init.sql`** — Postgres schema:

  ```sql
  CREATE TABLE workers (
      pool_id TEXT NOT NULL, worker_id TEXT NOT NULL, worker_group TEXT,
      successes INT NOT NULL DEFAULT 0, failures INT NOT NULL DEFAULT 0,
      consecutive_failures INT NOT NULL DEFAULT 0,
      last_active TIMESTAMPTZ, last_success TIMESTAMPTZ, last_failure TIMESTAMPTZ,
      last_failure_category TEXT,
      PRIMARY KEY (pool_id, worker_id)
  );
  CREATE TABLE task_results (
      pool_id TEXT NOT NULL, task_id TEXT NOT NULL, worker_id TEXT NOT NULL,
      run_id INT, run_state TEXT NOT NULL, category TEXT, reason_resolved TEXT,
      run_started TIMESTAMPTZ, classified_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (pool_id, task_id, worker_id)
  );
  CREATE INDEX idx_task_results_worker  ON task_results (pool_id, worker_id);
  CREATE INDEX idx_task_results_started ON task_results (pool_id, run_started);
  CREATE INDEX idx_task_results_cat     ON task_results (pool_id, category);
  CREATE TABLE quarantine_cache (
      pool_id TEXT NOT NULL, worker_id TEXT NOT NULL,
      quarantine_until TIMESTAMPTZ NOT NULL, reason TEXT,
      set_at TIMESTAMPTZ, client_id TEXT, fetched_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (pool_id, worker_id)
  );
  CREATE TABLE unclassified_logs (
      pool_id TEXT NOT NULL, task_id TEXT NOT NULL, run_id INT,
      worker_id TEXT NOT NULL, log_text TEXT NOT NULL,
      saved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (pool_id, task_id)
  );
  CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now());
  ```

- **`worker_health/pool_classifier_web/scripts/migrate.py`** — apply SQL files in order, skip already-applied versions
- **`worker_health/pool_classifier_web/docker-compose.yml`** — `postgres:16` for local dev
- **`tests/test_postgres_storage.py`** — parity tests against docker-compose Postgres

---

### ⬜ Phase 3 — Web layer (Flask)

Add the Flask app with multi-pool index and per-pool routes.

**To do:**

- **`worker_health/pool_classifier_web/pools.yaml`** — pool registry:

  ```yaml
  pools:
    - id: lambda-perf-a55
      provisioner: proj-autophone
      worker_type: gecko-t-lambda-perf-a55
      display_name: "Lambda Perf A55"
      schedule: "*/15 * * * *"
    - id: bitbar-gw-perf-a55
      ...
  ```

- **`worker_health/pool_classifier_web/registry.py`** — loads `pools.yaml`
- **`worker_health/pool_classifier_web/app.py`** — Flask routes:
  - `GET /healthz`
  - `GET /` — multi-pool summary index (Jinja template)
  - `GET /pools/<pool_id>` → `pc.render_html()`
  - `GET /pools/<pool_id>/overview.md` → `pc.render_md()`
  - `POST /classify/<pool_id>` — Cloud Scheduler hits this; calls `pc.classify_cycle()`, returns JSON summary
  - `GET /pools/<pool_id>/unclassified/<task_id>.log` — streams from `unclassified_logs` table
- **`worker_health/pool_classifier_web/templates/index.html`** — multi-pool summary page (dark theme, matching per-pool style)
- **`worker_health/pool_classifier_web/requirements.txt`** — pinned prod subset: `flask`, `gunicorn`, `requests`, `taskcluster`, `pyyaml`, `psycopg[binary]`, `sentry-sdk`

Local dev smoke test:
```sh
docker compose up -d postgres
python scripts/migrate.py
TC_TOKEN_FILE=~/.tc_token DATABASE_URL=postgresql://... flask --app worker_health.pool_classifier_web.app run -p 8080
curl -X POST localhost:8080/classify/lambda-perf-a55 | jq .
open http://localhost:8080/pools/lambda-perf-a55
```

---

### ⬜ Phase 4 — Terraform

Clone `~/git/hangar/terraform/` and adapt for pool-classifier. Flat root config, no modules.

**Files** (mirroring hangar, with noted deltas):

| File | Notes vs hangar |
|---|---|
| `main.tf` | Add `cloudscheduler.googleapis.com` API |
| `variables.tf` | `pools` variable (list of objects), `iap_authorized_members` default `["domain:mozilla.com"]` |
| `outputs.tf` | `populate_secrets_commands` for TC token |
| `network.tf` | Same: VPC, subnet, Serverless VPC Access connector, Service Networking peering |
| `sql.tf` | Postgres 16, REGIONAL HA, `db-g1-small`, private IP, SSL, PITR, deletion_protection |
| `secrets.tf` | Only 2 secrets: `pc-db-url` (TF-populated), `pc-tc-token` (manual) | <!-- pragma: allowlist secret -->
| `artifact_registry.tf` | Docker repo `pool-classifier`, keep-last-10 cleanup |
| `iam.tf` | Runtime SA `pool-classifier-run`, scheduler SA `pool-classifier-scheduler`; IAP binding to `var.iap_authorized_members` |
| `run.tf` | `cpu_idle=true` (no APScheduler — deviation from hangar); `timeout=1800s`; `ingress=INTERNAL_LOAD_BALANCER` |
| `lb.tf` | Same HTTPS LB + IAP + Cloud Armor; **add `/classify/*` path-matcher to non-IAP backend** for Scheduler OIDC |
| `armor.tf` | Verbatim from hangar |
| `scheduler.tf` | **New** — `for_each` over `var.pools`, one `google_cloud_scheduler_job` per pool, OIDC via scheduler SA, `attempt_deadline=1800s` |

---

### ⬜ Phase 5 — Containerize & deploy

**To do:**

- **`worker_health/Dockerfile`** — `python:3.11-slim`, install from `requirements.txt`, `pip install -e .`, gunicorn entrypoint
- **`worker_health/cloudbuild.yaml`** — build → push to Artifact Registry → `gcloud run deploy` (mirrors hangar pattern)
- `terraform apply` in sandbox project; `terraform plan` review
- Populate secrets: `gcloud secrets versions add pc-tc-token --data-file=~/.tc_token` <!-- pragma: allowlist secret -->
- Cloud Scheduler jobs auto-created by terraform from `pools.yaml` → `var.pools`
- IAP OAuth client prerequisite: confirm brand exists in target GCP project before apply

---

### ⬜ Phase 6 — Cutover

- Stop the long-running CLI on the current host
- Optional: one-shot history import — dump `pool_classifier.db` rows into Postgres with `pool_id='lambda-perf-a55'`
- Monitor Cloud Run logs for first few Scheduler ticks
- Decommission old SQLite results dirs

---

## Open Risks

| Risk | Mitigation |
|---|---|
| Long classify cycle exceeds 30-min timeout | Raise to 60 min (Cloud Run max); v2: split via Cloud Tasks per-worker |
| TC token rotation | Either redeploy on rotation, or re-read Secret Manager each cycle (~50ms) |
| IAP OAuth client prerequisite | Confirm GCP project has a brand before `terraform apply`; hangar's is pre-existing |
| Concurrent Scheduler retries double-counting | Postgres advisory lock at start of `classify_cycle()`: `pg_try_advisory_lock(hashtext('classify:'||pool_id))` → 409 on conflict |

---

## Verification (post-deploy)

```sh
# Dashboard (IAP-protected, browser)
open https://<domain>/

# Trigger one pool manually
gcloud scheduler jobs run pool-classifier-lambda-perf-a55 --location=us-west1

# Check logs
gcloud run services logs read pool-classifier --limit=200 | grep classify_cycle

# Inspect DB
gcloud sql connect <instance> --user=pool_classifier_app
SELECT pool_id, count(*) FROM task_results GROUP BY 1;
```

**Acceptance criteria:**
- Each pool in `pools.yaml` has a working `/pools/<id>` page
- 15-min Scheduler tick produces new `task_results` rows
- IAP blocks unauthenticated browsers; permits mozilla.com users
- `/classify/<id>` rejects browser requests; accepts Scheduler SA OIDC
- Cloud Armor rate-limits to 100 req/min/IP
