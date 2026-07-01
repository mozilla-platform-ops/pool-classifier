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
- Deployment follows the relops Terraform layout used by related services

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

### ✅ Phase 2 — Postgres adapter + migrations (DONE)

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
- **`worker_health/pool_classifier_web/docker-compose.yml`** — `postgres:16` for local dev (port 5433); data bind-mounted to `./pgdata/` on the host (not a named volume, so `docker compose down -v` won't destroy it)
- **`tests/test_postgres_storage.py`** — parity tests against docker-compose Postgres (skip unless `PC_TEST_DATABASE_URL` set)

**Changes made:**

- **`worker_health/pool_classifier_web/storage.py`** — `PostgresStorage` class appended; `_PgLogRef` unlink helper; `_to_iso()` helper; psycopg lazy import (graceful ImportError if not installed)
- **`worker_health/pool_classifier_web/migrations/001_init.sql`** — Postgres schema (5 tables, 3 indexes)
- **`worker_health/pool_classifier_web/scripts/__init__.py`** — empty package marker
- **`worker_health/pool_classifier_web/scripts/migrate.py`** — migration runner (`apply_migrations(dsn)` + `__main__` entrypoint)
- **`worker_health/pool_classifier_web/docker-compose.yml`** — postgres:16, port 5433
- **`tests/test_postgres_storage.py`** — parity tests (skip when `PC_TEST_DATABASE_URL` unset)
- **`Pipfile`** — added `psycopg = {extras = ["binary"], version = "*"}`

**Note:** `save_unclassified_log` / `list_unclassified_logs` use the `unclassified_logs` DB table (not filesystem). `list_unclassified_logs` yields a `_PgLogRef` as the third element; its `.unlink()` deletes the DB row. All existing tests pass unchanged.

**Testing:**

```sh
# 1. Existing tests (no Docker needed)
pipenv run pytest tests/ --ignore=tests/test_runner.py -x -q

# 2. Start Postgres and apply migrations
docker compose -f worker_health/pool_classifier_web/docker-compose.yml up -d postgres
docker compose -f worker_health/pool_classifier_web/docker-compose.yml run --rm migrate

# 3. Run parity tests
export PC_TEST_DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier  # pragma: allowlist secret
pipenv run pytest tests/test_postgres_storage.py -v
```

---

### ✅ Phase 3 — Web layer (Flask) (DONE)

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

**Changes made:**

- **`worker_health/pool_classifier_web/pools.yaml`** — pool registry; all `proj-autophone` and `releng-hardware` pools registered; supports `enabled: false` + `reason:` fields to suppress a pool without removing it
- **`worker_health/pool_classifier_web/registry.py`** — `Pool` dataclass (`enabled`, `reason` optional fields), `all_pools()` (enabled only), `all_pools_including_disabled()`, `get_pool()`, `detect_os()` (provisioner-first heuristic: autophone→android, then name-based macOS/Windows/Linux); cached at import from `POOLS_FILE` env or package-relative `pools.yaml`
- **`worker_health/pool_classifier_web/app.py`** — Flask app factory `create_app()` with all 6 routes; module-level `_classifiers` cache; startup warning if TC creds absent; index passes per-pool OS, errors-per-host and success-rate for 1h and 24h windows; disabled pools shown greyed-out with reason page
- **`worker_health/pool_classifier_web/templates/index.html`** — dark-theme Jinja template: banner, per-pool table with OS, alerting count, errors/host (1h/24h), success rate (1h/24h), oldest data timestamp; all columns sortable
- **`worker_health/pool_classifier_web/requirements.txt`** — prod subset for Docker image
- **`worker_health/pool_classifier_web/storage.py`** — `ClassifyLockBusy` exception; `classify_lock()` context manager on both `SqliteStorage` (no-op) and `PostgresStorage` (`pg_try_advisory_lock` on a separate connection, released on context exit)
- **`worker_health/pool_classifier.py`** — TC init made lazy: `_init_tc()` wrapped in try/except at `__init__`, `_ensure_tc()` added, `_list_workers()` calls it; `classify_cycle()` body wrapped with `with self.storage.classify_lock():`; `ClassifyLockBusy` imported; `render_html(os_label="")` passes OS badge to `_write_html()` for display in page header
- **`Pipfile`** — added `flask`, `gunicorn`
- **`tests/test_web_app.py`** — Flask test client tests (skip without `PC_TEST_DATABASE_URL`): healthz, index, pool HTML, 404, lock conflict → 409, unclassified log found/missing

**All 19 existing tests pass unchanged.**

Local dev smoke test:
```sh
docker compose -f worker_health/pool_classifier_web/docker-compose.yml up -d postgres
docker compose -f worker_health/pool_classifier_web/docker-compose.yml run --rm migrate
export TC_TOKEN_FILE=~/.tc_token
export DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier  # pragma: allowlist secret
pipenv run flask --app worker_health.pool_classifier_web.app:create_app run -p 8080
curl -sf localhost:8080/healthz
curl -sf -X POST localhost:8080/classify/proj-autophone/gecko-t-lambda-perf-a55 | jq .
open http://localhost:8080/pools/proj-autophone/gecko-t-lambda-perf-a55
open http://localhost:8080/

# Trigger all pools at once (local dev only):
bash pc_fetch_data.sh
```

---

### ✅ Phase 4 — Terraform (DONE)

Terraform config lives under `worker_health/pool_classifier_web/terraform/`.

**Changes made:**

- **`main.tf`** — providers, required APIs incl. `cloudscheduler.googleapis.com`, `data.google_project`
- **`variables.tf`** — `project_id`, `region` (default `us-west1`), `domain`, `db_password`, IAP OAuth client id/secret, `iap_authorized_members` (default `["domain:mozilla.com"]`), `cloud_run_min_instances` (default 0 — Scheduler wakes the service), `cloud_run_max_instances`, `cloud_run_image`, `pools` (list of `{id, provisioner, worker_type, schedule}`), `scheduler_attempt_deadline` (default `1800s`)
- **`outputs.tf`** — `load_balancer_ip`, `artifact_registry_hostname`, `cloud_run_url`, `db_private_ip` (sensitive), `populate_secrets_commands`
- **`network.tf`** — VPC, subnet (`10.9.0.0/24`), Service Networking peering, Serverless VPC Access connector
- **`sql.tf`** — Postgres 16, ENTERPRISE edition, `db-g1-small` ZONAL, private IP, SSL-only, PITR, `deletion_protection_enabled = true`; db `pool_classifier`, user `pc`. (Shared-core `db-g1-small` requires ENTERPRISE — not ENTERPRISE_PLUS — and can't do HA, so ZONAL. Acceptable: data re-derives every 15 min, plus 7-day backups + PITR. To add HA later: `edition = ENTERPRISE`, `availability_type = REGIONAL`, and a dedicated tier e.g. `db-custom-1-3840`.)
- **`secrets.tf`** — two secrets: `pc-db-url` (TF-populated from SQL private IP + `db_password`), `pc-tc-token` (manual `gcloud secrets versions add`) <!-- pragma: allowlist secret -->
- **`artifact_registry.tf`** — Docker repo `pool-classifier`, keep-last-10 cleanup
- **`iam.tf`** — runtime SA `pool-classifier-run` (secret accessor, cloudsql.client, log writer, AR reader); scheduler SA `pool-classifier-scheduler` (`roles/run.invoker` on the Cloud Run service); Cloud Build SA roles; IAP binding on the default backend to `var.iap_authorized_members`
- **`run.tf`** — Cloud Run v2; `ingress = INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER`; `timeout = 1800s`; `cpu_idle = true` (no in-process scheduler); env: `DATABASE_URL`, `TC_TOKEN_JSON`, `TC_ROOT_URL`, `POOLS_FILE`, `LOG_JSON`
- **`armor.tf`** — Cloud Armor: 100 req/min/IP throttle + OWASP XSS/SQLi/RFI rules + default allow
- **`lb.tf`** — global IP, managed SSL cert, serverless NEG, **two backends sharing the NEG**: `pc` (IAP-protected default) and `pc_classify` (no IAP); URL map `path_matcher` routes `/classify/*` → `pc_classify`, everything else → `pc`; HTTPS forwarding rule + HTTP→HTTPS redirect
- **`scheduler.tf`** — `for_each` over `var.pools`, one `google_cloud_scheduler_job` per pool, POST to `https://${domain}/classify/${provisioner}/${worker_type}`, OIDC via scheduler SA, `attempt_deadline = var.scheduler_attempt_deadline`
- **`terraform.tfvars.example`** — example values + all enabled `pools.yaml` entries pre-populated

**Notes:**

- Scheduler hits the LB (not the Cloud Run URL directly) — Cloud Run ingress is `INTERNAL_LOAD_BALANCER`. The `/classify/*` URL-map path-matcher routes to a no-IAP backend pointed at the same NEG. The app validates the OIDC bearer to keep `/classify/*` honest.
- `cloud_run_min_instances = 0` is fine: classify cycles are infrequent and tolerate cold start. Bump to 1 if oldest-data lag becomes a concern.
- Keep `terraform.tfvars` `pools` in sync with `pools.yaml`. Disabled pools (`enabled: false`) should NOT appear in `var.pools` — they have no Scheduler job.

**Files in original spec (kept for reference):**

| File | Notes |
|---|---|
| `main.tf` | Add `cloudscheduler.googleapis.com` API |
| `variables.tf` | `pools` variable (list of objects), `iap_authorized_members` default `["domain:mozilla.com"]` |
| `outputs.tf` | `populate_secrets_commands` for TC token |
| `network.tf` | VPC, subnet, Serverless VPC Access connector, Service Networking peering |
| `sql.tf` | Postgres 16, ENTERPRISE edition, `db-g1-small` ZONAL (shared-core has no HA), private IP, SSL, PITR, deletion_protection |
| `secrets.tf` | Only 2 secrets: `pc-db-url` (TF-populated), `pc-tc-token` (manual) | <!-- pragma: allowlist secret -->
| `artifact_registry.tf` | Docker repo `pool-classifier`, keep-last-10 cleanup |
| `iam.tf` | Runtime SA `pool-classifier-run`, scheduler SA `pool-classifier-scheduler`; IAP binding to `var.iap_authorized_members` |
| `run.tf` | `cpu_idle=true` (no APScheduler); `timeout=1800s`; `ingress=INTERNAL_LOAD_BALANCER` |
| `lb.tf` | HTTPS LB + IAP + Cloud Armor; **add `/classify/*` path-matcher to non-IAP backend** for Scheduler OIDC |
| `armor.tf` | Cloud Armor policy |
| `scheduler.tf` | **New** — `for_each` over `var.pools`, one `google_cloud_scheduler_job` per pool, OIDC via scheduler SA, `attempt_deadline=1800s` |

---

### ✅ Phase 5 — Containerize & deploy (DONE — deployed, IAP working)

**Done so far — OIDC validation on `/classify/*`:**

The `/classify/*` URL path bypasses IAP at the LB so Cloud Scheduler can reach it. Without app-level checks the endpoint would be open to the internet (rate-limited, but open). Added Google OIDC validation that mirrors what Scheduler already signs in `scheduler.tf`.

- **`worker_health/pool_classifier_web/auth.py`** — new. `require_scheduler_oidc` decorator: reads `Authorization: Bearer <jwt>`, calls `google.oauth2.id_token.verify_oauth2_token` with `aud = CLASSIFY_OIDC_AUDIENCE`, then optionally checks `email == CLASSIFY_OIDC_SA_EMAIL`. No-op when `CLASSIFY_OIDC_AUDIENCE` is unset (local dev).
- **`worker_health/pool_classifier_web/app.py`** — `/classify/<provisioner>/<worker_type>` wrapped with `@require_scheduler_oidc`.
- **`worker_health/pool_classifier_web/requirements.txt`** + **`Pipfile`** — added `google-auth`.
- **`worker_health/pool_classifier_web/terraform/run.tf`** — env vars `CLASSIFY_OIDC_AUDIENCE = "https://${var.domain}/"` and `CLASSIFY_OIDC_SA_EMAIL = google_service_account.pc_scheduler.email`. Audience matches the `oidc_token.audience` in `scheduler.tf`.
- **`tests/test_web_app.py`** — added `test_classify_missing_oidc_returns_401` and `test_classify_invalid_oidc_returns_401`. All 9 web tests pass.

**Done so far — container image + build pipeline:**

- **`Dockerfile`** — `python:3.11-slim`; installs `requirements.txt` then `pip install --no-deps -e .` (editable install keeps `pools.yaml`/`patterns.yaml`/`migrations/*.sql`/`templates/` on disk); non-root `app` user (uid 10001); `HEALTHCHECK` hits `/healthz`; `CMD` runs `docker-entrypoint.sh`. Build context is the repository root (`/app`), matching `POOLS_FILE=/app/worker_health/pool_classifier_web/pools.yaml` in `run.tf`.
- **`docker-entrypoint.sh`** — new. Applies migrations (idempotent; toggle with `RUN_MIGRATIONS`, default `true`), then `exec gunicorn` on `$PORT` (default 8080) with `--timeout 1800` to match the Cloud Run request timeout, `--workers 2 --threads 8` for the I/O-bound classify work. Serves app factory `worker_health.pool_classifier_web.app:create_app()`.
- **`.dockerignore`** — keeps the context lean: excludes `pgdata/`, `pool_classifier_results/`, `ur_*`/`sr_*` run dirs, caches, tests, docs (re-includes `README.md`, which `setup.py` reads at install time).
- **`cloudbuild.yaml`** — build → push to Artifact Registry (`$_REGION-docker.pkg.dev/$PROJECT_ID/pool-classifier/app`, tags `$COMMIT_SHA` + `latest`, `--cache-from latest`) → `gcloud run deploy pool-classifier`. `_REGION` default `us-west1` (matches terraform). Submit from the repository root: `gcloud builds submit --config cloudbuild.yaml .`

Locally verified: image builds; gunicorn boots and `/healthz` returns `ok` with no DB; the entrypoint migration step connects to the compose Postgres and applies/skips migrations.

**Done so far — GCP project + IAP rework:**

- **GCP project `relops-pool-classifier`** created (project number `410047876591`) under the `relops-dashboard`/Hangar folder (`723902893592`), billing account `01E7D5-97288E-E2EBA0`.
- **`terraform.tfvars`** created from the example (gitignored) — real project id, region, domain, generated URL-safe `db_password`, all 38 enabled pools.
- **IAP switched to a Google-managed OAuth client.** The legacy IAP OAuth Admin APIs (custom brands/clients) were shut down (Mar 2026) and are unavailable to new projects — `gcloud iap oauth-brands create` now errors. Reworked:
  - `lb.tf` — `iap { enabled = true }` (no `oauth2_client_id`/`secret`)
  - `main.tf` — google/google-beta providers bumped `~> 5.0` → `~> 6.0` (the `enabled` flag / managed-client support requires provider ≥ 6.0; now on 6.50.0)
  - `variables.tf` + `terraform.tfvars`(.example) — `iap_oauth2_client_id`/`secret` variables removed
  - `terraform validate` passes; `fmt` clean. **This removes the old IAP brand prerequisite entirely.**

**Done — deployed and running:**

- Billing linked; `terraform apply` succeeded (85 resources).
- **GCS state backend** in a shared state project: bucket `gs://moz-relops-tf-state` (project `relops-terraform-state`, folder `723902893592`), `prefix = "pool-classifier"`. Versioned, UBLA, public-access-prevented.
- Cloud SQL: **ENTERPRISE edition, `db-g1-small`, ZONAL** (shared-core can't do HA; edition must be ENTERPRISE not ENTERPRISE_PLUS). See sql.tf note for how to add HA later.
- Secrets: `pc-db-url` populated by terraform; `pc-tc-token` populated manually from `~/.tc_token` (JSON `{clientId, accessToken}`). <!-- pragma: allowlist secret -->
- **App image built + deployed** via `cloudbuild.yaml`. Cloud Build runs as the **Compute Engine default SA** on new projects (legacy `@cloudbuild.gserviceaccount.com` no longer provisioned) — iam.tf grants it `builds.builder`/`run.admin`/`serviceAccountUser`/`artifactregistry.writer`. `.gcloudignore` keeps the source upload small (excludes pgdata, .terraform, run dirs). `cloudbuild.yaml` uses a user-defined `$_TAG` (not `$COMMIT_SHA`, which is empty for manual `builds submit`). run.tf has `lifecycle { ignore_changes = [image, client, client_version] }` so apply doesn't revert deploys.
- Startup verified in logs: migrations applied to Cloud SQL (`001_init: applied`), gunicorn healthy, STARTUP probe passed.
- **DNS** `pool-classifier.relops.mozilla.com → 34.107.179.124` (LB IP); **managed SSL cert ACTIVE**.
- **IAP authentication works** — after the manual OAuth consent-screen step (below), the deny page shows `gcp-iap-mode=AUTHENTICATING` and resolves `aerickson@mozilla.com`.

**Manual one-time bootstrap (NOT terraformable):**

- **OAuth consent screen** ("Google Auth Platform" → *Get started*). There is **no IaC path** — the IAP OAuth Admin API that backed `google_iap_brand` was shut down (Mar 2026). Configure: app name, support email, **User type = External**, **Publishing status = In production**. (Internal scopes to the `firefox.gcp.mozilla.com` org and excludes `@mozilla.com` logins, which are a separate Cloud Identity; External + In Production is required so `@mozilla.com` accounts can authenticate. IAP IAM still gates actual access. No Google verification needed — IAP uses only basic scopes.)

**✅ IAP working — dashboard reachable as `@mozilla.com`.**

The blocker was **not** an org-level PAB (red herring — see the long diagnostic trail in git history). It was **two IAP-in-front-of-Cloud-Run config gaps** missing from the terraform, both now fixed:

1. **IAP service agent + `run.invoker` (the critical one).** IAP invokes Cloud Run *on behalf of* the authenticated user, so the IAP service agent must be provisioned and hold `roles/run.invoker` on the service. Missing → "The IAP service account is not provisioned" then a Cloud Run `403 Forbidden`. Fixed in `run.tf`:
   - `google_project_service_identity.iap` (provider `google-beta`) provisions `service-<num>@gcp-sa-iap.iam.gserviceaccount.com`
   - `google_cloud_run_v2_service_iam_member.iap_invoker` grants it `run.invoker`
   - Ref: <https://cloud.google.com/iap/docs/enabling-cloud-run>
2. **Explicit OAuth client instead of Google-managed.** The managed client (`iap { enabled = true }` only) stuck at "You don't have access"; a manually-created OAuth client (Console → Google Auth Platform → Clients → Web app; redirect URI `https://iap.googleapis.com/v1/oauth/clientIds/<id>:handleRedirect`) matched hangar and let the flow proceed. `lb.tf` now passes `oauth2_client_id`/`secret` (vars in `variables.tf`, values in gitignored `terraform.tfvars`).

`domain:mozilla.com` in `iap_authorized_members` works as the durable principal — no `user:`/group needed. The org-admin escalation (`RELOPS-2435`) is **not** required; can be closed.

Diagnostics that ruled out the org-PAB theory (kept for reference): no Domain Restricted Sharing anywhere; no IAM deny policy (project/folder `{}`); org PAB/ACM lists were `PERMISSION_DENIED` — unreadable, but ultimately irrelevant.

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
| IAP OAuth client (legacy brand/client APIs shut down Mar 2026) | Resolved — use the Google-managed OAuth client (`iap { enabled = true }`, provider ≥ 6.0); no brand/client to pre-create |
| OAuth consent screen + OAuth client not terraformable | Manual one-time console bootstrap: Google Auth Platform → consent screen (External / In production) + create a Web OAuth client. IAP OAuth Admin API is shut down. Documented in Phase 5. |
| IAP-on-Cloud-Run needs the IAP service agent as invoker | Resolved — `run.tf` provisions `google_project_service_identity.iap` + grants it `run.invoker`. Easy to miss; was the real cause of the "You don't have access" saga (not an org PAB). |
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
