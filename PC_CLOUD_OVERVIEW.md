# Pool Classifier — Cloud Deployment Overview

Operator reference for the Pool Classifier running on Google Cloud Run. For the
migration history and design rationale, see `PC_CLOUD_RUN_MIGRATION.md`. For
local dev, see `POOL_CLASSIFIER.md`.

## Key identifiers

| Thing | Value |
|---|---|
| GCP project | `relops-pool-classifier` (number `410047876591`) |
| Org / folder | `firefox.gcp.mozilla.com` / `723902893592` (the "Hangar"/relops folder) |
| Region | `us-west1` |
| Domain | `pool-classifier.relops.mozilla.com` |
| Load balancer IP | `34.107.179.124` (A record target) |
| Cloud Run service | `pool-classifier` |
| Cloud Run URL (LB-only ingress) | `https://pool-classifier-gpn5d6lwgq-uw.a.run.app` |
| Cloud SQL instance | `pool-classifier-db` (Postgres 16; db `pool_classifier`, user `pc`) |
| Artifact Registry image | `us-west1-docker.pkg.dev/relops-pool-classifier/pool-classifier/app` |
| Scheduler job | `pool-classifier-classify-all` (every 15 min) |
| Secrets | `pc-db-url` (TF-populated), `pc-tc-token` (manual) |
| Terraform state | `gs://moz-relops-tf-state`, prefix `pool-classifier` (project `relops-terraform-state`) |

Service accounts:
- Runtime: `pool-classifier-run@relops-pool-classifier.iam.gserviceaccount.com`
- Scheduler: `pool-classifier-scheduler@relops-pool-classifier.iam.gserviceaccount.com`
- IAP agent: `service-410047876591@gcp-sa-iap.iam.gserviceaccount.com`
- Cloud Build: `410047876591-compute@developer.gserviceaccount.com` (Compute default SA)

## Architecture

```
                        pool-classifier.relops.mozilla.com → 34.107.179.124
                                            │
                          ┌─────────────────▼─────────────────┐
 Browser (mozilla.com) ──►│ HTTPS LB + Cloud Armor + managed SSL│
                          │  URL map (pool-classifier-url-map): │
                          │   /classify*  → classify-backend ───┼──► (no IAP, OIDC only)
                          │   everything  → backend (IAP) ──────┼──► (IAP: @mozilla.com)
 Cloud Scheduler ────────►│                                     │
  (classify-all, OIDC)    └─────────────────┬───────────────────┘
                                            │ (both backends share one serverless NEG)
                                  ┌─────────▼──────────┐
                                  │ Cloud Run          │──► Cloud SQL (private IP, VPC connector)
                                  │ pool-classifier    │──► Secret Manager (pc-db-url, pc-tc-token)
                                  │ ingress=INTERNAL_LB│──► Taskcluster API (public egress)
                                  └────────────────────┘
```

**Two LB backends share one Cloud Run service via one NEG:**
- `pool-classifier-backend` — **IAP-protected** (browsers); serves the dashboard.
- `pool-classifier-classify-backend` — **no IAP**; serves `/classify*` for Cloud
  Scheduler. Protected instead by app-level OIDC (`auth.py`) + Cloud Run's own
  OIDC check (scheduler SA has `run.invoker`; `custom_audiences` lets Cloud Run
  accept the LB-domain audience).

**Auth, two paths:**
- *Dashboard:* IAP authenticates the Google user (explicit OAuth client +
  provisioned IAP service agent) → checks `roles/iap.httpsResourceAccessor`
  (`domain:mozilla.com`) → IAP invokes Cloud Run as the IAP service agent.
- *Scheduler:* signs an OIDC token (audience = `https://<domain>/`, scheduler
  SA) → Cloud Run validates it (needs `custom_audiences` + `run.invoker`) →
  `auth.py` re-validates the same token.

**Classify flow:** one Scheduler job → `POST /classify-all` → the app reads
`pools.yaml` (`registry.all_pools()`) and classifies every enabled pool
**sequentially** (proj-autophone first). `pools.yaml` is the single source of
truth — there is no terraform pool list.

## HTTP endpoints

| Method + path | Auth | Purpose |
|---|---|---|
| `GET /healthz` | none | Liveness (behind IAP at the LB) |
| `GET /` | IAP | Multi-pool dashboard |
| `GET /pools/<prov>/<wt>` | IAP | Per-pool page |
| `GET /pools/<prov>/<wt>/overview.md` | IAP | Markdown report |
| `GET /pools/<prov>/<wt>/unclassified/<task>.log` | IAP | Streamed unclassified log |
| `POST /classify/<prov>/<wt>` | OIDC | Classify one pool (manual / `pc_fetch_data.sh`) |
| `POST /classify-all` | OIDC | Sequential classify of all enabled pools (Scheduler) |

## Deploying

**Code change → rebuild + redeploy the image** (Cloud Build builds the
Dockerfile, pushes to Artifact Registry, `gcloud run deploy`):

```bash
cd worker_health   # project dir = build context
gcloud builds submit --config cloudbuild.yaml \
  --substitutions=_TAG=$(git rev-parse --short HEAD) \
  --project=relops-pool-classifier .
```
> Uses `_TAG` (not `$COMMIT_SHA`, which is empty for manual submits). Terraform's
> `lifecycle.ignore_changes` keeps `apply` from reverting the deployed image.

**Infra change → terraform:**

```bash
cd worker_health/pool_classifier_web/terraform
terraform plan      # review
terraform apply
```
> Secrets live in the gitignored `terraform.tfvars` (db_password, IAP client
> id/secret). If apply/init errors with a reauth/`invalid_rapt` message, refresh
> Application Default Credentials: `gcloud auth application-default login`.

## Debugging / operations

### Logs (Cloud Logging)
```bash
# Tail recent app + request logs (most recent first)
gcloud logging read \
  'resource.type=cloud_run_revision AND resource.labels.service_name=pool-classifier' \
  --project=relops-pool-classifier --limit=50 --freshness=30m \
  --order=desc --format="value(timestamp, textPayload)"

# Classify request statuses (200 good; 401 auth; 500 app error)
gcloud logging read \
  'resource.type=cloud_run_revision AND resource.labels.service_name=pool-classifier AND httpRequest.requestUrl=~"/classify"' \
  --project=relops-pool-classifier --limit=20 --freshness=1h \
  --format="table(timestamp, httpRequest.status, httpRequest.latency, httpRequest.requestUrl)"

# Errors/warnings only (tracebacks)
gcloud logging read \
  'resource.type=cloud_run_revision AND resource.labels.service_name=pool-classifier AND severity>=WARNING' \
  --project=relops-pool-classifier --limit=30 --freshness=1h \
  --format="value(timestamp, severity, textPayload)"
```

### Cloud Scheduler
```bash
gcloud scheduler jobs list --location=us-west1 --project=relops-pool-classifier
# Last attempt result (status: {} == success)
gcloud scheduler jobs describe pool-classifier-classify-all \
  --location=us-west1 --project=relops-pool-classifier \
  --format="yaml(state, lastAttemptTime, status, scheduleTime)"
# Trigger a run now (the sweep runs async; watch logs)
gcloud scheduler jobs run pool-classifier-classify-all \
  --location=us-west1 --project=relops-pool-classifier
```

### Cloud Run
```bash
gcloud run services describe pool-classifier --region=us-west1 \
  --project=relops-pool-classifier \
  --format="value(status.latestReadyRevisionName, status.url)"
# Current deployed image
gcloud run services describe pool-classifier --region=us-west1 \
  --project=relops-pool-classifier \
  --format="value(spec.template.spec.containers[0].image)"
# Change scaling (it's in lifecycle ignore_changes, so edit here, not terraform)
gcloud run services update pool-classifier --region=us-west1 \
  --project=relops-pool-classifier --min-instances=0 --max-instances=2
```

### Cloud Build
```bash
gcloud builds list --project=relops-pool-classifier --limit=5 \
  --format="table(id, status, createTime, duration)"
gcloud builds log <BUILD_ID> --project=relops-pool-classifier
```

### Cloud SQL
```bash
# Connect (opens a temporary public-IP path; or use Cloud SQL Auth Proxy)
gcloud sql connect pool-classifier-db --user=pc --project=relops-pool-classifier
# Useful checks once connected (psql):
#   SELECT pool_id, count(*), max(classified_at) FROM task_results GROUP BY 1 ORDER BY 3 DESC;
#   SHOW max_connections;
#   SELECT count(*) FROM pg_stat_activity;   -- connection pressure
```

### IAP / SSL / DNS
```bash
# IAP members on the dashboard backend
gcloud iap web get-iam-policy --resource-type=backend-services \
  --service=pool-classifier-backend --project=relops-pool-classifier
# Managed cert status (want managed.status=ACTIVE)
gcloud compute ssl-certificates list --project=relops-pool-classifier \
  --format="table(name, managed.status, managed.domainStatus)"
# DNS resolves to the LB IP?
dig +short pool-classifier.relops.mozilla.com A
```

### Secrets
```bash
gcloud secrets versions list pc-tc-token --project=relops-pool-classifier
# Rotate the Taskcluster token (JSON: {"clientId":"...","accessToken":"..."})
gcloud secrets versions add pc-tc-token --data-file=$HOME/.tc_token \
  --project=relops-pool-classifier
```

## Gotchas / non-obvious requirements

These bit us during the migration; documented so they don't again.

- **IAP + Cloud Run needs the IAP service agent as invoker.** Provision the IAP
  service identity and grant it `run.invoker` (terraform: `run.tf`). Symptom:
  "The IAP service account is not provisioned" / Cloud Run 403.
- **Scheduler OIDC needs `custom_audiences` on Cloud Run.** Scheduler signs the
  token with the LB-domain audience; Cloud Run rejects it (`401 access token
  could not be verified`) unless the domain is in `custom_audiences` (`run.tf`).
- **OAuth consent screen + OAuth client are manual.** The IAP OAuth Admin API was
  shut down (Mar 2026); there is no terraform for these. Create them in Console
  (Google Auth Platform): consent screen **External / In production**, plus a Web
  OAuth client whose redirect URI is
  `https://iap.googleapis.com/v1/oauth/clientIds/<id>:handleRedirect`. Put the
  client id/secret in `terraform.tfvars`.
- **`@mozilla.com` is a separate Cloud Identity from the `firefox.gcp.mozilla.com`
  org.** `domain:mozilla.com` works as the IAP principal; "Internal" consent
  audience does NOT (it scopes to the org).
- **Cloud Build runs as the Compute Engine default SA** on this (post-2024)
  project, not the legacy `@cloudbuild` SA. It has builder/run.admin/AR-writer/
  serviceAccountUser (`iam.tf`).
- **`db-g1-small` has a low connection limit.** The app holds a persistent DB
  connection per pool per gunicorn worker. Mitigations in place: `max_connections=100`,
  `GUNICORN_WORKERS=1`, `cloud_run_max_instances=2`, and the single sequential
  `/classify-all` job. Symptom if exceeded: "remaining connection slots are
  reserved for roles with privileges of pg_use_reserved_connections".
- **Cloud Run scaling drift.** `gcloud run deploy` re-stamps the scaling block, so
  it's in `lifecycle.ignore_changes`; change scaling via `gcloud run services
  update`, not terraform.
- **ADC reauth.** Terraform uses Application Default Credentials; Mozilla enforces
  periodic reauth. `gcloud auth application-default login` when it expires.
- **`.gcloudignore`** keeps the Cloud Build source upload small (excludes
  `pgdata/`, `.terraform/`, run dirs) — separate from `.dockerignore`.
