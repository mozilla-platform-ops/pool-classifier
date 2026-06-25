# Pool Classifier Repo Extraction Plan

Target repo name: `pool-classifier`

Goal: move the Cloud Run Pool Classifier service out of `android-tools/worker_health`
into its own repository with minimal behavior change, then clean up dependencies,
packaging, and app structure in follow-up work.

## Why Extract

- Pool Classifier now has its own production service, database, Terraform,
  Cloud Build deploy flow, scheduler, runbooks, and tests.
- Its deploy cadence is separate from the rest of `android-tools`.
- Unrelated commits can accidentally ride along in production deploys.
- Runtime packaging already differs from local development (`requirements.txt`
  for the container, `Pipfile` for local work).
- The web app and operational docs have become a standalone product surface.

## Principles

- Preserve behavior first; refactor after the service is proven in the new repo.
- Keep the package/module names initially unless changing them is required.
- Keep production deploy verification as the release gate.
- Prefer a history-preserving extraction if practical, but do not let perfect
  history block the move.
- Avoid mixing the extraction with HTML/template rewrites, DB query rewrites, or
  new health endpoints.

## Candidate Contents

Move these into `pool-classifier`:

- `worker_health/pool_classifier.py`
- `worker_health/pool_classifier_web/`
- `tests/test_postgres_storage.py`
- `tests/test_postgres_pooling.py`
- `tests/test_pool_classifier_web_routes.py`
- `tests/test_web_app.py`
- `Dockerfile`
- `cloudbuild.yaml`
- `docker-entrypoint.sh`
- `PC_CLOUD_OVERVIEW.md`
- `PC_CLOUD_RUN_MIGRATION.md`
- `PC_CLOUD_TODOS.md`
- `POOL_CLASSIFIER.md`
- Any small helper scripts that are truly Pool Classifier specific.

Leave behind or evaluate separately:

- Generic `worker_health` worker/taskcluster utilities.
- Runner/safe-runner scripts and their tests.
- Android device scripts unrelated to Pool Classifier.
- Ad hoc local notes unless they are useful operational docs.

## Proposed New Repo Layout

Initial low-risk layout:

```text
pool-classifier/
  Dockerfile
  cloudbuild.yaml
  docker-entrypoint.sh
  README.md
  docs/
    PC_CLOUD_OVERVIEW.md
    PC_CLOUD_RUN_MIGRATION.md
    PC_CLOUD_TODOS.md
    POOL_CLASSIFIER.md
  worker_health/
    __init__.py
    pool_classifier.py
    pool_classifier_web/
      ...
  tests/
    ...
```

Follow-up layout after extraction is stable:

```text
pool-classifier/
  src/pool_classifier/
  tests/
  docs/
  infra/terraform/
```

Do not do the package rename in the first extraction unless the import surface
forces it.

## Extraction Steps

1. **Inventory dependencies**
   - List all imports used by `pool_classifier.py` and `pool_classifier_web`.
   - Identify any imports from sibling `worker_health` modules.
   - Decide whether to copy those modules, vendor a small helper, or replace the
     dependency.

2. **Create the new repo**
   - Create GitHub repo `pool-classifier`.
   - Decide history strategy:
     - Preferred: use `git filter-repo` or equivalent to preserve history for
       Pool Classifier paths.
     - Fallback: clean initial import with a note pointing to the source repo.

3. **Move files with minimal code changes**
   - Keep `worker_health.pool_classifier` imports working initially.
   - Keep `Dockerfile`, `cloudbuild.yaml`, and Terraform paths aligned with the
     initial layout.
   - Preserve `POOLS_FILE=/app/worker_health/pool_classifier_web/pools.yaml`
     for the first deploy unless changing it is trivial and tested.

4. **Unify dependency management**
   - Pick one source of truth for runtime and local dependencies.
   - Recommended: modern Python packaging with `pyproject.toml` plus a lockfile,
     then generate or install container runtime deps from that.
   - Acceptance: adding a runtime dependency in one place must be enough for
     local tests and the Cloud Run image.

5. **Rebuild local development workflow**
   - Add a focused `README.md` with setup, test, local run, and deploy commands.
   - Keep `PC_CLOUD_OVERVIEW.md` as the operator runbook.
   - Ensure `pipenv run pytest` equivalent works in the new repo.

6. **Update Cloud Build / deploy references**
   - Confirm build context is the new repo root.
   - Confirm image remains:
     `us-west1-docker.pkg.dev/relops-pool-classifier/pool-classifier/app`
   - Confirm Cloud Run service remains `pool-classifier`.
   - Update docs to point at the new repo.

7. **Validate locally**
   - Run full tests.
   - Run container build locally if feasible.
   - Confirm static assets, templates, migrations, `pools.yaml`, and
     `patterns.yaml` are included in the image.

8. **Deploy from the new repo**
   - Deploy a no-op or docs-only plus version marker change from `pool-classifier`.
   - Verify:
     - Cloud Build success.
     - Cloud Run latest revision image tag matches the new repo commit.
     - Recent Cloud Run warnings/errors are clean.
     - `/classify-all` can be triggered and emits summary logs.

9. **Freeze old path**
   - Add a note in the old `android-tools/worker_health` tree pointing to
     `pool-classifier`.
   - Remove or archive old Pool Classifier files only after production has run
     successfully from the new repo.

## Acceptance Criteria

- New `pool-classifier` repo can run the full Pool Classifier test suite.
- Cloud Build deploy from the new repo succeeds.
- Cloud Run serves a revision built from the new repo.
- Scheduler-triggered `/classify-all` completes without app errors.
- Operator docs in the new repo are sufficient to deploy, debug logs, trigger
  scheduler, and inspect Cloud Run state.
- Old repo has a clear pointer to the new owner/location.

## Risks

- Hidden imports from other `worker_health` modules may make the initial move
  larger than expected.
- Data files may be missed by the Docker build or package config.
- Terraform paths may assume the old directory layout.
- Cloud Build IAM/source assumptions may need updates if moving to a different
  GitHub project or trigger model.
- A package rename during extraction would increase risk; defer it.

## Follow-Up Work After Extraction

- Move per-pool generated HTML into Jinja templates.
- Add `/readyz` or `/debug/health`.
- Optimize `query_workers()` category aggregation.
- Rename package from `worker_health` to `pool_classifier`.
- Convert Terraform folder to `infra/terraform`.
- Add CI specific to this service.
