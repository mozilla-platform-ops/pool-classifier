# Pool Classifier Cloud TODOs

Follow-ups noticed while deploying and operating the Cloud Run pool classifier.

## Next

- [ ] Unify or enforce runtime dependency sources.
  - Cloud Run installs from `worker_health/pool_classifier_web/requirements.txt`.
  - Local development uses `Pipfile` / `Pipfile.lock`.
  - We already missed `psycopg-pool` in the image once because these are separate.
- [ ] Add a readiness/debug health endpoint.
  - Keep `/healthz` cheap for liveness.
  - Add `/readyz` or `/debug/health` for DB pool checkout, migration availability,
    and required secret/config presence.
- [ ] Move generated per-pool HTML out of `pool_classifier.py`.
  - A Jinja template would make layout/favicon/nav changes easier and safer.
  - It would also reduce escaping risks in hand-built HTML strings.
- [ ] Optimize per-pool worker detail queries if pages get slow.
  - `query_workers()` currently fetches workers and then category counts per worker.
  - A grouped query can replace the per-worker category query loop.

## Recently Addressed

- [x] Update Cloud Run scaling/comment docs after switching to `psycopg_pool`.
- [x] Emit clear `classify-all` summary logs, including warnings on partial failures.
