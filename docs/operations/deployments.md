# Deployments

Use an immutable image and run the PostgreSQL-inclusive test suite before a
release or an operational deployment:

```sh
scripts/run_local_postgres_tests.sh
```

If Google Cloud credentials need refreshing, refresh both the CLI account and
Application Default Credentials together:

```sh
gcloud auth login aerickson@firefox.gcp.mozilla.com --update-adc
```

## Tagged release

Release versions have three matching identities: the version in `pyproject.toml`,
an annotated Git tag, and the deployed image tag. Follow the release workflow
in [`AGENTS.md`](../../AGENTS.md), then use the checked build wrapper:

Before creating the release commit, add a concise, Slack-ready summary at
`docs/release-notes/vVERSION.md`. Review it with the release diff; after the
production traffic gate succeeds, post that summary to the release channel.

```sh
scripts/run_local_postgres_tests.sh
scripts/build_release_image.sh
```

The wrapper derives the version from `pyproject.toml` and the commit from its
matching annotated tag. It rejects an unclean checkout or a tag not at `HEAD`.
It requests confirmation before submitting Cloud Build; use `--yes` only for
an explicitly approved non-interactive submission. It does not change
production traffic.

## Ad-hoc deployment

For a non-release deployment, build from committed `HEAD` with a commit-derived
tag. Do not create or reuse a semantic version tag:

```sh
scripts/run_local_postgres_tests.sh
scripts/build_ad_hoc_image.sh
```

The wrapper derives both the image tag and embedded commit from the same clean
`HEAD`, and requests confirmation before submitting Cloud Build. Use `--yes`
only for an explicitly approved non-interactive submission.

## Production gate

After Terraform has created the jobs, run migrations before creating a web
candidate. Replace `vVERSION` with either the release tag or commit-derived
tag. Inspect the candidate's own readiness and image digest before promotion.

```sh
IMAGE=us-west1-docker.pkg.dev/relops-pool-classifier/pool-classifier/app:vVERSION

gcloud run jobs update pool-classifier-migrate \
  --image="$IMAGE" --region=us-west1 --project=relops-pool-classifier
gcloud run jobs execute pool-classifier-migrate \
  --wait --region=us-west1 --project=relops-pool-classifier

gcloud run deploy pool-classifier --image="$IMAGE" --no-traffic \
  --region=us-west1 --project=relops-pool-classifier

gcloud run services update-traffic pool-classifier --to-latest \
  --region=us-west1 --project=relops-pool-classifier
```

With `--no-traffic`, Cloud Run can label the candidate revision `Retired` and
keep `latestReadyRevisionName` pointing to the traffic-serving revision. That is
expected at zero traffic; inspect the candidate's `Ready` condition and logs,
rather than treating `Retired` alone as failure.
