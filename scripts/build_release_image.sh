#!/usr/bin/env bash
# Build a tagged release image with provenance derived from the annotated tag.

set -euo pipefail

usage() {
  echo "usage: $0 [--dry-run] VERSION" >&2
  echo "VERSION must not include the v prefix (for example: 1.1.17)." >&2
  exit 2
}

dry_run=false
if [[ "${1:-}" == "--dry-run" ]]; then
  dry_run=true
  shift
fi
[[ $# -eq 1 ]] || usage

version="$1"
[[ "$version" != v* && "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || usage

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

tag="v$version"
release_commit="$(git rev-parse "${tag}^{commit}")"
head_commit="$(git rev-parse HEAD)"
project_version="$(sed -n 's/^version = "\([^"]*\)"$/\1/p' pyproject.toml)"

test "$project_version" = "$version"
test "$head_commit" = "$release_commit"
test -z "$(git status --porcelain)"

if "$dry_run"; then
  printf 'tag=%s\ncommit=%s\nimage_tag=%s\n' "$tag" "$release_commit" "$tag"
  exit 0
fi

exec gcloud builds submit --config cloudbuild.yaml \
  --substitutions="_TAG=$tag,COMMIT_SHA=$release_commit" \
  --project=relops-pool-classifier .
