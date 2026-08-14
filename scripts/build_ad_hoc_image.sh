#!/usr/bin/env bash
# Build a commit-derived operational image with immutable provenance from HEAD.

set -euo pipefail

usage() {
  echo "usage: $0 [--dry-run] [--yes]" >&2
  exit 2
}

dry_run=false
approved=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) dry_run=true ;;
    --yes) approved=true ;;
    *) usage ;;
  esac
  shift
done
[[ $# -eq 0 ]] || usage

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source_commit="$(git rev-parse HEAD)"
source_tag="sha-$source_commit"
test -z "$(git status --porcelain)"

if "$dry_run"; then
  printf 'commit=%s\nimage_tag=%s\n' "$source_commit" "$source_tag"
  exit 0
fi

if ! "$approved"; then
  if [[ ! -t 0 ]]; then
    echo "Refusing non-interactive build submission without --yes." >&2
    exit 2
  fi
  read -r -p "Submit operational build $source_tag from $source_commit? [y/N] " response
  [[ "$response" =~ ^[Yy]([Ee][Ss])?$ ]] || exit 0
fi

exec gcloud builds submit --config cloudbuild.yaml \
  --substitutions="_TAG=$source_tag,COMMIT_SHA=$source_commit" \
  --project=relops-pool-classifier .
