<!-- br-agent-instructions-v1 -->

---

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`/`bd`) for issue tracking. Issues are stored in `.beads/` and tracked in git.

### Essential Commands

```bash
# View ready issues (open, unblocked, not deferred)
br ready              # or: bd ready

# List and search
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br search "keyword"   # Full-text search

# Create and update
br create --title="..." --description="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once

# Sync with git
br sync --flush-only  # Export DB to JSONL
br sync --status      # Check sync status
```

### Workflow Pattern

1. **Start**: Run `br ready` to find actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Always run `br sync --flush-only` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only open, unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers 0-4, not words)
- **Types**: task, bug, feature, epic, chore, docs, question
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

### Session Protocol

**Before ending any session, run this checklist:**

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads changes to JSONL
git commit -m "..."     # Commit everything
git push                # Push to remote
```

### Best Practices

- Check `br ready` at session start to find available work
- Update status as you work (in_progress → closed)
- Create new issues with `br create` when you discover tasks
- Do not create a bead solely to track administrative changes to other beads
  (such as status, priority, ownership, or metadata updates); make and commit
  those tracker updates directly.
- Use descriptive titles and set appropriate priority/type
- Always sync before ending session

<!-- end-br-agent-instructions -->

---

## Release Workflow

A release version has three linked identities: the package version in
`pyproject.toml`, an annotated Git tag, and the deployed image tag. Keep them
identical. The image must also embed the immutable Git commit for provenance.

1. Start from a clean, up-to-date `main` and choose `VERSION` (without the `v`
   prefix). By default, release versions must advance only the patch component
   (for example, `1.2.0` to `1.2.1`). A minor or major version bump requires
   explicit user direction.
2. Change `[project].version` in `pyproject.toml` to `VERSION`, then run
   `uv lock` to refresh `uv.lock` metadata.
3. Run `scripts/run_local_postgres_tests.sh` successfully (the required full
   suite, including PostgreSQL-backed integration tests), then review the
   release diff.
4. Commit the version and lockfile together, for example:
   `git commit -m "chore: release v$VERSION"`.
5. Create an annotated tag only after that commit exists:
   `git tag -a "v$VERSION" -m "v$VERSION"`.
6. Push the release commit and tag: `git push origin main --follow-tags`.
7. Before a manual Cloud Build deploy, verify the clean checkout is exactly the
   release tag, then build it with matching image-tag and commit provenance:

   ```bash
   RELEASE_COMMIT="$(git rev-parse "v${VERSION}^{commit}")"
   test "$(git rev-parse HEAD)" = "$RELEASE_COMMIT"
   gcloud builds submit --config cloudbuild.yaml \
     --substitutions=_TAG="v$VERSION",COMMIT_SHA="$RELEASE_COMMIT" \
     --project=relops-pool-classifier .
   ```

8. Confirm Cloud Run serves the new revision/image and inspect recent warning
   logs before considering the release complete.

Do not tag an unchanged package version: a tag such as `v1.1.2` with
`pyproject.toml` still at `1.1.1` produces a misleading application version.

### Ad-hoc deployment workflow

For an operational or performance deployment that is not a release, do not
create or reuse a semver tag. Start from a clean, committed checkout and derive
both identities from `HEAD`. A successful `scripts/run_local_postgres_tests.sh`
is required before submitting the build:

```bash
scripts/run_local_postgres_tests.sh
scripts/build_ad_hoc_image.sh
```

Deploy the resulting `app:$SOURCE_TAG` through the same migration, candidate,
verification, and traffic-promotion gates. The package version can remain the
most recent release version, but the image tag and embedded commit identify the
actual deployed revision.

