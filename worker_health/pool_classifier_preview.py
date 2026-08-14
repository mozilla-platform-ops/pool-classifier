"""Read-only comparison of task classification rules."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import subprocess

from worker_health.pool_classifier_web.patterns_registry import (
    Pattern,
    classify_patterns,
    load_patterns,
    ordered_patterns,
)


PATTERNS_REPO_PATH = "worker_health/pool_classifier_web/patterns.yaml"


@dataclass(frozen=True)
class ClassificationPreview:
    category: str
    pattern: Pattern | None

    @property
    def severity(self) -> str | None:
        return self.pattern.severity if self.pattern else None


@dataclass(frozen=True)
class PreviewComparison:
    before: ClassificationPreview
    proposed: ClassificationPreview
    proposed_shadows_before: bool


def load_committed_patterns(repo_root: Path, base_ref: str) -> list[Pattern]:
    """Load patterns from a committed Git revision without touching the worktree."""
    result = subprocess.run(
        ["git", "-C", str(repo_root), "show", f"{base_ref}:{PATTERNS_REPO_PATH}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        message = result.stderr.strip() or f"git show exited {result.returncode}"
        raise ValueError(f"could not read patterns from {base_ref}: {message}")
    return load_patterns_text(result.stdout, f"{base_ref}:{PATTERNS_REPO_PATH}")


def load_patterns_text(text: str, source: str) -> list[Pattern]:
    """Load patterns from YAML supplied by Git rather than a filesystem path."""
    from yaml import safe_load

    data = safe_load(text)
    if not isinstance(data, dict) or not isinstance(data.get("patterns"), list):
        raise ValueError(f"{source}: expected a top-level patterns list")
    # Reuse the production parser/validation via a short-lived temporary file is
    # unnecessary; construct entries here with the same Pattern validation.
    return [
        Pattern(
            name=entry["name"], regex=entry["regex"], severity=entry["severity"],
            tags=entry.get("tags", []), description=entry.get("description", ""),
            enabled=entry.get("enabled", True),
        )
        for entry in data["patterns"]
    ]


def compare_classification(
    before_patterns: list[Pattern], proposed_patterns: list[Pattern],
    log_text: str, run_state: str, reason_resolved: str | None,
) -> PreviewComparison:
    """Classify one task run with two rule sets, without persisting anything."""
    before_category, before_pattern = classify_patterns(before_patterns, log_text, run_state, reason_resolved)
    proposed_category, proposed_pattern = classify_patterns(proposed_patterns, log_text, run_state, reason_resolved)
    proposed_names = [pattern.name for pattern in ordered_patterns(proposed_patterns)]
    shadows_before = bool(
        before_pattern and proposed_pattern and before_pattern.name != proposed_pattern.name
        and before_pattern.name in proposed_names
        and proposed_names.index(proposed_pattern.name) < proposed_names.index(before_pattern.name)
    )
    return PreviewComparison(
        before=ClassificationPreview(before_category, before_pattern),
        proposed=ClassificationPreview(proposed_category, proposed_pattern),
        proposed_shadows_before=shadows_before,
    )


def load_working_tree_patterns(repo_root: Path) -> list[Pattern]:
    """Load the normal editable filters file from the current checkout."""
    patterns, _ = load_patterns(repo_root / PATTERNS_REPO_PATH)
    return patterns


def select_terminal_run(status: dict, requested_run: int | None = None) -> dict:
    """Select a requested terminal run, or the newest terminal run by ID."""
    runs = status.get("status", {}).get("runs", [])
    terminal = [run for run in runs if run.get("state") in {"completed", "failed", "exception"}]
    if requested_run is not None:
        selected = next((run for run in terminal if run.get("runId") == requested_run), None)
        if selected is None:
            raise ValueError(f"task has no terminal run {requested_run}")
        return selected
    if not terminal:
        raise ValueError("task has no terminal runs")
    return max(terminal, key=lambda run: run.get("runId", -1))
