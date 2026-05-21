"""Pattern registry: loads patterns.yaml and provides classification + severity lookup."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

_DEFAULT_PATTERNS_FILE = Path(__file__).parent / "patterns.yaml"

_VALID_SEVERITIES = {"critical", "high", "low"}
_SEVERITY_RANK = {"critical": 0, "high": 1, "low": 2}


@dataclass
class Pattern:
    name: str
    regex: str
    severity: str
    tags: List[str] = field(default_factory=list)
    description: str = ""
    enabled: bool = True
    _compiled: object = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self):
        if self.severity not in _VALID_SEVERITIES:
            raise ValueError(
                f"Pattern '{self.name}': invalid severity '{self.severity}' (must be one of {_VALID_SEVERITIES})",
            )
        try:
            self._compiled = re.compile(self.regex)
        except re.error as e:
            raise ValueError(f"Pattern '{self.name}': invalid regex: {e}") from e

    def search(self, text: str) -> bool:
        return bool(self._compiled.search(text))


def _load_patterns() -> tuple[List[Pattern], Dict[str, str]]:
    patterns_file = Path(os.environ.get("PATTERNS_FILE", str(_DEFAULT_PATTERNS_FILE)))
    with open(patterns_file) as f:
        data = yaml.safe_load(f)
    patterns = []
    severity_map: Dict[str, str] = {}
    for entry in data["patterns"]:
        p = Pattern(
            name=entry["name"],
            regex=entry["regex"],
            severity=entry["severity"],
            tags=entry.get("tags", []),
            description=entry.get("description", ""),
            enabled=entry.get("enabled", True),
        )
        patterns.append(p)
        # last-writer wins for same category name — severity should be consistent
        severity_map[p.name] = p.severity
    return patterns, severity_map


_patterns, _severity_map = _load_patterns()


def all_patterns() -> List[Pattern]:
    """Return enabled patterns in match order: highest severity first, file order within a tier.

    Callers do first-match-wins; the sort ensures a critical pattern always beats a
    high pattern even if the high pattern appears earlier in patterns.yaml.
    """
    enabled = [p for p in _patterns if p.enabled]
    return sorted(enabled, key=lambda p: _SEVERITY_RANK[p.severity])


def severity_of(category_name: str) -> Optional[str]:
    """Return the severity tier for a category name, or None if unknown."""
    return _severity_map.get(category_name)


def categories_by_severity(severity: str) -> List[str]:
    """Return all category names mapped to the given severity tier."""
    return [name for name, sev in _severity_map.items() if sev == severity]
