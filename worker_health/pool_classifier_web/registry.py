"""Pool registry: loads pools.yaml and provides provisioner/worker_type-based lookup."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import yaml

_DEFAULT_POOLS_FILE = Path(__file__).parent / "pools.yaml"
AVAILABILITY_MODES = {"recent_contact", "listed"}
INCLUDE_VMS_POOLS_ENV = "INCLUDE_VMS_POOLS"
_POOL_DEFAULT_KEYS = {"schedule", "enabled", "reason", "availability_mode"}


@dataclass
class Pool:
    id: str
    provisioner: str
    worker_type: str
    schedule: str
    enabled: bool = True
    reason: str = ""
    availability_mode: str = "recent_contact"

    def __post_init__(self) -> None:
        if self.availability_mode not in AVAILABILITY_MODES:
            allowed = ", ".join(sorted(AVAILABILITY_MODES))
            raise ValueError(
                f"invalid availability_mode {self.availability_mode!r} for pool {self.id}; "
                f"expected one of: {allowed}",
            )


def _load_pools() -> Tuple[List[Pool], dict]:
    pools_file = Path(os.environ.get("POOLS_FILE", str(_DEFAULT_POOLS_FILE)))
    with open(pools_file) as f:
        data = yaml.safe_load(f)
    defaults = data.get("defaults", {})
    provisioner_defaults = data.get("provisioner_defaults", {})
    if not isinstance(defaults, dict) or not isinstance(provisioner_defaults, dict):
        raise ValueError("pool defaults must be mappings")
    for label, values in [("defaults", defaults), *provisioner_defaults.items()]:
        if not isinstance(values, dict):
            raise ValueError(f"pool defaults for {label} must be a mapping")
        invalid = set(values) - _POOL_DEFAULT_KEYS
        if invalid:
            raise ValueError(f"invalid pool defaults for {label}: {sorted(invalid)}")
    pools = []
    for entry in data["pools"]:
        merged = {**defaults, **provisioner_defaults.get(entry["provisioner"], {}), **entry}
        pools.append(Pool(id=merged.get("id", merged["worker_type"]), **{k: v for k, v in merged.items() if k != "id"}))
    by_prov_wt = {(p.provisioner, p.worker_type): p for p in pools}
    return pools, by_prov_wt


_pools, _by_prov_wt = _load_pools()


def detect_os(pool: "Pool") -> str:
    if pool.provisioner == "proj-autophone":
        return "android"
    wt = pool.worker_type.lower()
    if any(x in wt for x in ("osx", "arm64", "m4", "m-vms", "macos")):
        return "macos"
    if any(x in wt for x in ("win",)):
        return "windows"
    return "linux"


def is_vm_pool(pool: "Pool") -> bool:
    """Whether a pool follows the Taskcluster ``-vms`` worker-type convention."""
    return pool.worker_type.lower().endswith("-vms")


def all_pools() -> List[Pool]:
    """Return pools included in automatic classification.

    VM pools are intentionally omitted by default: they are short-lived and
    their job and worker volume overwhelms a normal classification cycle. Set
    ``INCLUDE_VMS_POOLS=1`` to include them for an intentional full run.
    """
    include_vms = os.environ.get(INCLUDE_VMS_POOLS_ENV) == "1"
    return [p for p in _pools if p.enabled and (include_vms or not is_vm_pool(p))]


def all_pools_including_disabled() -> List[Pool]:
    return _pools


def get_pool(provisioner: str, worker_type: str) -> Optional[Pool]:
    return _by_prov_wt.get((provisioner, worker_type))
