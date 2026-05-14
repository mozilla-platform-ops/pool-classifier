"""Pool registry: loads pools.yaml and provides provisioner/worker_type-based lookup."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import yaml

_DEFAULT_POOLS_FILE = Path(__file__).parent / "pools.yaml"


@dataclass
class Pool:
    id: str
    provisioner: str
    worker_type: str
    schedule: str


def _load_pools() -> Tuple[List[Pool], dict]:
    pools_file = Path(os.environ.get("POOLS_FILE", str(_DEFAULT_POOLS_FILE)))
    with open(pools_file) as f:
        data = yaml.safe_load(f)
    pools = [Pool(**p) for p in data["pools"]]
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


def all_pools() -> List[Pool]:
    return _pools


def get_pool(provisioner: str, worker_type: str) -> Optional[Pool]:
    return _by_prov_wt.get((provisioner, worker_type))
