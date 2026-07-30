"""Cached Taskcluster worker-type discovery."""
from __future__ import annotations
import os
from datetime import datetime, timezone
import taskcluster
from worker_health.pool_classifier_web import registry

PROVISIONERS = ("proj-autophone", "releng-hardware")
_cache = None

def discover(force=False):
    global _cache
    if _cache is not None and not force:
        return _cache
    queue = taskcluster.Queue({"rootUrl": os.environ.get("TC_ROOT_URL", "https://firefox-ci-tc.services.mozilla.com")})
    found = []
    for provisioner in PROVISIONERS:
        query = {}
        while True:
            response = queue.listWorkerTypes(provisioner, query=query)
            found.extend((provisioner, item["workerType"]) for item in response.get("workerTypes", []))
            if not response.get("continuationToken"):
                break
            query = {"continuationToken": response["continuationToken"]}
    configured = {(p.provisioner, p.worker_type): p for p in registry.all_pools_including_disabled()}
    rows = []
    for provisioner, worker_type in sorted(set(found)):
        pool = configured.pop((provisioner, worker_type), None)
        if pool and pool.enabled:
            status, reason = "covered", ""
        elif pool:
            status, reason = "excluded", pool.reason
        elif worker_type.lower().endswith("-vms"):
            status, reason = "ignored", "Globally ignored by the -vms convention"
        else:
            status, reason = "uncovered", ""
        rows.append({"provisioner": provisioner, "worker_type": worker_type, "status": status, "reason": reason})
    for pool in configured.values():
        rows.append({"provisioner": pool.provisioner, "worker_type": pool.worker_type, "status": "configured inactive", "reason": "Not returned by Taskcluster"})
    _cache = {"fetched_at": datetime.now(timezone.utc).isoformat(), "rows": rows}
    return _cache
