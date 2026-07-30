from worker_health.pool_classifier_web import discovery


def test_discovery_marks_vms_worker_types_as_ignored(monkeypatch):
    class Queue:
        def listWorkerTypes(self, provisioner, query):
            return {"workerTypes": [{"workerType": "worker-vms"}]} if provisioner == "releng-hardware" else {}

    monkeypatch.setattr(discovery.taskcluster, "Queue", lambda config: Queue())
    monkeypatch.setattr(discovery.registry, "all_pools_including_disabled", lambda: [])
    monkeypatch.setattr(discovery, "_cache", None)

    row = discovery.discover(force=True)["rows"][0]
    assert row["status"] == "ignored"
    assert row["reason"] == "Globally ignored by the -vms convention"
