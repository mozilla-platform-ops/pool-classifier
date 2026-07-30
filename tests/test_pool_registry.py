from __future__ import annotations

import pytest

from worker_health.pool_classifier_web import registry


def test_availability_mode_defaults_to_recent_contact():
    pool = registry.Pool("id", "provisioner", "worker-type", "*/15 * * * *")
    assert pool.availability_mode == "recent_contact"


def test_pool_id_defaults_to_worker_type(monkeypatch, tmp_path):
    pools_file = tmp_path / "pools.yaml"
    pools_file.write_text("pools:\n  - provisioner: provisioner\n    worker_type: worker-type\n    schedule: '* * * * *'\n")
    monkeypatch.setenv("POOLS_FILE", str(pools_file))
    pools, _ = registry._load_pools()

    assert pools[0].id == "worker-type"


def test_invalid_availability_mode_rejected():
    with pytest.raises(ValueError, match="invalid availability_mode"):
        registry.Pool(
            "id",
            "provisioner",
            "worker-type",
            "*/15 * * * *",
            availability_mode="unknown",
        )


def test_all_proj_autophone_pools_use_listed_mode():
    android_pools = [pool for pool in registry.all_pools_including_disabled() if pool.provisioner == "proj-autophone"]
    assert android_pools
    assert {pool.availability_mode for pool in android_pools} == {"listed"}


def test_other_pools_keep_recent_contact_default():
    other_pools = [pool for pool in registry.all_pools_including_disabled() if pool.provisioner != "proj-autophone"]
    assert other_pools
    assert {pool.availability_mode for pool in other_pools} == {"recent_contact"}


def test_all_pools_skips_vms_worker_types_by_default(monkeypatch):
    normal = registry.Pool("normal", "provisioner", "worker-type", "*/15 * * * *")
    vm = registry.Pool("vm", "provisioner", "worker-type-vms", "*/15 * * * *")
    monkeypatch.setattr(registry, "_pools", [normal, vm])
    monkeypatch.delenv(registry.INCLUDE_VMS_POOLS_ENV, raising=False)

    assert registry.all_pools() == [normal]


def test_all_pools_includes_vms_worker_types_when_requested(monkeypatch):
    normal = registry.Pool("normal", "provisioner", "worker-type", "*/15 * * * *")
    vm = registry.Pool("vm", "provisioner", "worker-type-vms", "*/15 * * * *")
    monkeypatch.setattr(registry, "_pools", [normal, vm])
    monkeypatch.setenv(registry.INCLUDE_VMS_POOLS_ENV, "1")

    assert registry.all_pools() == [normal, vm]


def test_is_vm_pool_only_matches_vms_suffix():
    assert registry.is_vm_pool(registry.Pool("vm", "p", "worker-vms", "* * * * *"))
    assert registry.is_vm_pool(registry.Pool("upper", "p", "worker-VMS", "* * * * *"))
    assert not registry.is_vm_pool(registry.Pool("other", "p", "worker-vm-test", "* * * * *"))
