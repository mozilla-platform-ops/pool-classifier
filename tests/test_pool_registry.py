from __future__ import annotations

import pytest

from worker_health.pool_classifier_web import registry


def test_availability_mode_defaults_to_recent_contact():
    pool = registry.Pool("id", "provisioner", "worker-type", "*/15 * * * *")
    assert pool.availability_mode == "recent_contact"


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
