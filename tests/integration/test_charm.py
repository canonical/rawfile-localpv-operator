#!/usr/bin/env pythonstring3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import shlex
from pathlib import Path

import pytest
import yaml
from kubernetes import client, config, utils
from pytest_operator.plugin import OpsTest
from utils import read_file_from_pod, wait_for_pod

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("charmcraft.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    """Build and deploy the charm-under-test and related charms."""
    charm = next(Path(".").glob("rawfile-localpv*.charm"), None)
    if not charm:
        logger.info("Building rawfile-localpv charm.")
        charms = await ops_test.build_charm(".", return_all=True)
        charm = next((c for c in charms if "24.04" in c.name), None)
        if not charm:
            logger.error("Failed to build charm for 24.04.")
            assert False, "Charm build failed for 24.04."

    overlays = [
        ops_test.Bundle("canonical-kubernetes", channel="latest/edge"),
        Path(__file__).parent / "overlay.yaml",
    ]
    context = {"charm": charm.resolve()}
    bundle, *overlays = await ops_test.async_render_bundles(*overlays, **context)

    logger.info("Deploying rawfile-localpv testing bundle.")
    model = ops_test.model_full_name
    cmd = f"juju deploy -m {model} {bundle} " + " ".join(f"--overlay={f}" for f in overlays)
    rc, stdout, stderr = await ops_test.run(*shlex.split(cmd))
    if rc != 0:
        logger.error(f"Bundle deploy failed: {(stderr or stdout).strip()}")
    assert rc == 0, f"Bundle deploy failed: {(stderr or stdout).strip()}"
    logger.info(stdout)

    await ops_test.model.wait_for_idle(status="active", timeout=60 * 60)


async def test_multiple_providers(kubeconfig: Path, ops_test: OpsTest):
    """Test multiple storage providers by deploying test pods and verifying output."""
    config.load_kube_config(str(kubeconfig))
    api_client = client.ApiClient()
    core_v1 = client.CoreV1Api(api_client)
    manifests_path = Path("tests/integration/data")
    primary_manifests = manifests_path / "primary-pod.yaml"
    secondary_manifest = manifests_path / "secondary-pod.yaml"

    for manifest_file in [primary_manifests, secondary_manifest]:
        with manifest_file.open() as f:
            for manifest in yaml.safe_load_all(f):
                utils.create_from_dict(api_client, manifest)

    wait_for_pod(core_v1, "primary-pod", "default", target_state="Running")
    wait_for_pod(core_v1, "secondary-pod", "default", target_state="Running")

    primary_content = read_file_from_pod(core_v1, "primary-pod", "/data/primary.txt")
    assert primary_content == "Hello from primary!"

    secondary_content = read_file_from_pod(core_v1, "secondary-pod", "/data/secondary.txt")
    assert secondary_content == "Hello from secondary!"
