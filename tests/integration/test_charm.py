#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import pytest
import yaml
from kubernetes import client, config, utils
from utils import read_file_from_pod, wait_for_pod

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("charmcraft.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.usefixtures("kubernetes_cluster")
def test_deploy_pools(juju: jubilant.Juju, charm_path: Path):
    juju.deploy(
        charm=charm_path,
        app="primary-localpv",
        config={
            "namespace": "primary",
            "storage-class-name": "primary-sc",
            "node-selector": "storagePool=primary",
            "create-namespace": True,
        },
    )
    juju.deploy(
        charm=charm_path,
        app="secondary-localpv",
        config={
            "namespace": "secondary",
            "storage-class-name": "secondary-sc",
            "node-selector": "storagePool=secondary",
            "create-namespace": True,
        },
    )

    juju.integrate("k8s", "primary-localpv:kubernetes")
    juju.integrate("k8s", "secondary-localpv:kubernetes")

    juju.wait(jubilant.all_active)


@pytest.mark.usefixtures("juju")
def test_multiple_providers(kubeconfig: Path):
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
