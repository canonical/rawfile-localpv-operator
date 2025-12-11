#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for the Rawfile LocalPV charm.

These tests verify:
- Multi-pool deployment with different storage classes
- PVC provisioning and binding
- Pod scheduling with node selectors
- Data persistence across storage pools
"""

import logging
from pathlib import Path
from typing import List

import jubilant
import pytest
import yaml
from kubernetes import client, config, utils
from utils import (
    POD_WAIT_TIMEOUT,
    PVC_WAIT_TIMEOUT,
    create_pod_with_pvc,
    create_pvc,
    create_storage_class,
    delete_pod,
    delete_pv,
    delete_pvc,
    delete_storage_class,
    get_pv_for_pvc,
    get_pv_reclaim_policy,
    k8s_resource_cleanup,
    pv_exists,
    read_file_from_pod,
    verify_storage_class_exists,
    wait_for_pod,
    wait_for_pod_deleted,
    wait_for_pv_deleted,
    wait_for_pvc_bound,
    wait_for_pvc_deleted,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("charmcraft.yaml").read_text())
APP_NAME = METADATA["name"]

# Test resource names for cleanup tracking
PRIMARY_RESOURCES = [
    {"kind": "Pod", "name": "primary-pod"},
    {"kind": "PersistentVolumeClaim", "name": "primary-pvc"},
]
SECONDARY_RESOURCES = [
    {"kind": "Pod", "name": "secondary-pod"},
    {"kind": "PersistentVolumeClaim", "name": "secondary-pvc"},
]


@pytest.mark.usefixtures("kubernetes_cluster")
class TestDeployment:
    """Test suite for charm deployment and configuration."""

    def test_deploy_pools(self, juju: jubilant.Juju, charm_path: Path):
        """Test deploying multiple storage pools with different configurations.

        This test verifies that:
        - Multiple instances of the charm can be deployed
        - Each instance can have its own namespace and storage class
        - Node selectors are properly configured
        - Charms reach active status after integration
        """
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

    def test_storage_classes_created(self, kubeconfig: Path):
        """Verify that storage classes are created after deployment.

        This test ensures that the charm properly creates the configured
        storage classes in the Kubernetes cluster.
        """
        config.load_kube_config(str(kubeconfig))
        storage_api = client.StorageV1Api()

        assert verify_storage_class_exists(storage_api, "primary-sc"), (
            "Primary storage class 'primary-sc' was not created"
        )
        assert verify_storage_class_exists(storage_api, "secondary-sc"), (
            "Secondary storage class 'secondary-sc' was not created"
        )


@pytest.mark.usefixtures("juju")
class TestStorageProvisioning:
    """Test suite for storage provisioning and data persistence."""

    @staticmethod
    def _load_manifests(manifest_file: Path) -> List[dict]:
        """Load all manifests from a YAML file.

        Args:
            manifest_file: Path to the YAML manifest file.

        Returns:
            List of manifest dictionaries.
        """
        with manifest_file.open() as f:
            return list(yaml.safe_load_all(f))

    def test_multiple_providers(self, kubeconfig: Path):
        """Test that multiple storage providers can provision volumes independently.

        This test verifies:
        - PVCs are created and bound using the correct storage class
        - Pods are scheduled on nodes matching the storage pool
        - Data can be written to and read from the provisioned volumes
        - Each storage pool operates independently

        The test uses a cleanup context manager to ensure resources
        are deleted even if the test fails.
        """
        config.load_kube_config(str(kubeconfig))
        api_client = client.ApiClient()
        core_v1 = client.CoreV1Api(api_client)
        manifests_path = Path("tests/integration/data")
        primary_manifests = manifests_path / "primary-pod.yaml"
        secondary_manifest = manifests_path / "secondary-pod.yaml"

        all_resources = PRIMARY_RESOURCES + SECONDARY_RESOURCES

        with k8s_resource_cleanup(api_client, all_resources, namespace="default"):
            # Create all resources from manifests
            for manifest_file in [primary_manifests, secondary_manifest]:
                for manifest in self._load_manifests(manifest_file):
                    utils.create_from_dict(api_client, manifest)
                    logger.info(
                        "Created %s '%s'",
                        manifest.get("kind"),
                        manifest.get("metadata", {}).get("name"),
                    )

            # Wait for PVCs to be bound first (ensures storage provisioning works)
            wait_for_pvc_bound(core_v1, "primary-pvc", "default", timeout=PVC_WAIT_TIMEOUT)
            wait_for_pvc_bound(core_v1, "secondary-pvc", "default", timeout=PVC_WAIT_TIMEOUT)

            # Wait for pods to be running
            wait_for_pod(
                core_v1,
                "primary-pod",
                "default",
                target_state="Running",
                timeout=POD_WAIT_TIMEOUT,
            )
            wait_for_pod(
                core_v1,
                "secondary-pod",
                "default",
                target_state="Running",
                timeout=POD_WAIT_TIMEOUT,
            )

            # Verify data was written correctly to each storage pool
            primary_content = read_file_from_pod(
                core_v1, "primary-pod", "/data/primary.txt", namespace="default"
            )
            assert primary_content == "Hello from primary!", (
                f"Expected 'Hello from primary!' but got '{primary_content}'"
            )

            secondary_content = read_file_from_pod(
                core_v1, "secondary-pod", "/data/secondary.txt", namespace="default"
            )
            assert secondary_content == "Hello from secondary!", (
                f"Expected 'Hello from secondary!' but got '{secondary_content}'"
            )


@pytest.mark.usefixtures("juju")
class TestVolumeLifecycle:
    """Test suite for volume lifecycle management and reclaim policies."""

    CSI_DRIVER_BASE = "rawfile.csi.openebs.io"
    # Use the primary app name
    PRIMARY_APP_NAME = "primary-localpv"

    @property
    def csi_provisioner(self) -> str:
        return f"{self.PRIMARY_APP_NAME}-{self.CSI_DRIVER_BASE}"

    def test_volume_deletion_with_delete_policy(self, kubeconfig: Path):
        """Test that PV is deleted when PVC is deleted with 'Delete' reclaim policy."""
        config.load_kube_config(str(kubeconfig))
        core_v1 = client.CoreV1Api()
        storage_api = client.StorageV1Api()

        sc_name = "test-delete-policy-sc"
        pvc_name = "test-delete-policy-pvc"
        pod_name = "test-delete-policy-pod"
        namespace = "default"

        try:
            create_storage_class(
                storage_api,
                name=sc_name,
                provisioner=self.csi_provisioner,
                reclaim_policy="Delete",
            )

            create_pvc(
                core_v1,
                name=pvc_name,
                namespace=namespace,
                storage_class=sc_name,
                size="1Gi",
            )

            create_pod_with_pvc(
                core_v1,
                pod_name=pod_name,
                pvc_name=pvc_name,
                namespace=namespace,
                node_selector={"storagePool": "primary"},
            )

            wait_for_pvc_bound(core_v1, pvc_name, namespace, timeout=PVC_WAIT_TIMEOUT)

            pv_name = get_pv_for_pvc(core_v1, pvc_name, namespace)
            assert pv_name is not None, f"PVC '{pvc_name}' is not bound to any PV"

            reclaim_policy = get_pv_reclaim_policy(core_v1, pv_name)
            assert reclaim_policy == "Delete", (
                f"Expected reclaim policy 'Delete' but got '{reclaim_policy}'"
            )

            delete_pod(core_v1, pod_name, namespace)
            wait_for_pod_deleted(core_v1, pod_name, namespace)

            delete_pvc(core_v1, pvc_name, namespace)

            wait_for_pvc_deleted(core_v1, pvc_name, namespace)

            wait_for_pv_deleted(core_v1, pv_name, timeout=PVC_WAIT_TIMEOUT)

            logger.info("Successfully verified: PV '%s' was deleted after PVC deletion", pv_name)

        finally:
            try:
                delete_pod(core_v1, pod_name, namespace)
            except Exception:
                pass
            try:
                delete_pvc(core_v1, pvc_name, namespace)
            except Exception:
                pass
            try:
                delete_storage_class(storage_api, sc_name)
            except Exception:
                pass

    def test_volume_retain_policy(self, kubeconfig: Path):
        """Test that PV is retained when PVC is deleted with 'Retain' reclaim policy."""
        config.load_kube_config(str(kubeconfig))
        core_v1 = client.CoreV1Api()
        storage_api = client.StorageV1Api()

        sc_name = "test-retain-policy-sc"
        pvc_name = "test-retain-policy-pvc"
        pod_name = "test-retain-policy-pod"
        namespace = "default"
        pv_name = None

        try:
            create_storage_class(
                storage_api,
                name=sc_name,
                provisioner=self.csi_provisioner,
                reclaim_policy="Retain",
            )

            create_pvc(
                core_v1,
                name=pvc_name,
                namespace=namespace,
                storage_class=sc_name,
                size="1Gi",
            )

            create_pod_with_pvc(
                core_v1,
                pod_name=pod_name,
                pvc_name=pvc_name,
                namespace=namespace,
                node_selector={"storagePool": "primary"},
            )

            wait_for_pvc_bound(core_v1, pvc_name, namespace, timeout=PVC_WAIT_TIMEOUT)

            pv_name = get_pv_for_pvc(core_v1, pvc_name, namespace)
            assert pv_name is not None, f"PVC '{pvc_name}' is not bound to any PV"

            reclaim_policy = get_pv_reclaim_policy(core_v1, pv_name)
            assert reclaim_policy == "Retain", (
                f"Expected reclaim policy 'Retain' but got '{reclaim_policy}'"
            )

            delete_pod(core_v1, pod_name, namespace)
            wait_for_pod_deleted(core_v1, pod_name, namespace)

            delete_pvc(core_v1, pvc_name, namespace)

            wait_for_pvc_deleted(core_v1, pvc_name, namespace)

            assert pv_exists(core_v1, pv_name), (
                f"PV '{pv_name}' was deleted but should have been retained"
            )

            pv = core_v1.read_persistent_volume(pv_name)
            assert pv.status.phase == "Released", (
                f"Expected PV status 'Released' but got '{pv.status.phase}'"
            )

            logger.info(
                "Successfully verified: PV '%s' was retained after PVC deletion with status '%s'",
                pv_name,
                pv.status.phase,
            )

        finally:
            try:
                delete_pod(core_v1, pod_name, namespace)
            except Exception:
                pass
            try:
                delete_pvc(core_v1, pvc_name, namespace)
            except Exception:
                pass
            if pv_name:
                try:
                    delete_pv(core_v1, pv_name)
                    logger.info("Cleaned up retained PV '%s'", pv_name)
                except Exception:
                    pass
            try:
                delete_storage_class(storage_api, sc_name)
            except Exception:
                pass
