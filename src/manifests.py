# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
# ruff: noqa: C901
"""Patches for managing rawfile-localpv manifests."""

import logging
from functools import cached_property
from typing import Dict, Optional, Protocol

from lightkube.codecs import AnyResource
from lightkube.core.resource import NamespacedResource
from lightkube.models.apps_v1 import StatefulSet
from lightkube.models.rbac_v1 import ClusterRole, ClusterRoleBinding, RoleBinding
from lightkube.resources.apps_v1 import DaemonSet
from lightkube.resources.storage_v1 import StorageClass
from ops.manifests import ConfigRegistry, ManifestLabel, Manifests, Patch

from literals import (
    DRIVER_FORMATTER_CONFIG,
    DRIVER_NAME,
    NAMESPACE_CONFIG,
    NODE_SELECTOR_CONFIG,
    NODE_STORAGE_PATH_CONFIG,
    RBAC_FORMATTER_CONFIG,
    RECLAIM_POLICY_CONFIG,
    STORAGE_CLASS_NAME_CONFIG,
)

log = logging.getLogger(__name__)


class HasManifests(Protocol):
    """Base protocol for manipulations."""

    manifests: Manifests


class CSIDriverNameMixin:
    """Mixin providing a cached property for the CSI driver name."""

    @cached_property
    def csi_driver_name(self: HasManifests) -> str:
        """Return the formatted CSI driver name using the configured formatter."""
        formatter = self.manifests.config.get(DRIVER_FORMATTER_CONFIG)
        if not formatter:
            log.warning(
                "%s is missing in configuration. Falling back to the default driver name: '%s'",
                DRIVER_FORMATTER_CONFIG,
                DRIVER_NAME,
            )
            return DRIVER_NAME
        fmt_context = {"name": DRIVER_NAME, "app": self.manifests.model.app.name}
        return formatter.format(**fmt_context)


class ConfigureStorageClass(CSIDriverNameMixin, Patch):
    """Patch to adjust the StorageClass attributes."""

    def __call__(self, obj: AnyResource) -> None:
        """Adjust the StorageClass."""
        if not isinstance(obj, StorageClass):
            return

        if not obj.metadata or not obj.metadata.name:
            return

        storage_class_name = self.manifests.config.get(STORAGE_CLASS_NAME_CONFIG)
        if not storage_class_name:
            log.warning(
                "StorageClass name is missing in configuration. Skipping StorageClass patch."
            )
        obj.metadata.name = storage_class_name
        obj.provisioner = self.csi_driver_name

        reclaim_policy = self.manifests.config.get(RECLAIM_POLICY_CONFIG)
        if not reclaim_policy:
            log.warning(
                "Reclaim policy is missing in configuration. "
                "Skipping setting reclaim policy for StorageClass '%s'.",
                obj.metadata.name,
            )
        obj.reclaimPolicy = reclaim_policy


class UpdateCSIDriverName(CSIDriverNameMixin, Patch):
    """Patch to adjust the CSI driver name for the csi-driver container."""

    def __call__(self, obj: AnyResource) -> None:
        """Update the CSIDriverName."""
        if not isinstance(obj, (DaemonSet, StatefulSet)):
            return

        if not obj.metadata or not obj.metadata.name:
            return

        if not obj.spec:
            log.warning(
                "Resource '%s' is missing 'spec'. Skipping CSI driver name update.",
                obj.metadata.name,
            )
            return
        if not obj.spec.template.spec:
            log.warning(
                "Resource '%s' is missing pod spec. Skipping CSI driver name update.",
                obj.metadata.name,
            )
            return

        for container in obj.spec.template.spec.containers:
            if container.name == "csi-driver":
                for env in container.env or []:
                    if env.name == "PROVISIONER_NAME":
                        env.value = self.csi_driver_name


class DaemonSetAdjustments(Patch):
    """Patch to adjust the DaemonSet for the rawfile-csi-node."""

    def _parse_node_selector(self) -> Optional[Dict[str, str]]:
        """Parse the node selector configuration into a dictionary."""
        parsed = {}
        raw_config = self.manifests.config.get(NODE_SELECTOR_CONFIG)
        if not raw_config:
            log.info("No node selector configuration found.")
            return None

        selector_strings = raw_config.split(" ")
        for item_str in selector_strings:
            clean_item_str = item_str.strip()

            if not clean_item_str:
                continue

            parts = clean_item_str.split("=", 1)
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip()
                if key:
                    parsed[key] = value
        return parsed

    def __call__(self, obj: AnyResource) -> None:
        """Adjust the DaemonSet to adhere with the charm configuration."""
        if not isinstance(obj, DaemonSet):
            return

        obj_metadata = obj.metadata
        if not obj_metadata or not obj_metadata.name:
            log.warning("DaemonSet is missing metadata or name. Skipping patch.")
            return

        if obj_metadata.name != "rawfile-csi-node":
            return

        spec = obj.spec
        if not spec:
            log.warning("DaemonSet '%s' is missing 'spec'. Skipping patch.", obj_metadata.name)
            return

        template = spec.template
        if not template:
            log.warning("DaemonSet '%s' is missing 'template'. Skipping patch.", obj_metadata.name)
            return

        pod_spec = template.spec
        if not pod_spec:
            log.warning("DaemonSet '%s' is missing 'pod spec'. Skipping patch.", obj_metadata.name)
            return

        if not pod_spec.volumes:
            log.warning("DaemonSet '%s' has no volumes. Skipping patch.", obj_metadata.name)
            return

        pod_spec.nodeSelector = self._parse_node_selector()

        for vol in pod_spec.volumes:
            if vol.name == "socket-dir":
                host_path = vol.hostPath
                if host_path:
                    host_path.path = (
                        f"/var/lib/kubelet/plugins/{self.manifests.model.app.name}-rawfile-csi"
                    )
            if vol.name == "data-dir":
                host_path = vol.hostPath
                if host_path:
                    storage_path = self.manifests.config.get(NODE_STORAGE_PATH_CONFIG)
                    if not storage_path:
                        log.warning(
                            "No storage path configured for 'data-dir' in DaemonSet '%s'.",
                            obj_metadata.name,
                        )
                    else:
                        host_path.path = storage_path

        containers = pod_spec.containers
        if not containers:
            log.warning(
                "DaemonSet '%s' has not containers. Skipping env patch.", obj_metadata.name
            )
            return

        for container in containers:
            if container.name == "node-driver-registrar":
                env_vars = container.env
                if not env_vars:
                    log.warning(
                        "Container 'node-driver-registrar' in DaemonSet '%s' has no env vars.",
                        obj_metadata.name,
                    )
                    return

                for env in env_vars:
                    if env.name == "DRIVER_REG_SOCK_PATH":
                        env.value = (
                            f"/var/lib/kubelet/plugins/"
                            f"{self.manifests.model.app.name}-rawfile-csi/csi.sock"
                        )
                        break
                break


class AdjustNamespace(Patch):
    """Patch to adjust the namespace of namespaced resources."""

    def __call__(self, obj: AnyResource) -> None:
        """Adjust the Namespace."""
        if isinstance(obj, NamespacedResource) and obj.metadata:
            ns = self.manifests.config.get(NAMESPACE_CONFIG)
            obj.metadata.namespace = ns


class CSIDriverAdjustments(CSIDriverNameMixin, Patch):
    """Patch class to adjust the CSIDriver name."""

    def __call__(self, obj: AnyResource) -> None:
        """Adjust the CSIDriverName."""
        if not obj.metadata or not obj.metadata.name:
            return
        if obj.kind == "CSIDriver":
            obj.metadata.name = self.csi_driver_name


class RBACAdjustments(Patch):
    """Patch class to adjust RBAC resource names and namespaces."""

    def _rename(self, name: str) -> str:
        """Rename the resource using the specified formatter.

        Arguments:
            name: The original name of the resource.

        Returns:
            The formatted name, or the original name if the formatter is missing.
        """
        formatter = self.manifests.config.get(RBAC_FORMATTER_CONFIG)
        if not formatter:
            log.warning("%s is empty. Fallback to default name.")
            return name

        fmt_context = {"name": name, "app": self.manifests.model.app.name}
        return formatter.format(**fmt_context)

    def __call__(self, obj: AnyResource) -> None:
        """Adjust the RBAC resource names and namespaces."""
        ns = self.manifests.config.get(NAMESPACE_CONFIG)
        if not obj.metadata or not obj.metadata.name:
            log.error("Resource is missing metadata or name: %s. Skipping patch.", obj)
            return

        if isinstance(obj, (ClusterRole, ClusterRoleBinding)):
            obj.metadata.name = self._rename(obj.metadata.name)

        if isinstance(obj, (ClusterRoleBinding, RoleBinding)):
            for subject in obj.subjects or []:
                if subject.kind == "ServiceAccount":
                    subject.namespace = ns
            if obj.roleRef.kind == "ClusterRole":
                obj.roleRef.name = self._rename(obj.roleRef.name)


class RawfileLocalPVManifests(Manifests):
    """Class for managing rawfile-localpv resources."""

    def __init__(self, charm):
        manipulations = [
            AdjustNamespace(self),
            ConfigureStorageClass(self),
            CSIDriverAdjustments(self),
            DaemonSetAdjustments(self),
            ManifestLabel(self),
            RBACAdjustments(self),
            UpdateCSIDriverName(self),
        ]

        super().__init__("rawfile-local-pv", charm.model, "upstream", manipulations)
        self.charm_config = charm.config

    @property
    def config(self) -> Dict:
        """Return a cleaned up configuration dictionary.

        Returns:
            Dict[str, str]: The cleaned configuration dictionary.
        """
        config = dict(self.charm_config)

        keys_to_remove = [k for k, v in config.items() if v in ("", None)]
        for k in keys_to_remove:
            del config[k]
        return config
