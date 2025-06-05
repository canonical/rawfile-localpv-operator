#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging
from functools import cached_property
from typing import List, cast

import ops
from charms.reconciler import Reconciler, status
from lightkube import ApiError, Client
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Namespace
from ops.manifests import Collector, ManifestClientError, ResourceAnalysis

from literals import CREATE_NAMESPACE_CONFIG, DEFAULT_NAMESPACE, NAMESPACE_CONFIG
from manifests import RawfileLocalPVManifests

log = logging.getLogger(__name__)


class RawfileLocalPVOperatorCharm(ops.CharmBase):
    """Charm the application."""

    _stored = ops.StoredState()

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        self.reconciler = Reconciler(self, self.reconcile)
        self.collector = Collector(
            RawfileLocalPVManifests(self),
        )

        self.framework.observe(self.on.list_versions_action, self._list_versions)
        self.framework.observe(self.on.list_resources_action, self._list_resources)
        self.framework.observe(self.on.scrub_resources_action, self._scrub_resources)
        self.framework.observe(self.on.sync_resources_action, self._sync_resources)

        self._stored.set_default(namespace=self._configured_namespace)
        self._stored.set_default(is_terminating=False)

    @cached_property
    def _client(self) -> Client:
        return Client(field_manager=self.model.app.name)

    @property
    def _configured_namespace(self) -> str:
        return str(self.config.get(NAMESPACE_CONFIG) or DEFAULT_NAMESPACE)

    def _purge_manifests(self) -> None:
        if not self.unit.is_leader():
            return

        status.add(ops.MaintenanceStatus("Removing Kubernetes Resources"))
        for manifest in self.collector.manifests.values():
            manifest.delete_manifests(ignore_unauthorized=True, ignore_not_found=True)
        raise status.ReconcilerError("Removing charm, preventing reconcile.")

    def _install_manifests(self):
        if not self.unit.is_leader():
            return

        status.add(ops.MaintenanceStatus("Deploying Rawfile LocalPV"))
        for manifest in self.collector.manifests.values():
            try:
                manifest.apply_manifests()
            except ManifestClientError as e:
                failure_msg = " -> ".join(map(str, e.args))
                status.add(ops.WaitingStatus(failure_msg))
                log.warning("Encountered retriable installation error: %s", e)
                raise status.ReconcilerError(failure_msg)

    def _create_namespace(self) -> None:
        """Create the configured namespace."""
        namespace = self._configured_namespace
        log.info(f"Ensuring namespace '{namespace}' exists")
        ns_resource = Namespace(metadata=ObjectMeta(name=namespace))
        try:
            self._client.create(ns_resource)
        except ApiError as e:
            if e.status.code == 409:
                log.info(f"Namespace '{namespace}' already exists")
            else:
                log.exception(f"Failed to create namespace '{namespace}'")
                status.add(ops.WaitingStatus(f"Waiting to create namespace: {namespace}"))
                raise status.ReconcilerError(f"Failed to create namespace '{namespace}'")

    def _check_namespace(self) -> None:
        """Check if the configured namespace exists in the Kubernetes cluster."""
        self.unit.status = ops.MaintenanceStatus("Evaluating Namespace")
        namespace = self._configured_namespace

        try:
            self._client.get(Namespace, name=namespace)
        except ApiError as e:
            create_ns = self.config.get(CREATE_NAMESPACE_CONFIG)
            log.error(e)
            if e.status.code == 404 and create_ns:
                log.info(
                    f"Namespace '{namespace}' not found, but create-namespace=True"
                    "Assuming it will be managed by the charm."
                )
                self._create_namespace()
                return
            if e.status.code == 404 and not create_ns:
                log.warning(f"Namespace '{namespace}' not found and not managed by the charm.")
                status.add(ops.BlockedStatus(f"Missing namespace '{namespace}'"))
                raise status.ReconcilerError("Namespace not found")

            status.add(ops.WaitingStatus("Waiting for Kubernetes API"))
            raise status.ReconcilerError("Waiting for Kubernetes API")

    def _check_teardown(self, event: ops.EventBase) -> bool:
        """Check if the charm is terminating."""
        if cast(bool, self._stored.is_terminating):
            return True
        if isinstance(event, (ops.StopEvent, ops.RemoveEvent)):
            self._stored.is_terminating = True
            return True

        return False

    def _list_versions(self, event: ops.ActionEvent) -> None:
        self.collector.list_versions(event)

    def _list_resources(self, event: ops.ActionEvent) -> None:
        resources = event.params.get("resources", "")
        self.collector.list_resources(event, "", resources)

    def _prevent_collisions(self, event: ops.EventBase) -> None:
        if not self.unit.is_leader():
            return

        status.add(ops.MaintenanceStatus("Detecting resource collisions"))
        analyses: List[ResourceAnalysis] = self.collector.analyze_resources(
            event=event, manifests=None, resources=None
        )

        total_conflicts = sum(len(a.conflicting) for a in analyses)
        if total_conflicts == 0:
            return

        plural = "s" if total_conflicts != 1 else ""
        msg = f"{total_conflicts} Kubernetes resource collision{plural} (action: list-resources)"
        log.error(msg)

        for analysis in analyses:
            if analysis.conflicting:
                log.error(
                    "Collision count in '%s' is %d", analysis.manifest, len(analysis.conflicting)
                )
                for conflicting in sorted(map(str, analysis.conflicting)):
                    log.error(" %s", conflicting)
        status.add(ops.BlockedStatus(msg))
        raise status.ReconcilerError(msg)

    def reconcile(self, event):
        """Reconcile the charm state."""
        if self._check_teardown(event):
            self._purge_manifests()
            return

        self._check_namespace()
        self._prevent_collisions(event)
        self._install_manifests()
        self._update_status(event)

    def _scrub_resources(self, event: ops.ActionEvent) -> None:
        resources = event.params.get("resources", "")
        self.collector.scrub_resources(event, "", resources)

    def _sync_resources(self, event: ops.ActionEvent) -> None:
        resources = event.params.get("resources", "")
        try:
            self.collector.apply_missing_resources(event, "", resources)
        except ManifestClientError as e:
            msg = "Failed to sync missing resources: "
            msg += " -> ".join(map(str, e.args))
            event.set_results({"result": msg})

    def _update_status(self, _):
        if unready := self.collector.unready:
            self.unit.status = ops.WaitingStatus(", ".join(unready))
            raise status.ReconcilerError("Waiting for deployment")
        self.unit.status = ops.ActiveStatus("Ready")
        self.unit.set_workload_version(self.collector.short_version)


if __name__ == "__main__":  # pragma: nocover
    ops.main(RawfileLocalPVOperatorCharm)
