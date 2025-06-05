# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

from unittest.mock import MagicMock, PropertyMock, patch

import httpx
from lightkube.core.exceptions import ApiError
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.apps_v1 import DaemonSet
from ops import testing
from ops.manifests import ManifestClientError

from charm import RawfileLocalPVOperatorCharm


class FakeApiError(ApiError):
    def __init__(self, *args, **kwargs) -> None:
        response = httpx.Response(status_code=404, content="{}")
        super().__init__(response=response)
        self.status.code = 404


@patch.object(RawfileLocalPVOperatorCharm, "_client", new_callable=PropertyMock)
@patch("ops.manifests.Manifests.client", new_callable=PropertyMock)
def test_base(mock_manifest_client, _: PropertyMock):
    fake_client = MagicMock()
    fake_client.get.side_effect = ManifestClientError
    fake_client.list.return_value = []

    mock_manifest_client.return_value = fake_client

    ctx = testing.Context(RawfileLocalPVOperatorCharm)
    state = testing.State(leader=True)
    out = ctx.run(ctx.on.start(), state)
    assert out.unit_status == testing.ActiveStatus("Ready")


@patch.object(RawfileLocalPVOperatorCharm, "_client", new_callable=PropertyMock)
def test_blocks_when_missing_ns_not_managed(mock_client):
    fake_client = MagicMock()
    fake_client.get.side_effect = FakeApiError()
    mock_client.return_value = fake_client

    ns = "my-namespace"
    ctx = testing.Context(RawfileLocalPVOperatorCharm)
    state = testing.State(config={"create-namespace": False, "namespace": ns})
    out = ctx.run(ctx.on.config_changed(), state)
    assert out.unit_status == testing.BlockedStatus(f"Missing namespace '{ns}'")


@patch("ops.manifests.Manifests.client", new_callable=PropertyMock)
@patch.object(RawfileLocalPVOperatorCharm, "_client", new_callable=PropertyMock)
def test_create_namespace(mock_client, _):
    fake_client = MagicMock()
    fake_client.get.side_effect = FakeApiError()
    mock_client.return_value = fake_client

    ns = "my-namespace"
    ctx = testing.Context(RawfileLocalPVOperatorCharm)
    state = testing.State(config={"create-namespace": True, "namespace": ns})
    out = ctx.run(ctx.on.config_changed(), state)
    assert out.unit_status == testing.ActiveStatus("Ready")


@patch.object(RawfileLocalPVOperatorCharm, "_client", new_callable=PropertyMock)
@patch("ops.manifests.Manifests.delete_manifests", new_callable=MagicMock())
def test_remove_manifests(mock_delete: MagicMock, _: PropertyMock):
    fake_client = MagicMock()
    fake_client.get.side_effect = ManifestClientError
    fake_client.list.return_value = []
    fake_client.delete.return_value = None

    ctx = testing.Context(RawfileLocalPVOperatorCharm)
    state = testing.State(leader=True)
    out = ctx.run(ctx.on.remove(), state)

    mock_delete.assert_called_once()
    stored_state = next(
        (
            stored
            for stored in out.stored_states
            if stored.owner_path == "RawfileLocalPVOperatorCharm"
        ),
        None,
    )
    assert stored_state
    assert stored_state.content.get("is_terminating")


@patch.object(RawfileLocalPVOperatorCharm, "_client", new_callable=PropertyMock)
@patch("ops.manifests.Manifests.delete_manifests", new_callable=MagicMock())
def test_remove_manifests_non_leader(mock_delete: MagicMock, _: PropertyMock):
    fake_client = MagicMock()
    fake_client.get.side_effect = ManifestClientError
    fake_client.list.return_value = []
    fake_client.delete.return_value = None

    ctx = testing.Context(RawfileLocalPVOperatorCharm)
    state = testing.State(leader=False)
    out = ctx.run(ctx.on.remove(), state)

    mock_delete.assert_not_called()
    stored_state = next(
        (
            stored
            for stored in out.stored_states
            if stored.owner_path == "RawfileLocalPVOperatorCharm"
        ),
        None,
    )
    assert stored_state
    assert stored_state.content.get("is_terminating")


@patch.object(RawfileLocalPVOperatorCharm, "_client", new_callable=PropertyMock)
@patch("ops.manifests.Manifests.client", new_callable=PropertyMock)
def test_apply_manifests(mock_manifest_client: MagicMock, _: PropertyMock):
    fake_client = MagicMock()
    fake_client.apply.side_effect = ManifestClientError("Foo!")
    fake_client.get.side_effect = FakeApiError()
    fake_client.list.return_value = []
    fake_client.delete.return_value = None
    fake_client.send.return_value = []

    mock_manifest_client.return_value = fake_client

    ctx = testing.Context(RawfileLocalPVOperatorCharm)
    state = testing.State(leader=True, config={"create-namespace": True})
    out = ctx.run(ctx.on.config_changed(), state)

    assert out.unit_status == testing.WaitingStatus("Foo!")


@patch.object(RawfileLocalPVOperatorCharm, "_client", new_callable=PropertyMock)
@patch("ops.manifests.Manifests.client", new_callable=PropertyMock)
def test_conflicts(mock_manifest_client: MagicMock, _: PropertyMock):
    fake_client = MagicMock()
    fake_ds = DaemonSet(metadata=ObjectMeta(name="rawfile-csi-node", namespace="foo"))

    def get_side_effect(resource_type, name, namespace=None, **kwargs):
        if resource_type is DaemonSet and name == "rawfile-csi-node" and namespace == "foo":
            return fake_ds
        raise FakeApiError()

    fake_client.get.side_effect = get_side_effect
    fake_client.list.return_value = []
    fake_client.delete.return_value = None
    fake_client.send.return_value = []

    mock_manifest_client.return_value = fake_client

    ctx = testing.Context(RawfileLocalPVOperatorCharm)
    state = testing.State(leader=True, config={"create-namespace": True, "namespace": "foo"})
    out = ctx.run(ctx.on.config_changed(), state)

    assert out.unit_status == testing.BlockedStatus(
        "1 Kubernetes resource collision (action: list-resources)"
    )
