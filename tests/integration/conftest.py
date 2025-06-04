import logging
from pathlib import Path
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import yaml
from pytest_operator.plugin import OpsTest

log = logging.getLogger(__name__)

def pytest_addoption(parser: pytest.Parser):
    """Parse additional pytest options.

    --charm-file
        Can be used multiple times, specifies which local charm files are available.
        Expected filename format: {charmName}_{base}-{arch}.charm
        Example: k8s-worker_ubuntu-22.04-amd64_ubuntu-24.04-amd64.charm
        Some tests use subordinate charms (e.g. Ceph) that expect the charm
        base to match.

    Args:
        parser: Pytest parser.
    """
    parser.addoption(
        "--charm-file",
        dest="charm_files",
        action="append",
        default=[],
        help=(
            "Can be used multiple times, specifies which local charm files are available. "
            r"Expected filename format: {charmName}_{base}-{arch}.charm. "
            "Example: k8s-worker_ubuntu-22.04-amd64_ubuntu-24.04-amd64.charm. "
            "Some tests use subordinate charms (e.g. Ceph) that expect the charm "
            "base to match."
        ),
    )

@pytest_asyncio.fixture(scope="module")
async def kubeconfig(ops_test: OpsTest) -> AsyncGenerator[Path, None]:
    control_plane = ops_test.model.applications["k8s"]
    (leader,) = [u for u in control_plane.units if (await u.is_leader_from_status())]
    action = await leader.run_action("get-kubeconfig")
    action = await action.wait()
    success = (
        action.status == "completed"
        and action.results["return-code"] == 0
        and "kubeconfig" in action.results
    )

    if not success:
        log.error(f"status: {action.status}")
        log.error(f"results:\n{yaml.safe_dump(action.results, indent=2)}")
        pytest.fail("Failed to copy kubeconfig from k8s")

    kubeconfig_path = ops_test.tmp_path / "kubeconfig"
    with kubeconfig_path.open("w") as f:
        f.write(action.results["kubeconfig"])
    yield kubeconfig_path
