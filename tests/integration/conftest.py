# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import re
from pathlib import Path
from typing import Generator

import jubilant
import pytest
import yaml

log = logging.getLogger(__name__)

K8S_CONSTRAINTS = {
    "cores": "2",
    "mem": "8G",
    "root-disk": "16G",
    "virt-type": "virtual-machine",
}


def pytest_addoption(parser: pytest.Parser):
    """Parse additional pytest options.

    --charm-file
        This option can be used multiple times to specify local charm files
        that should be made available. The expected filename format is:
        `{charmName}_{base}-{arch}.charm`.
        For example: `rawfile-localpv_ubuntu-22.04-amd64.charm`
        Since Rawfile LocalPV is a subordinate charm, its base is expected
        to match the base of the Kubernetes charms it works with.

    --base
        Specifies which Ubuntu base to test against.
        Example: --base ubuntu@22.04

    Args:
        parser: Pytest parser.
    """
    parser.addoption(
        "--charm-file",
        dest="charm_files",
        action="append",
        default=[],
        help=(
            "This option can be used multiple times to specify local charm files"
            "that should be made available. The expected filename format is:"
            r"`{charmName}_{base}-{arch}.charm`."
            "For example: `rawfile-localpv_ubuntu-22.04-amd64.charm`"
            "Since Rawfile LocalPV is a subordinate charm, its base is expected"
            "to match the base of the Kubernetes charms it works with."
        ),
    )
    parser.addoption(
        "--base",
        dest="base",
        default="ubuntu@22.04",
        help=("Specifies which Ubuntu base to test against. Example: --base ubuntu@22.04"),
    )
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    keep_models = bool(request.config.getoption("--keep-models"))

    with jubilant.temp_model(keep=keep_models) as juju:
        juju.wait_timeout = 10 * 60

        yield juju

        if request.session.testsfailed:
            log = juju.debug_log(limit=1000)
            print(log, end="")


@pytest.fixture(scope="module")
def charm_path(request: pytest.FixtureRequest) -> Path:
    """Return the Path to charm file matching the specified base."""
    base = request.config.getoption("base")
    if not base:
        pytest.fail("No --base option provided to pytest.")

    # NOTE: (mateo) The base string uses '@' as a separator, but charmcraft uses
    # '-' instead.
    base = str(base).replace("@", "-")
    charm_files = request.config.getoption("charm_files")
    print("Charm files provided:", charm_files)
    charm_file = next((Path(f) for f in charm_files if base in Path(f).name), None)
    if not charm_file:
        pytest.fail(f"No charm file found for base '{base}'. Charm files provided: {charm_files}")

    assert charm_file is not None
    return charm_file.resolve()


@pytest.fixture(scope="module")
def arch(charm_path: Path):
    match = re.search(r"(arm64|amd64)", str(charm_path))
    return match.group(1) if match else "amd64"


@pytest.fixture(scope="module")
def kubernetes_cluster(juju: jubilant.Juju, request: pytest.FixtureRequest, arch: str):
    base = request.config.getoption("base")
    constraints = {**K8S_CONSTRAINTS, "arch": arch}
    juju.deploy(
        charm="k8s",
        channel="latest/edge",
        constraints=constraints,
        base=base,
        config={"local-storage-enabled": False, "node-labels": "storagePool=primary"},
        num_units=2,
    )
    juju.deploy(
        charm="k8s-worker",
        channel="latest/edge",
        constraints=constraints,
        base=base,
        config={"node-labels": "storagePool=secondary"},
    )
    juju.integrate("k8s", "k8s-worker:cluster")
    juju.integrate("k8s", "k8s-worker:containerd")
    juju.integrate("k8s", "k8s-worker:cos-tokens")
    juju.wait(jubilant.all_active)

    yield juju.model


@pytest.fixture(scope="module")
def kubeconfig(
    juju: jubilant.Juju, tmp_path_factory: pytest.TempPathFactory
) -> Generator[Path, None, None]:
    tmp_dir = tmp_path_factory.mktemp("kubernetes")
    kubeconfig_path = tmp_dir / "kubeconfig"

    try:
        task = juju.run("k8s/0", "get-kubeconfig", wait=60)
    except jubilant.TaskError as e:
        pytest.fail(f"Failed to get kubeconfig: {e}")

    kubeconfig = task.results.get("kubeconfig")
    if not kubeconfig:
        log.error("'get-kubeconfig' action results: %s", yaml.safe_dump(task.results))
        pytest.fail("Failed to copy kubeconfig from k8s")

    kubeconfig_path.write_text(kubeconfig)
    yield kubeconfig_path
