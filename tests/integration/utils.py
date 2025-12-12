# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import pprint
import shlex
import time
from contextlib import contextmanager
from typing import Generator, List, Optional

import pytest
from kubernetes import client, stream, watch
from kubernetes.client.models import EventsV1EventList
from tenacity import retry, stop_after_attempt, wait_fixed

log = logging.getLogger(__name__)

# Timeout constants (in seconds)
POD_WAIT_TIMEOUT = 300
PVC_WAIT_TIMEOUT = 120
RETRY_INTERVAL = 5
MAX_RETRIES = 10


def wait_for_pvc_bound(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    timeout: int = PVC_WAIT_TIMEOUT,
) -> None:
    """Wait for a PVC to become Bound.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PVC.
        namespace: Namespace of the PVC.
        timeout: Maximum seconds to wait for PVC to become Bound.

    Raises:
        pytest.fail: If PVC does not become Bound within timeout.
    """
    k8s_watch = watch.Watch()
    log.info("Waiting for PVC '%s' in namespace '%s' to become Bound", name, namespace)

    for event in k8s_watch.stream(
        func=core_api.list_namespaced_persistent_volume_claim,
        namespace=namespace,
        timeout_seconds=timeout,
    ):
        resource = event["object"]
        if resource.metadata.name != name:
            continue
        pvc_phase = resource.status.phase
        log.debug("PVC %s phase: %s", name, pvc_phase)
        if pvc_phase == "Bound":
            log.info("PVC '%s' is now Bound", name)
            k8s_watch.stop()
            return

    pytest.fail(f"Timeout after {timeout}s waiting for PVC '{name}' to become Bound")


def wait_for_pod(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    timeout: int = POD_WAIT_TIMEOUT,
    target_state: str = "Running",
) -> None:
    """Wait for kubernetes pod to reach desired state.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the kubernetes pod.
        namespace: Namespace in which the pod is running.
        timeout: Maximum seconds to wait for pod to reach target_state.
        target_state: Expected pod status (e.g.: Running or Succeeded).

    Raises:
        pytest.fail: If pod does not reach target_state within timeout.
    """
    k8s_watch = watch.Watch()

    log.info("Waiting for pod '%s' to reach state '%s'", name, target_state)
    for event in k8s_watch.stream(
        func=core_api.list_namespaced_pod, namespace=namespace, timeout_seconds=timeout
    ):
        resource = event["object"]
        if resource.metadata.name != name:
            continue
        pod_state = resource.status.phase
        log.debug("Pod '%s' state: %s (waiting for %s)", name, pod_state, target_state)
        if pod_state == target_state:
            log.info("Pod '%s' reached target state '%s'", name, target_state)
            k8s_watch.stop()
            return
        if pod_state == "Failed":
            k8s_watch.stop()
            pytest.fail(f"Pod '{name}' entered Failed state")
    else:
        log.info(f"Pod failed to start within allotted timeout: '{timeout}s'")
        events: EventsV1EventList = core_api.list_namespaced_event(
            namespace, field_selector=f"involvedObject.name={name}"
        )
        for event in events.items:
            event_interest = ", ".join(
                [event.type, event.reason, pprint.pformat(event.source), event.message]
            )
            log.info(event_interest)
        pytest.fail(
            "Timeout after {}s while waiting for {} pod to reach {} status".format(
                timeout, name, target_state
            )
        )


@retry(stop=stop_after_attempt(5), wait=wait_fixed(2), reraise=True)
def read_file_from_pod(
    core_v1: client.CoreV1Api,
    pod_name: str,
    file_path: str,
    namespace: str = "default",
    container: Optional[str] = None,
) -> str:
    """Read a file from a pod using kubernetes exec.

    This function includes retry logic to handle transient failures
    that may occur when the pod is still initializing.

    Args:
        core_v1: Instance of CoreV1 kubernetes api.
        pod_name: Name of the pod to read from.
        file_path: Path to the file inside the pod.
        namespace: Namespace of the pod.
        container: Specific container name (optional).

    Returns:
        Contents of the file as a string.

    Raises:
        Exception: If file cannot be read after all retries.
    """
    exec_cmd = ["/bin/sh", "-c", f"cat {shlex.quote(file_path)}"]
    kwargs = {
        "command": exec_cmd,
        "stderr": True,
        "stdin": False,
        "stdout": True,
        "tty": False,
    }
    if container:
        kwargs["container"] = container

    try:
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            **kwargs,
        )
        content = resp.strip()
        log.debug("Read %d bytes from %s:%s", len(content), pod_name, file_path)
        return content
    except Exception as e:
        log.error("Failed to read file %s from pod %s: %s", file_path, pod_name, e)
        raise


@contextmanager
def k8s_resource_cleanup(
    api_client: client.ApiClient,
    resources: List[dict],
    namespace: str = "default",
) -> Generator[None, None, None]:
    """Context manager to ensure K8s resources are cleaned up after tests.

    Args:
        api_client: Kubernetes API client.
        resources: List of resource dicts with 'kind' and 'name' keys.
        namespace: Namespace of the resources.

    Yields:
        None
    """
    core_v1 = client.CoreV1Api(api_client)
    try:
        yield
    finally:
        for resource in resources:
            kind = resource.get("kind", "").lower()
            name = resource.get("name", "")
            try:
                if kind == "pod":
                    core_v1.delete_namespaced_pod(name, namespace, grace_period_seconds=0)
                    log.info("Deleted pod '%s'", name)
                elif kind == "persistentvolumeclaim":
                    core_v1.delete_namespaced_persistent_volume_claim(name, namespace)
                    log.info("Deleted PVC '%s'", name)
            except client.ApiException as e:
                if e.status != 404:
                    log.warning("Failed to delete %s '%s': %s", kind, name, e)


@contextmanager
def managed_storage_class(
    storage_api: client.StorageV1Api,
    name: str,
    provisioner: str,
    reclaim_policy: str = "Delete",
    volume_binding_mode: str = "WaitForFirstConsumer",
    parameters: Optional[dict] = None,
) -> Generator[str, None, None]:
    """Context manager for storage class creation and cleanup.

    Args:
        storage_api: Instance of StorageV1 kubernetes api.
        name: Name of the storage class.
        provisioner: CSI provisioner name.
        reclaim_policy: Reclaim policy ('Delete' or 'Retain').
        volume_binding_mode: Volume binding mode.
        parameters: Optional storage class parameters.

    Yields:
        Name of the created storage class.
    """
    create_storage_class(
        storage_api, name, provisioner, reclaim_policy, volume_binding_mode, parameters
    )
    try:
        yield name
    finally:
        try:
            delete_storage_class(storage_api, name)
        except Exception as e:
            log.warning("Failed to delete storage class '%s': %s", name, e)


@contextmanager
def managed_pvc(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    storage_class: str,
    size: str = "1Gi",
    access_modes: Optional[List[str]] = None,
) -> Generator[str, None, None]:
    """Context manager for PVC creation and cleanup.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PVC.
        namespace: Namespace for the PVC.
        storage_class: Storage class name.
        size: Storage size (e.g., '1Gi').
        access_modes: List of access modes. Defaults to ['ReadWriteOnce'].

    Yields:
        Name of the created PVC.
    """
    create_pvc(core_api, name, namespace, storage_class, size, access_modes)
    try:
        yield name
    finally:
        try:
            delete_pvc(core_api, name, namespace)
        except Exception as e:
            log.warning("Failed to delete PVC '%s': %s", name, e)


@contextmanager
def managed_pod(
    core_api: client.CoreV1Api,
    pod_name: str,
    pvc_name: str,
    namespace: str,
    node_selector: Optional[dict] = None,
    command: Optional[List[str]] = None,
) -> Generator[str, None, None]:
    """Context manager for pod creation and cleanup.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        pod_name: Name of the pod.
        pvc_name: Name of the PVC to mount.
        namespace: Namespace for the pod.
        node_selector: Optional node selector dict.
        command: Optional command to run. Defaults to sleep.

    Yields:
        Name of the created pod.
    """
    create_pod_with_pvc(core_api, pod_name, pvc_name, namespace, node_selector, command)
    try:
        yield pod_name
    finally:
        try:
            delete_pod(core_api, pod_name, namespace)
        except Exception as e:
            log.warning("Failed to delete pod '%s': %s", pod_name, e)


@contextmanager
def managed_pv(
    core_api: client.CoreV1Api,
    pv_name: str,
) -> Generator[str, None, None]:
    """Context manager for PV cleanup.

    This context manager doesn't create a PV (they are created by provisioners),
    but ensures cleanup of the PV after use.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        pv_name: Name of the PV to manage.

    Yields:
        Name of the PV.
    """
    try:
        yield pv_name
    finally:
        try:
            delete_pv(core_api, pv_name)
        except Exception as e:
            log.warning("Failed to delete PV '%s': %s", pv_name, e)


def verify_storage_class_exists(
    storage_api: client.StorageV1Api,
    name: str,
) -> bool:
    """Verify that a storage class exists.

    Args:
        storage_api: Instance of StorageV1 kubernetes api.
        name: Name of the storage class.

    Returns:
        True if the storage class exists, False otherwise.
    """
    try:
        storage_api.read_storage_class(name)
        log.info("Storage class '%s' exists", name)
        return True
    except client.ApiException as e:
        if e.status == 404:
            log.warning("Storage class '%s' not found", name)
            return False
        raise


def wait_for_pvc_deleted(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    timeout: int = PVC_WAIT_TIMEOUT,
) -> None:
    """Wait for a PVC to be deleted.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PVC.
        namespace: Namespace of the PVC.
        timeout: Maximum seconds to wait for PVC deletion.

    Raises:
        pytest.fail: If PVC is not deleted within timeout.
    """
    log.info("Waiting for PVC '%s' to be deleted", name)
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            core_api.read_namespaced_persistent_volume_claim(name, namespace)
            log.debug("PVC '%s' still exists, waiting...", name)
            time.sleep(RETRY_INTERVAL)
        except client.ApiException as e:
            if e.status == 404:
                log.info("PVC '%s' has been deleted", name)
                return
            raise

    pytest.fail(f"Timeout after {timeout}s waiting for PVC '{name}' to be deleted")


def wait_for_pv_deleted(
    core_api: client.CoreV1Api,
    name: str,
    timeout: int = PVC_WAIT_TIMEOUT,
) -> None:
    """Wait for a PV to be deleted.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PV.
        timeout: Maximum seconds to wait for PV deletion.

    Raises:
        pytest.fail: If PV is not deleted within timeout.
    """
    log.info("Waiting for PV '%s' to be deleted", name)
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            core_api.read_persistent_volume(name)
            log.debug("PV '%s' still exists, waiting...", name)
            time.sleep(RETRY_INTERVAL)
        except client.ApiException as e:
            if e.status == 404:
                log.info("PV '%s' has been deleted", name)
                return
            raise

    pytest.fail(f"Timeout after {timeout}s waiting for PV '{name}' to be deleted")


def get_pv_for_pvc(
    core_api: client.CoreV1Api,
    pvc_name: str,
    namespace: str,
) -> Optional[str]:
    """Get the PV name bound to a PVC.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        pvc_name: Name of the PVC.
        namespace: Namespace of the PVC.

    Returns:
        Name of the bound PV, or None if not bound.
    """
    try:
        pvc = core_api.read_namespaced_persistent_volume_claim(pvc_name, namespace)
        pv_name = pvc.spec.volume_name
        log.info("PVC '%s' is bound to PV '%s'", pvc_name, pv_name)
        return pv_name
    except client.ApiException as e:
        if e.status == 404:
            log.warning("PVC '%s' not found", pvc_name)
            return None
        raise


def pv_exists(
    core_api: client.CoreV1Api,
    name: str,
) -> bool:
    """Check if a PV exists.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PV.

    Returns:
        True if the PV exists, False otherwise.
    """
    try:
        core_api.read_persistent_volume(name)
        return True
    except client.ApiException as e:
        if e.status == 404:
            return False
        raise


def get_pv_reclaim_policy(
    core_api: client.CoreV1Api,
    name: str,
) -> Optional[str]:
    """Get the reclaim policy of a PV.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PV.

    Returns:
        The reclaim policy string (e.g., 'Delete', 'Retain'), or None if PV not found.
    """
    try:
        pv = core_api.read_persistent_volume(name)
        return pv.spec.persistent_volume_reclaim_policy
    except client.ApiException as e:
        if e.status == 404:
            return None
        raise


def create_storage_class(
    storage_api: client.StorageV1Api,
    name: str,
    provisioner: str,
    reclaim_policy: str = "Delete",
    volume_binding_mode: str = "WaitForFirstConsumer",
    parameters: Optional[dict] = None,
) -> None:
    """Create a storage class.

    Args:
        storage_api: Instance of StorageV1 kubernetes api.
        name: Name of the storage class.
        provisioner: CSI provisioner name.
        reclaim_policy: Reclaim policy ('Delete' or 'Retain').
        volume_binding_mode: Volume binding mode.
        parameters: Optional storage class parameters.
    """
    sc = client.V1StorageClass(
        api_version="storage.k8s.io/v1",
        kind="StorageClass",
        metadata=client.V1ObjectMeta(name=name),
        provisioner=provisioner,
        reclaim_policy=reclaim_policy,
        volume_binding_mode=volume_binding_mode,
        parameters=parameters or {},
    )
    try:
        storage_api.create_storage_class(sc)
        log.info("Created storage class '%s' with reclaim policy '%s'", name, reclaim_policy)
    except client.ApiException as e:
        if e.status == 409:
            log.warning("Storage class '%s' already exists", name)
        else:
            raise


def delete_storage_class(
    storage_api: client.StorageV1Api,
    name: str,
) -> None:
    """Delete a storage class.

    Args:
        storage_api: Instance of StorageV1 kubernetes api.
        name: Name of the storage class to delete.
    """
    try:
        storage_api.delete_storage_class(name)
        log.info("Deleted storage class '%s'", name)
    except client.ApiException as e:
        if e.status != 404:
            raise


def create_pvc(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    storage_class: str,
    size: str = "1Gi",
    access_modes: Optional[List[str]] = None,
) -> None:
    """Create a PersistentVolumeClaim.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PVC.
        namespace: Namespace for the PVC.
        storage_class: Storage class name.
        size: Storage size (e.g., '1Gi').
        access_modes: List of access modes. Defaults to ['ReadWriteOnce'].
    """
    if access_modes is None:
        access_modes = ["ReadWriteOnce"]

    pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=client.V1ObjectMeta(name=name, namespace=namespace),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=access_modes,
            resources=client.V1VolumeResourceRequirements(requests={"storage": size}),
            storage_class_name=storage_class,
        ),
    )
    try:
        core_api.create_namespaced_persistent_volume_claim(namespace, pvc)
        log.info("Created PVC '%s' in namespace '%s'", name, namespace)
    except client.ApiException as e:
        if e.status == 409:
            log.warning("PVC '%s' already exists", name)
        else:
            raise


def delete_pvc(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
) -> None:
    """Delete a PersistentVolumeClaim.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PVC.
        namespace: Namespace of the PVC.
    """
    try:
        core_api.delete_namespaced_persistent_volume_claim(name, namespace)
        log.info("Deleted PVC '%s'", name)
    except client.ApiException as e:
        if e.status != 404:
            raise


def delete_pv(
    core_api: client.CoreV1Api,
    name: str,
) -> None:
    """Delete a PersistentVolume.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the PV.
    """
    try:
        core_api.delete_persistent_volume(name)
        log.info("Deleted PV '%s'", name)
    except client.ApiException as e:
        if e.status != 404:
            raise


def create_pod_with_pvc(
    core_api: client.CoreV1Api,
    pod_name: str,
    pvc_name: str,
    namespace: str,
    node_selector: Optional[dict] = None,
    command: Optional[List[str]] = None,
) -> None:
    """Create a pod that uses a PVC.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        pod_name: Name of the pod.
        pvc_name: Name of the PVC to mount.
        namespace: Namespace for the pod.
        node_selector: Optional node selector dict.
        command: Optional command to run. Defaults to sleep.
    """
    if command is None:
        command = ["/bin/sh", "-c", "sleep 3600"]

    pod = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(name=pod_name, namespace=namespace),
        spec=client.V1PodSpec(
            node_selector=node_selector,
            containers=[
                client.V1Container(
                    name="main",
                    image="busybox",
                    command=command,
                    volume_mounts=[client.V1VolumeMount(name="data", mount_path="/data")],
                )
            ],
            volumes=[
                client.V1Volume(
                    name="data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=pvc_name
                    ),
                )
            ],
        ),
    )
    try:
        core_api.create_namespaced_pod(namespace, pod)
        log.info("Created pod '%s' with PVC '%s'", pod_name, pvc_name)
    except client.ApiException as e:
        if e.status == 409:
            log.warning("Pod '%s' already exists", pod_name)
        else:
            raise


def delete_pod(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    grace_period: int = 0,
) -> None:
    """Delete a pod.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the pod.
        namespace: Namespace of the pod.
        grace_period: Grace period in seconds for pod termination.
    """
    try:
        core_api.delete_namespaced_pod(name, namespace, grace_period_seconds=grace_period)
        log.info("Deleted pod '%s'", name)
    except client.ApiException as e:
        if e.status != 404:
            raise


def wait_for_pod_deleted(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    timeout: int = POD_WAIT_TIMEOUT,
) -> None:
    """Wait for a pod to be deleted.

    Args:
        core_api: Instance of CoreV1 kubernetes api.
        name: Name of the pod.
        namespace: Namespace of the pod.
        timeout: Maximum seconds to wait for pod deletion.

    Raises:
        pytest.fail: If pod is not deleted within timeout.
    """
    log.info("Waiting for pod '%s' to be deleted", name)
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            core_api.read_namespaced_pod(name, namespace)
            log.debug("Pod '%s' still exists, waiting...", name)
            time.sleep(RETRY_INTERVAL)
        except client.ApiException as e:
            if e.status == 404:
                log.info("Pod '%s' has been deleted", name)
                return
            raise

    pytest.fail(f"Timeout after {timeout}s waiting for pod '{name}' to be deleted")
