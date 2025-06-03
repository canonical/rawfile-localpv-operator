import logging
import pprint
import shlex

import pytest
from kubernetes import client, stream, watch
from kubernetes.client.models import EventsV1EventList

log = logging.getLogger(__name__)


def wait_for_pod(
    core_api: client.CoreV1Api,
    name: str,
    namespace: str,
    timeout: int = 120,
    target_state: str = "Running",
) -> None:
    """Wait for kubernetes pod to reach desired state.

    If the state is not reached within specified timeout, pytest.fail is executed.

    :param core_api: instance of CoreV1 kubernetes api.
    :param name: name of the kubernetes pod.
    :param namespace: namespace in which the pod is running
    :param timeout: Maximum seconds to wait for pod to reach target_state
    :param target_state: Expected pod status. (e.g.: Running or Succeeded)
    :return: None
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
        log.debug("%s state: %s", name, pod_state)
        if pod_state == target_state:
            break
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


def read_file_from_pod(
    core_v1: client.CoreV1Api, pod_name: str, file_path: str, namespace="default"
):
    """Read a file from a pod using kubernetes exec."""
    exec_cmd = ["/bin/sh", "-c", f"cat {shlex.quote(file_path)}"]
    try:
        resp = stream.stream(
            core_v1.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=exec_cmd,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
        return resp.strip()
    except Exception as e:
        log.error(f"Failed to read file {file_path} from pod {pod_name}: {e}")
        raise
