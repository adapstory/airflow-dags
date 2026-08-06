"""Kubernetes Job operator with time-bounded pod discovery."""

from __future__ import annotations

from collections.abc import Sequence
from time import monotonic, sleep
from typing import Any, cast

from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.common.compat.sdk import AirflowException
from kubernetes.client import models as k8s

DEFAULT_JOB_POD_DISCOVERY_TIMEOUT_SECONDS = 6 * 60 * 60


class BoundedKubernetesJobOperator(KubernetesJobOperator):  # type: ignore[misc]
    """Wait for the Job controller to create observable pods."""

    def __init__(
        self,
        *,
        pod_discovery_timeout_seconds: float = DEFAULT_JOB_POD_DISCOVERY_TIMEOUT_SECONDS,
        pod_discovery_poll_interval_seconds: float = 1,
        **kwargs: Any,
    ) -> None:
        if pod_discovery_timeout_seconds <= 0:
            raise ValueError("pod_discovery_timeout_seconds must be greater than zero")
        if pod_discovery_poll_interval_seconds <= 0:
            raise ValueError("pod_discovery_poll_interval_seconds must be greater than zero")
        self.pod_discovery_timeout_seconds = pod_discovery_timeout_seconds
        self.pod_discovery_poll_interval_seconds = pod_discovery_poll_interval_seconds
        super().__init__(**kwargs)

    def get_pods(
        self,
        pod_request_obj: k8s.V1Pod,
        context: Any,
        *,
        exclude_checked: bool = True,
    ) -> Sequence[k8s.V1Pod]:
        """Poll with a real delay until all expected Job pods are visible."""
        label_selector = self._build_find_pod_label_selector(
            context, exclude_checked=exclude_checked
        )
        deadline = monotonic() + self.pod_discovery_timeout_seconds

        while True:
            pod_list = cast(
                Sequence[k8s.V1Pod],
                self.client.list_namespaced_pod(
                    namespace=pod_request_obj.metadata.namespace,
                    label_selector=label_selector,
                ).items,
            )
            if len(pod_list) >= self.parallelism:
                for pod_instance in pod_list:
                    self.log_matching_pod(pod=pod_instance, context=context)
                return pod_list

            remaining_seconds = deadline - monotonic()
            if remaining_seconds <= 0:
                timeout = f"{self.pod_discovery_timeout_seconds:g}"
                raise AirflowException(
                    f"No pods running with labels {label_selector} within {timeout} seconds"
                )
            sleep(min(self.pod_discovery_poll_interval_seconds, remaining_seconds))
