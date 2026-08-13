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

    def create_job(self, job_request_obj: k8s.V1Job) -> k8s.V1Job:
        """Adopt an active prior-attempt Job instead of duplicating its worker."""
        prior_job = self._active_prior_attempt_job(job_request_obj)
        if prior_job is not None:
            self.log.info(
                "Adopting active prior-attempt Job %s in namespace %s",
                prior_job.metadata.name,
                prior_job.metadata.namespace,
            )
            return prior_job
        return super().create_job(job_request_obj)

    def _active_prior_attempt_job(self, job_request_obj: k8s.V1Job) -> k8s.V1Job | None:
        labels = getattr(
            getattr(getattr(job_request_obj, "spec", None), "template", None),
            "metadata",
            None,
        )
        labels = getattr(labels, "labels", None) or {}
        current_try = str(labels.get("try_number") or "")
        if not current_try or current_try == "1":
            return None

        identity_keys = ("dag_id", "task_id", "run_id", "kubernetes_pod_operator")
        if any(not labels.get(key) for key in identity_keys):
            raise AirflowException(
                "retry Job is missing the logical Airflow workload identity labels"
            )
        label_selector = ",".join(f"{key}={labels[key]}" for key in identity_keys)
        namespace = job_request_obj.metadata.namespace
        pods = self.client.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector,
        ).items

        jobs_by_name: dict[str, k8s.V1Job] = {}
        for pod in pods:
            pod_labels = getattr(pod.metadata, "labels", None) or {}
            if str(pod_labels.get("try_number") or "") == current_try:
                continue
            if getattr(pod.status, "phase", None) not in {"Pending", "Running"}:
                continue
            owner_job_name = next(
                (
                    reference.name
                    for reference in (pod.metadata.owner_references or [])
                    if reference.controller and reference.kind == "Job"
                ),
                None,
            )
            if not owner_job_name:
                raise AirflowException(
                    f"active orphan pod {pod.metadata.name} has no owning Job; "
                    "refusing to create a concurrent retry worker"
                )
            try:
                owner_job = self.job_client.read_namespaced_job(
                    name=owner_job_name,
                    namespace=namespace,
                )
            except Exception as error:
                if getattr(error, "status", None) != 404:
                    raise
                raise AirflowException(
                    f"active orphan pod {pod.metadata.name} references missing Job "
                    f"{owner_job_name}; refusing to create a concurrent retry worker"
                ) from error
            conditions = getattr(owner_job.status, "conditions", None) or []
            terminal = any(
                getattr(condition, "status", None) == "True"
                and getattr(condition, "type", None) in {"Complete", "Failed"}
                for condition in conditions
            )
            if not terminal:
                jobs_by_name[owner_job_name] = owner_job

        if len(jobs_by_name) > 1:
            names = ", ".join(sorted(jobs_by_name))
            raise AirflowException(
                f"multiple active prior-attempt Jobs found ({names}); refusing another duplicate"
            )
        return next(iter(jobs_by_name.values()), None)

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
