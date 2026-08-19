"""Kubernetes Job operator with time-bounded pod discovery."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from time import monotonic, sleep
from typing import Any, cast

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.common.compat.sdk import AirflowException
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError

from dags.airflow_pod_cleanup import (
    persist_airflow_pod_termination_receipt,
    requires_airflow_pod_termination_receipt,
)

DEFAULT_JOB_POD_DISCOVERY_TIMEOUT_SECONDS = 6 * 60 * 60
_REMOTE_COMPUTE_SELECTOR_KEY = "adapstory.com/compute-class"
_REMOTE_COMPUTE_SELECTOR_VALUE = "remote"
_XCOM_SIDECAR_NAME = "airflow-xcom-sidecar"
_XCOM_EPHEMERAL_STORAGE_REQUEST = "32Mi"
_XCOM_EPHEMERAL_STORAGE_LIMIT = "128Mi"
_CONTROL_PLANE_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class ResilientKubernetesHook(KubernetesHook):
    """Observe a running Job across bounded Kubernetes control-plane outages."""

    def __init__(
        self,
        *,
        control_plane_observation_timeout_seconds: float = 5 * 60,
        control_plane_observation_initial_backoff_seconds: float = 1,
        control_plane_observation_max_backoff_seconds: float = 30,
        **kwargs: Any,
    ) -> None:
        if control_plane_observation_timeout_seconds <= 0:
            raise ValueError("control_plane_observation_timeout_seconds must be greater than zero")
        if control_plane_observation_initial_backoff_seconds <= 0:
            raise ValueError(
                "control_plane_observation_initial_backoff_seconds must be greater than zero"
            )
        if (
            control_plane_observation_max_backoff_seconds
            < control_plane_observation_initial_backoff_seconds
        ):
            raise ValueError(
                "control_plane_observation_max_backoff_seconds must be at least the initial backoff"
            )
        self.control_plane_observation_timeout_seconds = control_plane_observation_timeout_seconds
        self.control_plane_observation_initial_backoff_seconds = (
            control_plane_observation_initial_backoff_seconds
        )
        self.control_plane_observation_max_backoff_seconds = (
            control_plane_observation_max_backoff_seconds
        )
        super().__init__(**kwargs)

    def wait_until_job_complete(
        self, job_name: str, namespace: str, job_poll_interval: float = 10
    ) -> k8s.V1Job:
        observation_deadline: float | None = None
        backoff_seconds = self.control_plane_observation_initial_backoff_seconds
        while True:
            self.log.info("Requesting status for the job '%s' ", job_name)
            try:
                job = self.get_job_status(job_name=job_name, namespace=namespace)
            except (ApiException, HTTPError, ConnectionError, TimeoutError) as error:
                if isinstance(error, ApiException) and (
                    error.status not in _CONTROL_PLANE_RETRYABLE_STATUSES
                ):
                    raise
                if observation_deadline is None:
                    observation_deadline = (
                        monotonic() + self.control_plane_observation_timeout_seconds
                    )
                remaining_seconds = observation_deadline - monotonic()
                if remaining_seconds <= 0:
                    raise AirflowException(
                        "control_plane_observation_unavailable: Kubernetes control-plane "
                        "observation remained unavailable for "
                        f"{self.control_plane_observation_timeout_seconds:g} seconds"
                    ) from error
                delay = min(backoff_seconds, remaining_seconds)
                self.log.warning(
                    "Kubernetes control-plane observation unavailable for Job %s; "
                    "retrying in %.1f seconds",
                    job_name,
                    delay,
                )
                sleep(delay)
                backoff_seconds = min(
                    backoff_seconds * 2,
                    self.control_plane_observation_max_backoff_seconds,
                )
                continue
            observation_deadline = None
            backoff_seconds = self.control_plane_observation_initial_backoff_seconds
            if self.is_job_complete(job=job):
                return job
            self.log.info(
                "The job '%s' is incomplete. Sleeping for %i sec.",
                job_name,
                job_poll_interval,
            )
            sleep(job_poll_interval)


class _TerminationReceiptCleanupMixin:
    """Seal failed/evicted pod evidence in the operator cleanup critical section."""

    client: Any

    def cleanup(
        self,
        pod: k8s.V1Pod,
        remote_pod: k8s.V1Pod,
        xcom_result: Any = None,
        context: Any = None,
    ) -> None:
        if remote_pod is not None and requires_airflow_pod_termination_receipt(remote_pod):
            namespace = str(remote_pod.metadata.namespace)
            persist_airflow_pod_termination_receipt(
                core_api=self.client,
                pod=remote_pod,
                namespace=namespace,
            )
        super().cleanup(pod, remote_pod, xcom_result=xcom_result, context=context)  # type: ignore[misc]


class ReceiptKubernetesPodOperator(_TerminationReceiptCleanupMixin, KubernetesPodOperator):  # type: ignore[misc]
    """KPO that cannot delete a failed pod before receipt read-back."""


class BoundedKubernetesJobOperator(_TerminationReceiptCleanupMixin, KubernetesJobOperator):  # type: ignore[misc]
    """Wait for the Job controller to create observable pods."""

    def __init__(
        self,
        *,
        pod_discovery_timeout_seconds: float = DEFAULT_JOB_POD_DISCOVERY_TIMEOUT_SECONDS,
        pod_discovery_poll_interval_seconds: float = 1,
        control_plane_observation_timeout_seconds: float = 5 * 60,
        control_plane_observation_initial_backoff_seconds: float = 1,
        control_plane_observation_max_backoff_seconds: float = 30,
        **kwargs: Any,
    ) -> None:
        if pod_discovery_timeout_seconds <= 0:
            raise ValueError("pod_discovery_timeout_seconds must be greater than zero")
        if pod_discovery_poll_interval_seconds <= 0:
            raise ValueError("pod_discovery_poll_interval_seconds must be greater than zero")
        self.pod_discovery_timeout_seconds = pod_discovery_timeout_seconds
        self.pod_discovery_poll_interval_seconds = pod_discovery_poll_interval_seconds
        self.control_plane_observation_timeout_seconds = control_plane_observation_timeout_seconds
        self.control_plane_observation_initial_backoff_seconds = (
            control_plane_observation_initial_backoff_seconds
        )
        self.control_plane_observation_max_backoff_seconds = (
            control_plane_observation_max_backoff_seconds
        )
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> ResilientKubernetesHook:
        return ResilientKubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
            control_plane_observation_timeout_seconds=(
                self.control_plane_observation_timeout_seconds
            ),
            control_plane_observation_initial_backoff_seconds=(
                self.control_plane_observation_initial_backoff_seconds
            ),
            control_plane_observation_max_backoff_seconds=(
                self.control_plane_observation_max_backoff_seconds
            ),
        )

    def create_job(self, job_request_obj: k8s.V1Job) -> k8s.V1Job:
        """Adopt an active prior-attempt Job instead of duplicating its worker."""
        self._harden_remote_xcom_sidecar(job_request_obj)
        prior_job = self._active_prior_attempt_job(job_request_obj)
        if prior_job is not None:
            self.log.info(
                "Adopting active prior-attempt Job %s in namespace %s",
                prior_job.metadata.name,
                prior_job.metadata.namespace,
            )
            return prior_job
        return super().create_job(job_request_obj)

    @staticmethod
    def _harden_remote_xcom_sidecar(job_request_obj: k8s.V1Job) -> None:
        """Make Airflow's injected sidecar admissible on the tainted remote node."""

        job_spec = getattr(job_request_obj, "spec", None)
        template = getattr(job_spec, "template", None)
        pod_spec = getattr(template, "spec", None)
        if pod_spec is None:
            return
        if (pod_spec.node_selector or {}).get(_REMOTE_COMPUTE_SELECTOR_KEY) != (
            _REMOTE_COMPUTE_SELECTOR_VALUE
        ):
            return
        for container in pod_spec.containers:
            if container.name != _XCOM_SIDECAR_NAME:
                continue
            resources = container.resources or k8s.V1ResourceRequirements()
            requests = dict(resources.requests or {})
            limits = dict(resources.limits or {})
            requests.setdefault("ephemeral-storage", _XCOM_EPHEMERAL_STORAGE_REQUEST)
            limits.setdefault("ephemeral-storage", _XCOM_EPHEMERAL_STORAGE_LIMIT)
            resources.requests = requests
            resources.limits = limits
            container.resources = resources

            security_context = container.security_context or k8s.V1SecurityContext()
            security_context.allow_privilege_escalation = False
            security_context.capabilities = k8s.V1Capabilities(drop=["ALL"])
            security_context.read_only_root_filesystem = True
            security_context.run_as_group = 65532
            security_context.run_as_non_root = True
            security_context.run_as_user = 65532
            container.security_context = security_context
            return

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
