"""Owner-aware cleanup for Airflow Kubernetes pods."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

AIRFLOW_POD_LABEL_SELECTOR = "dag_id,task_id,try_number,airflow_version"


@dataclass(frozen=True)
class CleanupReport:
    deleted_pods: tuple[str, ...]
    protected_job_owned_pods: tuple[str, ...]


def _controller_job_name(pod: Any) -> str | None:
    for owner in pod.metadata.owner_references or ():
        if owner.controller and owner.kind == "Job" and owner.api_version == "batch/v1":
            return str(owner.name)
    return None


def _job_exists(*, batch_api: Any, name: str, namespace: str) -> bool:
    try:
        batch_api.read_namespaced_job(name=name, namespace=namespace)
    except ApiException as exc:
        if exc.status == 404:
            return False
        raise
    return True


def _eligible_for_cleanup(pod: Any, *, now: datetime, min_pending_minutes: int) -> bool:
    phase = (pod.status.phase or "").casefold()
    reason = (pod.status.reason or "").casefold()
    restart_policy = (pod.spec.restart_policy or "").casefold()
    return (
        phase == "succeeded"
        or (phase == "failed" and restart_policy == "never")
        or reason == "evicted"
        or (
            phase == "pending"
            and now - pod.metadata.creation_timestamp
            > timedelta(minutes=max(5, min_pending_minutes))
        )
    )


def cleanup_airflow_pods(
    *,
    core_api: Any,
    batch_api: Any,
    namespace: str,
    min_pending_minutes: int = 5,
    now: datetime | None = None,
    verbose: bool = False,
) -> CleanupReport:
    """Delete eligible Airflow pods without racing a live Job owner."""
    current_time = now or datetime.now(UTC)
    deleted: list[str] = []
    protected: list[str] = []
    list_kwargs: dict[str, Any] = {
        "namespace": namespace,
        "limit": 500,
        "label_selector": AIRFLOW_POD_LABEL_SELECTOR,
    }

    while True:
        pod_page = core_api.list_namespaced_pod(**list_kwargs)
        for pod in pod_page.items:
            pod_name = str(pod.metadata.name)
            job_name = _controller_job_name(pod)
            if job_name and _job_exists(batch_api=batch_api, name=job_name, namespace=namespace):
                protected.append(pod_name)
                if verbose:
                    print(f'Protecting pod "{pod_name}" while owning Job "{job_name}" exists')
                continue
            if not _eligible_for_cleanup(
                pod, now=current_time, min_pending_minutes=min_pending_minutes
            ):
                continue
            if verbose:
                print(f'Deleting pod "{pod_name}"')
            core_api.delete_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                body=client.V1DeleteOptions(),
            )
            deleted.append(pod_name)

        continue_token = pod_page.metadata._continue
        if not continue_token:
            break
        list_kwargs["_continue"] = continue_token

    return CleanupReport(tuple(deleted), tuple(protected))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--min-pending-minutes", type=int, default=5)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    config.load_incluster_config()
    cleanup_airflow_pods(
        core_api=client.CoreV1Api(),
        batch_api=client.BatchV1Api(),
        namespace=args.namespace,
        min_pending_minutes=args.min_pending_minutes,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
