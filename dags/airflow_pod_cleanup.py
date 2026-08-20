"""Owner-aware cleanup for Airflow Kubernetes pods."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

AIRFLOW_POD_LABEL_SELECTOR = "dag_id,task_id,try_number,airflow_version"
AIRFLOW_POD_TERMINATION_RECEIPT_SCHEMA = "AirflowPodTerminationReceipt/v1"
_RECEIPT_DATA_KEY = "receipt.json"
_DNS_LABEL = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")


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


def _rfc3339(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _terminated_state(value: Any) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "exitCode": int(value.exit_code),
        "finishedAt": _rfc3339(value.finished_at),
        "message": value.message,
        "reason": value.reason,
        "signal": int(value.signal or 0),
        "startedAt": _rfc3339(value.started_at),
    }


def _container_statuses(pod: Any) -> list[dict[str, object]]:
    statuses: list[dict[str, object]] = []
    for status in pod.status.container_statuses or ():
        statuses.append(
            {
                "lastTermination": _terminated_state(
                    getattr(getattr(status, "last_state", None), "terminated", None)
                ),
                "name": str(status.name),
                "ready": bool(status.ready),
                "restartCount": int(status.restart_count),
                "terminated": _terminated_state(
                    getattr(getattr(status, "state", None), "terminated", None)
                ),
            }
        )
    return sorted(statuses, key=lambda item: str(item["name"]))


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _termination_receipt(pod: Any) -> dict[str, object]:
    uid = str(pod.metadata.uid)
    phase = str(pod.status.phase or "")
    reason = str(pod.status.reason or "")
    payload: dict[str, object] = {
        "schema": AIRFLOW_POD_TERMINATION_RECEIPT_SCHEMA,
        "classification": "evicted" if reason.casefold() == "evicted" else "terminated",
        "pod": {
            "containerStatuses": _container_statuses(pod),
            "creationTimestamp": _rfc3339(pod.metadata.creation_timestamp),
            "deletionTimestamp": _rfc3339(pod.metadata.deletion_timestamp),
            "message": pod.status.message,
            "name": str(pod.metadata.name),
            "namespace": str(pod.metadata.namespace),
            "nodeName": pod.spec.node_name,
            "phase": phase,
            "reason": reason,
            "uid": uid,
        },
    }
    payload["receiptSha256"] = (
        "sha256:" + hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    )
    return payload


def _termination_receipt_name(pod: Any) -> str:
    uid = str(pod.metadata.uid).casefold()
    suffix = (
        uid
        if len(uid) <= 39 and _DNS_LABEL.fullmatch(uid)
        else hashlib.sha256(uid.encode("utf-8")).hexdigest()[:32]
    )
    return f"airflow-pod-termination-{suffix}"


def persist_airflow_pod_termination_receipt(
    *, core_api: Any, pod: Any, namespace: str
) -> dict[str, Any]:
    """Persist and exactly read back an immutable receipt before pod deletion."""
    receipt = _termination_receipt(pod)
    receipt_json = _canonical_json(receipt)
    receipt_name = _termination_receipt_name(pod)
    body = client.V1ConfigMap(
        immutable=True,
        metadata=client.V1ObjectMeta(
            name=receipt_name,
            namespace=namespace,
            labels={
                "app.kubernetes.io/managed-by": "airflow-pod-cleanup",
                "adapstory.com/evidence-type": "pod-termination",
            },
            annotations={
                "adapstory.com/receipt-sha256": str(receipt["receiptSha256"]),
            },
        ),
        data={_RECEIPT_DATA_KEY: receipt_json},
    )
    try:
        core_api.create_namespaced_config_map(
            namespace=namespace,
            body=body,
            field_manager="airflow-pod-cleanup",
        )
    except ApiException as exc:
        if exc.status != 409:
            raise
    stored = core_api.read_namespaced_config_map(name=receipt_name, namespace=namespace)
    if (
        stored.immutable is not True
        or stored.metadata.name != receipt_name
        or stored.data != {_RECEIPT_DATA_KEY: receipt_json}
    ):
        raise ValueError("Airflow pod termination receipt exact read-back failed")
    return receipt


def requires_airflow_pod_termination_receipt(pod: Any) -> bool:
    return (pod.status.phase or "").casefold() == "failed" or (
        pod.status.reason or ""
    ).casefold() == "evicted"


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
            if requires_airflow_pod_termination_receipt(pod):
                persist_airflow_pod_termination_receipt(
                    core_api=core_api,
                    pod=pod,
                    namespace=namespace,
                )
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
