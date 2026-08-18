from __future__ import annotations

import importlib
import json
import sys
from datetime import UTC, datetime, timedelta
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest
from kubernetes.client.rest import ApiException

pytestmark = pytest.mark.unit


def _install_airflow_job_operator_stub(monkeypatch: pytest.MonkeyPatch) -> type[Exception]:
    class AirflowException(Exception):
        pass

    class KubernetesJobOperator:
        template_fields: tuple[str, ...] = ()

        def __init__(self, *, parallelism: int = 1, **_: object) -> None:
            self.parallelism = parallelism
            self.client: object | None = None
            self.logged_pods: list[object] = []
            self.created_jobs: list[object] = []
            self.cleanup_calls: list[tuple[object, object]] = []
            self.log = SimpleNamespace(info=lambda *_args: None)

        def create_job(self, job_request_obj: object) -> object:
            self.created_jobs.append(job_request_obj)
            return job_request_obj

        def _build_find_pod_label_selector(
            self, context: object, *, exclude_checked: bool = True
        ) -> str:
            assert context == {"run_id": "delayed-pod"}
            assert exclude_checked is True
            return "job-name=delayed-job"

        def log_matching_pod(self, *, pod: object, context: object) -> None:
            assert context == {"run_id": "delayed-pod"}
            self.logged_pods.append(pod)

        def cleanup(
            self,
            pod: object,
            remote_pod: object,
            xcom_result: object = None,
            context: object = None,
        ) -> None:
            del xcom_result, context
            self.cleanup_calls.append((pod, remote_pod))

    class KubernetesPodOperator(KubernetesJobOperator):
        pass

    modules = {
        "airflow": ModuleType("airflow"),
        "airflow.providers": ModuleType("airflow.providers"),
        "airflow.providers.cncf": ModuleType("airflow.providers.cncf"),
        "airflow.providers.cncf.kubernetes": ModuleType("airflow.providers.cncf.kubernetes"),
        "airflow.providers.cncf.kubernetes.operators": ModuleType(
            "airflow.providers.cncf.kubernetes.operators"
        ),
        "airflow.providers.cncf.kubernetes.operators.job": ModuleType(
            "airflow.providers.cncf.kubernetes.operators.job"
        ),
        "airflow.providers.cncf.kubernetes.operators.pod": ModuleType(
            "airflow.providers.cncf.kubernetes.operators.pod"
        ),
        "airflow.providers.common": ModuleType("airflow.providers.common"),
        "airflow.providers.common.compat": ModuleType("airflow.providers.common.compat"),
        "airflow.providers.common.compat.sdk": ModuleType("airflow.providers.common.compat.sdk"),
    }
    cast(
        Any, modules["airflow.providers.cncf.kubernetes.operators.job"]
    ).KubernetesJobOperator = KubernetesJobOperator
    cast(
        Any, modules["airflow.providers.cncf.kubernetes.operators.pod"]
    ).KubernetesPodOperator = KubernetesPodOperator
    cast(Any, modules["airflow.providers.common.compat.sdk"]).AirflowException = AirflowException
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    return AirflowException


def test_operator_cleanup_persists_failed_pod_receipt_before_parent_deletion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")
    events: list[str] = []
    monkeypatch.setattr(
        module,
        "persist_airflow_pod_termination_receipt",
        lambda **_kwargs: events.append("receipt-read-back"),
    )
    operator = module.ReceiptKubernetesPodOperator(task_id="failed-cleanup")
    operator.client = object()
    remote_pod = SimpleNamespace(
        metadata=SimpleNamespace(name="failed", namespace="airflow"),
        status=SimpleNamespace(phase="Failed", reason="Evicted"),
    )

    operator.cleanup(object(), remote_pod)

    assert events == ["receipt-read-back"]
    assert operator.cleanup_calls == [(operator.cleanup_calls[0][0], remote_pod)]


def test_operator_cleanup_fails_closed_when_receipt_read_back_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")

    def fail(**_kwargs: object) -> None:
        raise ValueError("exact read-back failed")

    monkeypatch.setattr(module, "persist_airflow_pod_termination_receipt", fail)
    operator = module.BoundedKubernetesJobOperator(task_id="failed-cleanup")
    operator.client = object()
    remote_pod = SimpleNamespace(
        metadata=SimpleNamespace(name="failed", namespace="airflow"),
        status=SimpleNamespace(phase="Failed", reason="Evicted"),
    )

    with pytest.raises(ValueError, match="exact read-back failed"):
        operator.cleanup(object(), remote_pod)
    assert operator.cleanup_calls == []


class _DelayedPodClient:
    def __init__(self, responses: list[list[object]]) -> None:
        self._responses = iter(responses)
        self.calls: list[tuple[str, str]] = []

    def list_namespaced_pod(self, *, namespace: str, label_selector: str) -> object:
        self.calls.append((namespace, label_selector))
        return SimpleNamespace(items=next(self._responses))


class _ExistingJobClient:
    def __init__(self, jobs: dict[str, object]) -> None:
        self.jobs = jobs
        self.reads: list[tuple[str, str]] = []

    def read_namespaced_job(self, *, name: str, namespace: str) -> object:
        self.reads.append((name, namespace))
        if name not in self.jobs:
            raise ApiException(status=404, reason="Not Found")
        return self.jobs[name]


def _job_request(*, try_number: str = "2") -> object:
    return SimpleNamespace(
        metadata=SimpleNamespace(name="job-new-attempt", namespace="airflow"),
        spec=SimpleNamespace(
            template=SimpleNamespace(
                metadata=SimpleNamespace(
                    labels={
                        "dag_id": "serp_benchmark_improvement_wave",
                        "task_id": "build_pack_side_swe_bench_verified_candidate",
                        "run_id": "manual__critical_path_preflight__source__000046",
                        "kubernetes_pod_operator": "True",
                        "try_number": try_number,
                    }
                )
            )
        ),
    )


def _active_prior_attempt_pod(*, owner_job: str | None) -> object:
    owner_references = []
    if owner_job:
        owner_references.append(SimpleNamespace(controller=True, kind="Job", name=owner_job))
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="prior-worker",
            namespace="airflow",
            labels={"try_number": "1"},
            owner_references=owner_references,
        ),
        status=SimpleNamespace(phase="Running"),
    )


def test_job_operator_retry_adopts_active_prior_attempt_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")
    prior_job = SimpleNamespace(
        metadata=SimpleNamespace(name="prior-job", namespace="airflow"),
        status=SimpleNamespace(active=1, conditions=[]),
    )
    operator = module.BoundedKubernetesJobOperator(task_id="retry-adoption")
    operator.client = _DelayedPodClient([[_active_prior_attempt_pod(owner_job="prior-job")]])
    operator.job_client = _ExistingJobClient({"prior-job": prior_job})

    result = operator.create_job(_job_request())

    assert result is prior_job
    assert operator.created_jobs == []


def test_remote_job_hardens_and_budgets_the_xcom_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")
    job = module.k8s.V1Job(
        metadata=module.k8s.V1ObjectMeta(name="remote-job", namespace="airflow"),
        spec=module.k8s.V1JobSpec(
            template=module.k8s.V1PodTemplateSpec(
                metadata=module.k8s.V1ObjectMeta(labels={"try_number": "1"}),
                spec=module.k8s.V1PodSpec(
                    containers=[
                        module.k8s.V1Container(name="base"),
                        module.k8s.V1Container(name="airflow-xcom-sidecar"),
                    ],
                    node_selector={"adapstory.com/compute-class": "remote"},
                ),
            )
        ),
    )
    operator = module.BoundedKubernetesJobOperator(task_id="remote-xcom")

    result = operator.create_job(job)

    sidecar = result.spec.template.spec.containers[1]
    assert sidecar.resources.requests["ephemeral-storage"] == "32Mi"
    assert sidecar.resources.limits["ephemeral-storage"] == "128Mi"
    assert sidecar.security_context.allow_privilege_escalation is False
    assert sidecar.security_context.capabilities.drop == ["ALL"]


def test_job_operator_retry_rejects_active_orphan_before_creating_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    airflow_exception = _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")
    operator = module.BoundedKubernetesJobOperator(task_id="retry-orphan")
    operator.client = _DelayedPodClient([[_active_prior_attempt_pod(owner_job="deleted-job")]])
    operator.job_client = _ExistingJobClient({})

    with pytest.raises(airflow_exception, match="active orphan pod prior-worker"):
        operator.create_job(_job_request())

    assert operator.created_jobs == []


def test_job_operator_waits_between_polls_until_delayed_pod_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")

    clock = SimpleNamespace(now=100.0)
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(module, "monotonic", lambda: clock.now)
    monkeypatch.setattr(module, "sleep", sleep)
    pod = SimpleNamespace(metadata=SimpleNamespace(name="delayed-pod"))
    client = _DelayedPodClient([[], [], [pod]])
    operator = module.BoundedKubernetesJobOperator(
        task_id="delayed-pod",
        pod_discovery_timeout_seconds=5,
        pod_discovery_poll_interval_seconds=2,
    )
    operator.client = client

    result = operator.get_pods(
        SimpleNamespace(metadata=SimpleNamespace(namespace="airflow")),
        {"run_id": "delayed-pod"},
    )

    assert result == [pod]
    assert sleeps == [2, 2]
    assert len(client.calls) == 3
    assert operator.logged_pods == [pod]


def test_job_operator_observes_quota_delayed_pod_after_old_sixty_second_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Context: live quota admission delayed Job-controller pod creation for
    # roughly 74 seconds while the owning Job remained valid and later completed.
    # Decision: preserve a finite discovery deadline, but prove the governed
    # D19 bound observes reconciliation beyond the former 60-second cutoff.
    # Reason: retrying a successfully completing Job creates cleanup races and
    # consumes the exact-nine retry budget without changing the workload.
    # Revisit when: Kubernetes exposes an event-driven Job pod watch contract.
    _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")
    clock = SimpleNamespace(now=100.0)
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(module, "monotonic", lambda: clock.now)
    monkeypatch.setattr(module, "sleep", sleep)
    pod = SimpleNamespace(metadata=SimpleNamespace(name="quota-delayed-pod"))
    client = _DelayedPodClient([*([[]] * 75), [pod]])
    operator = module.BoundedKubernetesJobOperator(task_id="quota-delayed-pod")
    operator.client = client

    result = operator.get_pods(
        SimpleNamespace(metadata=SimpleNamespace(namespace="airflow")),
        {"run_id": "delayed-pod"},
    )

    assert result == [pod]
    assert sum(sleeps) > 60


def test_job_operator_pod_discovery_has_a_bounded_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    airflow_exception = _install_airflow_job_operator_stub(monkeypatch)
    sys.modules.pop("dags.serp_kubernetes_job_operator", None)
    module = importlib.import_module("dags.serp_kubernetes_job_operator")

    clock = SimpleNamespace(now=10.0)
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(module, "monotonic", lambda: clock.now)
    monkeypatch.setattr(module, "sleep", sleep)
    operator = module.BoundedKubernetesJobOperator(
        task_id="missing-pod",
        pod_discovery_timeout_seconds=5,
        pod_discovery_poll_interval_seconds=2,
    )
    operator.client = _DelayedPodClient([[], [], [], []])

    with pytest.raises(airflow_exception, match="within 5 seconds"):
        operator.get_pods(
            SimpleNamespace(metadata=SimpleNamespace(namespace="airflow")),
            {"run_id": "delayed-pod"},
        )

    assert sleeps == [2, 2, 1]


def _pod(
    name: str,
    *,
    phase: str = "Succeeded",
    reason: str | None = None,
    owner_job: str | None = None,
    age_minutes: int = 10,
) -> object:
    owner_references = []
    if owner_job:
        owner_references.append(
            SimpleNamespace(api_version="batch/v1", controller=True, kind="Job", name=owner_job)
        )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace="airflow",
            uid=f"00000000-0000-4000-8000-{len(name):012d}",
            creation_timestamp=datetime.now(UTC) - timedelta(minutes=age_minutes),
            deletion_timestamp=None,
            owner_references=owner_references,
        ),
        spec=SimpleNamespace(restart_policy="Never", node_name="adapstory"),
        status=SimpleNamespace(
            phase=phase,
            reason=reason,
            message="node pressure evicted the pod" if reason == "Evicted" else None,
            container_statuses=[
                SimpleNamespace(
                    name="base",
                    ready=False,
                    restart_count=0,
                    state=SimpleNamespace(
                        terminated=SimpleNamespace(
                            exit_code=137,
                            reason="OOMKilled",
                            message="container exceeded memory limit",
                            signal=9,
                            started_at=datetime(2026, 8, 16, 10, 0, tzinfo=UTC),
                            finished_at=datetime(2026, 8, 16, 10, 5, tzinfo=UTC),
                        )
                    ),
                )
            ],
        ),
    )


class _CoreApi:
    def __init__(
        self,
        pods: list[object],
        *,
        receipt_failure: ApiException | None = None,
        corrupt_receipt_readback: bool = False,
    ) -> None:
        self.pods = pods
        self.deleted: list[str] = []
        self.receipt_failure = receipt_failure
        self.corrupt_receipt_readback = corrupt_receipt_readback
        self.receipts: dict[str, Any] = {}
        self.operations: list[str] = []

    def list_namespaced_pod(self, **kwargs: object) -> object:
        assert kwargs["namespace"] == "airflow"
        assert kwargs["label_selector"] == "dag_id,task_id,try_number,airflow_version"
        return SimpleNamespace(items=self.pods, metadata=SimpleNamespace(_continue=None))

    def delete_namespaced_pod(self, *, name: str, namespace: str, body: object) -> object:
        assert namespace == "airflow"
        assert body is not None
        self.operations.append(f"delete:{name}")
        self.deleted.append(name)
        return SimpleNamespace()

    def create_namespaced_config_map(self, *, namespace: str, body: Any, field_manager: str) -> Any:
        assert namespace == "airflow"
        assert field_manager == "airflow-pod-cleanup"
        if self.receipt_failure is not None:
            raise self.receipt_failure
        name = str(body.metadata.name)
        if name in self.receipts:
            raise ApiException(status=409, reason="Already Exists")
        self.operations.append(f"create-receipt:{name}")
        self.receipts[name] = body
        return body

    def read_namespaced_config_map(self, *, name: str, namespace: str) -> Any:
        assert namespace == "airflow"
        self.operations.append(f"read-receipt:{name}")
        if self.corrupt_receipt_readback:
            return SimpleNamespace(
                immutable=True,
                metadata=SimpleNamespace(name=name),
                data={"receipt.json": "{}"},
            )
        return self.receipts[name]


class _BatchApi:
    def __init__(self, existing_jobs: set[str]) -> None:
        self.existing_jobs = existing_jobs
        self.reads: list[str] = []

    def read_namespaced_job(self, *, name: str, namespace: str) -> object:
        assert namespace == "airflow"
        self.reads.append(name)
        if name not in self.existing_jobs:
            raise ApiException(status=404, reason="Not Found")
        return SimpleNamespace(metadata=SimpleNamespace(name=name))


def test_cleanup_protects_pod_while_its_owning_job_exists() -> None:
    from dags.airflow_pod_cleanup import cleanup_airflow_pods

    core_api = _CoreApi(
        [
            _pod("operator-still-observing", owner_job="d19-pack-side"),
            _pod("orphaned-job-pod", owner_job="expired-job"),
            _pod("ordinary-executor-pod"),
        ]
    )

    report = cleanup_airflow_pods(
        core_api=core_api,
        batch_api=_BatchApi({"d19-pack-side"}),
        namespace="airflow",
        now=datetime.now(UTC),
    )

    assert report.protected_job_owned_pods == ("operator-still-observing",)
    assert report.deleted_pods == ("orphaned-job-pod", "ordinary-executor-pod")
    assert core_api.deleted == ["orphaned-job-pod", "ordinary-executor-pod"]


def test_cleanup_persists_and_reads_back_eviction_receipt_before_delete() -> None:
    from dags.airflow_pod_cleanup import cleanup_airflow_pods

    core_api = _CoreApi([_pod("evicted-task", phase="Failed", reason="Evicted")])

    report = cleanup_airflow_pods(
        core_api=core_api,
        batch_api=_BatchApi(set()),
        namespace="airflow",
        now=datetime(2026, 8, 16, 11, 0, tzinfo=UTC),
    )

    receipt_name = "airflow-pod-termination-00000000-0000-4000-8000-000000000012"
    receipt_config_map = core_api.receipts[receipt_name]
    receipt = json.loads(receipt_config_map.data["receipt.json"])
    assert receipt_config_map.immutable is True
    assert (
        receipt_config_map.metadata.annotations["adapstory.com/receipt-sha256"]
        == receipt["receiptSha256"]
    )
    assert receipt["receiptSha256"].startswith("sha256:")
    assert receipt["schema"] == "AirflowPodTerminationReceipt/v1"
    assert receipt["classification"] == "evicted"
    assert receipt["pod"]["name"] == "evicted-task"
    assert receipt["pod"]["uid"] == "00000000-0000-4000-8000-000000000012"
    assert receipt["pod"]["phase"] == "Failed"
    assert receipt["pod"]["reason"] == "Evicted"
    assert receipt["pod"]["containerStatuses"][0]["terminated"] == {
        "exitCode": 137,
        "finishedAt": "2026-08-16T10:05:00Z",
        "message": "container exceeded memory limit",
        "reason": "OOMKilled",
        "signal": 9,
        "startedAt": "2026-08-16T10:00:00Z",
    }
    assert core_api.operations == [
        f"create-receipt:{receipt_name}",
        f"read-receipt:{receipt_name}",
        "delete:evicted-task",
    ]
    assert report.deleted_pods == ("evicted-task",)

    core_api.deleted.clear()
    core_api.operations.clear()
    repeated = cleanup_airflow_pods(
        core_api=core_api,
        batch_api=_BatchApi(set()),
        namespace="airflow",
        now=datetime(2026, 8, 16, 11, 15, tzinfo=UTC),
    )
    assert core_api.operations == [f"read-receipt:{receipt_name}", "delete:evicted-task"]
    assert repeated.deleted_pods == ("evicted-task",)


def test_cleanup_keeps_failed_pod_when_termination_receipt_cannot_be_persisted() -> None:
    from dags.airflow_pod_cleanup import cleanup_airflow_pods

    core_api = _CoreApi(
        [_pod("failed-task", phase="Failed")],
        receipt_failure=ApiException(status=403, reason="Forbidden"),
    )

    with pytest.raises(ApiException, match="Forbidden"):
        cleanup_airflow_pods(
            core_api=core_api,
            batch_api=_BatchApi(set()),
            namespace="airflow",
            now=datetime(2026, 8, 16, 11, 0, tzinfo=UTC),
        )

    assert core_api.deleted == []


def test_cleanup_keeps_failed_pod_when_termination_receipt_readback_differs() -> None:
    from dags.airflow_pod_cleanup import cleanup_airflow_pods

    core_api = _CoreApi([_pod("failed-task", phase="Failed")], corrupt_receipt_readback=True)

    with pytest.raises(ValueError, match="exact read-back failed"):
        cleanup_airflow_pods(
            core_api=core_api,
            batch_api=_BatchApi(set()),
            namespace="airflow",
            now=datetime(2026, 8, 16, 11, 0, tzinfo=UTC),
        )

    assert core_api.deleted == []
