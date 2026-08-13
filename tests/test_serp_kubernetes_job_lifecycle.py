from __future__ import annotations

import importlib
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
        "airflow.providers.common": ModuleType("airflow.providers.common"),
        "airflow.providers.common.compat": ModuleType("airflow.providers.common.compat"),
        "airflow.providers.common.compat.sdk": ModuleType("airflow.providers.common.compat.sdk"),
    }
    cast(
        Any, modules["airflow.providers.cncf.kubernetes.operators.job"]
    ).KubernetesJobOperator = KubernetesJobOperator
    cast(Any, modules["airflow.providers.common.compat.sdk"]).AirflowException = AirflowException
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    return AirflowException


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
            creation_timestamp=datetime.now(UTC) - timedelta(minutes=age_minutes),
            owner_references=owner_references,
        ),
        spec=SimpleNamespace(restart_policy="Never"),
        status=SimpleNamespace(phase=phase, reason=None),
    )


class _CoreApi:
    def __init__(self, pods: list[object]) -> None:
        self.pods = pods
        self.deleted: list[str] = []

    def list_namespaced_pod(self, **kwargs: object) -> object:
        assert kwargs["namespace"] == "airflow"
        assert kwargs["label_selector"] == "dag_id,task_id,try_number,airflow_version"
        return SimpleNamespace(items=self.pods, metadata=SimpleNamespace(_continue=None))

    def delete_namespaced_pod(self, *, name: str, namespace: str, body: object) -> object:
        assert namespace == "airflow"
        assert body is not None
        self.deleted.append(name)
        return SimpleNamespace()


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
