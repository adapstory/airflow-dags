from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

from dags.serp_benchmark_catalog import mandatory_benchmark_adapters_ready
from dags.serp_d19_history_observer import (
    AIRFLOW_HISTORY_TRUSTED_BASE_URL,
    KubernetesD19HistoryFenceClient,
)
from dags.serp_eval_contracts import (
    build_nightly_regression_plan,
    default_nightly_regression_conf,
    governance_notification_pending,
    nightly_regression_runtime_ready,
    produce_d19_run_history_observation,
    write_airflow_plan_artifact,
    write_scheduled_d6_regression_receipt,
)
from dags.serp_evidence_workload_identity import (
    hardened_runtime_container_security_context,
    hardened_runtime_pod_security_context,
    hardened_runtime_volume_mounts,
    hardened_runtime_volumes,
    minio_web_identity_env_vars,
    minio_web_identity_executor_config,
    minio_web_identity_volume_mounts,
    minio_web_identity_volumes,
    vault_transit_env_vars,
    vault_transit_volume_mounts,
    vault_transit_volumes,
)

D19_DAG_ID = "serp_benchmark_improvement_wave"
D19_VERIFICATION_TASK_ID = "persist_paired_evaluation_verification_evidence"
D19_HISTORY_OBSERVER_SERVICE_ACCOUNT = "airflow-serp-d19-history-observer"
D19_HISTORY_OBSERVER_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "d19-history-observer",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
_HISTORY_SECRET_ROOT = "/var/run/secrets/adapstory/airflow-history-observer"
_KUBERNETES_SECRET_ROOT = "/var/run/secrets/adapstory/kubernetes-api"

NIGHTLY_EVALUATOR_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name="airflow-serp-benchmark-evaluator",
    labels={
        "adapstory.com/serp-evidence-workload": "true",
        "adapstory.com/serp-network-profile": "benchmark-evaluator",
        "component": "worker",
        "release": "airflow",
        "tier": "airflow",
    },
)

D19_HISTORY_API_CREDENTIALS_VOLUME = k8s.V1Volume(
    name="d19-history-api-credentials",
    secret=k8s.V1SecretVolumeSource(
        secret_name="airflow-serp-d19-history-observer-api",
        items=[
            k8s.V1KeyToPath(key="ca.crt", path="ca.crt"),
            k8s.V1KeyToPath(key="password", path="password"),
            k8s.V1KeyToPath(key="username", path="username"),
        ],
    ),
)
D19_HISTORY_KUBERNETES_API_VOLUME = k8s.V1Volume(
    name="d19-history-kubernetes-api",
    projected=k8s.V1ProjectedVolumeSource(
        sources=[
            k8s.V1VolumeProjection(
                service_account_token=k8s.V1ServiceAccountTokenProjection(
                    expiration_seconds=600,
                    path="token",
                )
            ),
            k8s.V1VolumeProjection(
                config_map=k8s.V1ConfigMapProjection(
                    items=[k8s.V1KeyToPath(key="ca.crt", path="ca.crt")],
                    name="kube-root-ca.crt",
                )
            ),
        ]
    ),
)
D19_HISTORY_OBSERVER_EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels=D19_HISTORY_OBSERVER_LABELS),
        spec=k8s.V1PodSpec(
            automount_service_account_token=False,
            containers=[
                k8s.V1Container(
                    name="base",
                    env=[
                        *minio_web_identity_env_vars(()),
                        *vault_transit_env_vars(
                            auth_role="serp-d19-history-observer-attestor-role"
                        ),
                        k8s.V1EnvVar(
                            name="ADAPSTORY_AIRFLOW_API_BASE_URL",
                            value=AIRFLOW_HISTORY_TRUSTED_BASE_URL,
                        ),
                        k8s.V1EnvVar(
                            name="ADAPSTORY_AIRFLOW_API_CA_FILE",
                            value=f"{_HISTORY_SECRET_ROOT}/ca.crt",
                        ),
                        k8s.V1EnvVar(
                            name="ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_PASSWORD_FILE",
                            value=f"{_HISTORY_SECRET_ROOT}/password",
                        ),
                        k8s.V1EnvVar(
                            name="ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_USERNAME_FILE",
                            value=f"{_HISTORY_SECRET_ROOT}/username",
                        ),
                        k8s.V1EnvVar(
                            name="ADAPSTORY_KUBERNETES_API_CA_FILE",
                            value=f"{_KUBERNETES_SECRET_ROOT}/ca.crt",
                        ),
                        k8s.V1EnvVar(
                            name="ADAPSTORY_KUBERNETES_API_TOKEN_FILE",
                            value=f"{_KUBERNETES_SECRET_ROOT}/token",
                        ),
                    ],
                    security_context=hardened_runtime_container_security_context(),
                    volume_mounts=[
                        *minio_web_identity_volume_mounts(),
                        *vault_transit_volume_mounts(),
                        k8s.V1VolumeMount(
                            name="d19-history-api-credentials",
                            mount_path=_HISTORY_SECRET_ROOT,
                            read_only=True,
                        ),
                        k8s.V1VolumeMount(
                            name="d19-history-kubernetes-api",
                            mount_path=_KUBERNETES_SECRET_ROOT,
                            read_only=True,
                        ),
                        *hardened_runtime_volume_mounts(),
                    ],
                )
            ],
            security_context=hardened_runtime_pod_security_context(),
            service_account_name=D19_HISTORY_OBSERVER_SERVICE_ACCOUNT,
            volumes=[
                *minio_web_identity_volumes(),
                *vault_transit_volumes(),
                D19_HISTORY_API_CREDENTIALS_VOLUME,
                D19_HISTORY_KUBERNETES_API_VOLUME,
                *hardened_runtime_volumes(),
            ],
        ),
    )
}


def validate_nightly_regression_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise ValueError("scheduled D6 requires authoritative DagRun metadata")
    run_type = getattr(dag_run, "run_type", None)
    run_type_value = getattr(run_type, "value", run_type)
    if run_type_value != "scheduled":
        raise ValueError("D6 only admits scheduler-created DagRuns")
    supplied_conf = getattr(dag_run, "conf", None) or {}
    if supplied_conf:
        raise ValueError("scheduled D6 rejects caller-supplied dag_run.conf")
    logical_date = context.get("logical_date") or getattr(dag_run, "logical_date", None)
    if not isinstance(logical_date, datetime | str):
        raise ValueError("scheduled D6 requires a datetime logical_date")
    generated_at = _datetime_string(logical_date, "scheduled D6 logical_date")
    runtime_conf = default_nightly_regression_conf(generated_at=generated_at)
    return write_airflow_plan_artifact(build_nightly_regression_plan(runtime_conf))


def load_triggered_d19_verification(child_run_id: str, **context: Any) -> dict[str, Any]:
    task_instance = context.get("ti")
    if task_instance is None:
        raise ValueError("D6 child verification loader requires TaskInstance context")
    value = task_instance.xcom_pull(
        task_ids=D19_VERIFICATION_TASK_ID,
        dag_id=D19_DAG_ID,
        run_id=child_run_id,
    )
    if not isinstance(value, Mapping):
        raise ValueError("triggered D19 verification XCom is missing or invalid")
    return dict(value)


def observe_triggered_d19_run(
    child_run_id: str,
    logical_date: datetime | str,
    **context: Any,
) -> dict[str, Any]:
    task_instance = context.get("ti")
    if task_instance is None:
        raise ValueError("D6 child observer requires Task SDK TaskInstance context")
    logical_date_value = _datetime_value(logical_date, "D19 child logical_date")
    state = task_instance.get_dagrun_state(D19_DAG_ID, child_run_id)
    state_value = getattr(state, "value", state)
    same_logical_date_count = task_instance.get_dr_count(
        dag_id=D19_DAG_ID,
        logical_dates=[logical_date_value],
    )
    same_logical_date_success_count = task_instance.get_dr_count(
        dag_id=D19_DAG_ID,
        logical_dates=[logical_date_value],
        states=["success"],
    )
    if state_value != "success":
        raise ValueError("triggered D19 child must finish successfully")
    if same_logical_date_count != 1 or same_logical_date_success_count != 1:
        raise ValueError("scheduled D6 requires exactly one successful D19 child per logical date")
    return {
        "dagId": D19_DAG_ID,
        "logicalDate": _datetime_string(logical_date_value, "D19 child logical_date"),
        "observedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "runId": child_run_id,
        "sameLogicalDateRunCount": 1,
        "sameLogicalDateSuccessCount": 1,
        "schema": "D19CurrentRunObservation/v1",
        "state": "success",
    }


def release_d19_history_fence(
    history_result: Mapping[str, Any],
    *,
    fence_client: KubernetesD19HistoryFenceClient | None = None,
) -> dict[str, str]:
    if not isinstance(history_result, Mapping):
        raise ValueError("D6 history result is required to release its fence")
    fence = history_result.get("fence")
    if not isinstance(fence, Mapping):
        raise ValueError("D6 history result fence is missing")
    client = fence_client or KubernetesD19HistoryFenceClient.from_environment()
    client.release(fence)
    return {"status": "released"}


def _datetime_value(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} is invalid") from exc
    else:
        raise ValueError(f"{field_name} is required")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(UTC)


def _datetime_string(value: datetime | str, field_name: str) -> str:
    return _datetime_value(value, field_name).isoformat().replace("+00:00", "Z")


default_args = {
    "owner": "serp-eval-runner",
    "start_date": datetime(2026, 7, 5, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_nightly_regression_suite",
    default_args=default_args,
    description="Scheduled D6 parent for one fenced native D19 evaluation",
    schedule=(
        "@daily"
        if mandatory_benchmark_adapters_ready() and nightly_regression_runtime_ready()
        else None
    ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "d6", "d19"],
)

validate_plan = PythonOperator(
    task_id="validate_nightly_regression_plan",
    python_callable=validate_nightly_regression_plan,
    executor_config=NIGHTLY_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

produce_history = PythonOperator(
    task_id="produce_d19_run_history_observation",
    python_callable=produce_d19_run_history_observation,
    op_kwargs={
        "parent_airflow_run": {
            "dagId": "{{ dag.dag_id }}",
            "logicalDate": "{{ logical_date.isoformat() }}",
            "runId": "{{ run_id }}",
            "runType": "{{ dag_run.run_type.value }}",
            "startDate": "{{ dag_run.start_date.isoformat() }}",
        },
        "plan_json": "{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}",
    },
    executor_config=D19_HISTORY_OBSERVER_EXECUTOR_CONFIG,
    dag=dag,
)

trigger_d19 = TriggerDagRunOperator(
    task_id="trigger_benchmark_improvement_wave",
    trigger_dag_id="serp_benchmark_improvement_wave",
    trigger_run_id="d6__{{ run_id }}",
    logical_date="{{ logical_date }}",
    conf="{{ ti.xcom_pull(task_ids='produce_d19_run_history_observation')['d19TriggerConf'] }}",
    reset_dag_run=False,
    wait_for_completion=True,
    allowed_states=["success"],
    failed_states=["failed"],
    poke_interval=30,
    skip_when_already_exists=False,
    deferrable=True,
    dag=dag,
)

load_child_verification = PythonOperator(
    task_id="load_triggered_d19_verification",
    python_callable=load_triggered_d19_verification,
    op_args=["d6__{{ run_id }}"],
    executor_config=NIGHTLY_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

observe_child = PythonOperator(
    task_id="observe_triggered_d19_run",
    python_callable=observe_triggered_d19_run,
    op_args=["d6__{{ run_id }}", "{{ logical_date }}"],
    executor_config=NIGHTLY_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

write_receipt = PythonOperator(
    task_id="write_scheduled_d6_regression_receipt",
    python_callable=write_scheduled_d6_regression_receipt,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}",
        "{{ ti.xcom_pull(task_ids='produce_d19_run_history_observation') }}",
        "{{ ti.xcom_pull(task_ids='load_triggered_d19_verification') }}",
        "{{ ti.xcom_pull(task_ids='observe_triggered_d19_run') }}",
    ],
    executor_config=NIGHTLY_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

release_fence = PythonOperator(
    task_id="release_d19_history_fence",
    python_callable=release_d19_history_fence,
    op_args=["{{ ti.xcom_pull(task_ids='produce_d19_run_history_observation') }}"],
    trigger_rule=TriggerRule.ALL_DONE,
    executor_config=D19_HISTORY_OBSERVER_EXECUTOR_CONFIG,
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    executor_config=NIGHTLY_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

(
    validate_plan
    >> produce_history
    >> trigger_d19
    >> load_child_verification
    >> observe_child
    >> write_receipt
    >> release_fence
)
[write_receipt, release_fence] >> notify_governance
