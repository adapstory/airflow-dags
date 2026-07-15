from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from dags.serp_benchmark_catalog_workload import (
    BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    benchmark_catalog_acquisition_container_security_context,
    benchmark_catalog_acquisition_env_vars,
    benchmark_catalog_acquisition_pod_security_context,
    benchmark_catalog_acquisition_web_identity_volume_mounts,
    benchmark_catalog_acquisition_web_identity_volumes,
)
from dags.serp_eval_contracts import (
    build_benchmark_improvement_wave_plan,
    governance_notification_pending,
    load_materialized_benchmark_catalog_snapshot,
    load_model_catalog_promotion_snapshot,
    write_airflow_plan_artifact,
    write_improvement_spec_artifact,
    write_paired_eval_request_artifact,
)
from dags.serp_evidence_workload_identity import (
    kubernetes_pod_launcher_executor_config,
    minio_web_identity_env_vars,
    minio_web_identity_executor_config,
    minio_web_identity_volume_mounts,
    minio_web_identity_volumes,
)
from dags.serp_web_seed_crawl_refresh import current_airflow_runtime_image

D19_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-evaluator"
D19_EVALUATOR_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-evaluator",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D19_NATIVE_ADAPTER_RUNNER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1000m", "memory": "3Gi"},
)
_D19_EVALUATOR_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
)
D19_EVALUATOR_WEB_IDENTITY_VOLUMES = minio_web_identity_volumes()
D19_EVALUATOR_WEB_IDENTITY_VOLUME_MOUNTS = minio_web_identity_volume_mounts()
D19_EVALUATOR_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name=D19_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT,
    labels=D19_EVALUATOR_WORKLOAD_LABELS,
)


def d19_evaluator_env_vars() -> list[k8s.V1EnvVar]:
    """Return the minimal STS-only runtime contract for paired evaluation."""

    return minio_web_identity_env_vars(_D19_EVALUATOR_ENV_NAMES)


def validate_benchmark_improvement_wave_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))


def write_improvement_spec(plan_json: str, promotion_snapshot: dict[str, Any]) -> dict[str, Any]:
    return write_improvement_spec_artifact(plan_json, promotion_snapshot)


def load_materialized_benchmark_catalog(plan_json: str) -> dict[str, Any]:
    return load_materialized_benchmark_catalog_snapshot(plan_json)


def load_model_catalog_promotion(plan_json: str) -> dict[str, Any]:
    return load_model_catalog_promotion_snapshot(plan_json)


def write_paired_eval_request(
    plan_json: str,
    catalog_snapshot: dict[str, Any],
    promotion_snapshot: dict[str, Any],
) -> dict[str, Any]:
    return write_paired_eval_request_artifact(plan_json, catalog_snapshot, promotion_snapshot)


default_args = {
    "owner": "serp-eval-runner",
    "start_date": datetime(2026, 7, 5, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_benchmark_improvement_wave",
    default_args=default_args,
    description="SERP D19 benchmark ratchet keep/discard contract",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "improvement"],
)

validate_plan = PythonOperator(
    task_id="validate_benchmark_improvement_wave_plan",
    python_callable=validate_benchmark_improvement_wave_plan,
    executor_config=D19_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

write_spec = PythonOperator(
    task_id="write_improvement_spec",
    python_callable=write_improvement_spec,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}",
        "{{ ti.xcom_pull(task_ids='load_model_catalog_promotion') }}",
    ],
    executor_config=D19_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

materialize_catalog = KubernetesPodOperator(
    task_id="materialize_live_benchmark_catalog",
    name="serp-d19-benchmark-catalog-acquisition",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "dags.serp_benchmark_catalog_materializer"],
    arguments=[
        "--plan-json-urlencoded",
        "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') | urlencode }}",
    ],
    env_vars=benchmark_catalog_acquisition_env_vars(),
    service_account_name=BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=benchmark_catalog_acquisition_web_identity_volumes(),
    volume_mounts=benchmark_catalog_acquisition_web_identity_volume_mounts(),
    labels=BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    container_resources=BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    security_context=benchmark_catalog_acquisition_pod_security_context(),
    container_security_context=benchmark_catalog_acquisition_container_security_context(),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=1,
    retry_delay=timedelta(seconds=BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS),
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

load_catalog = PythonOperator(
    task_id="load_materialized_benchmark_catalog",
    python_callable=load_materialized_benchmark_catalog,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

load_promotion = PythonOperator(
    task_id="load_model_catalog_promotion",
    python_callable=load_model_catalog_promotion,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

write_request = PythonOperator(
    task_id="write_paired_eval_request",
    python_callable=write_paired_eval_request,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog') }}",
        "{{ ti.xcom_pull(task_ids='load_model_catalog_promotion') }}",
    ],
    executor_config=D19_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

run_paired_evaluation = KubernetesPodOperator(
    task_id="run_paired_benchmark_evaluation",
    name="serp-paired-benchmark-evaluation",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "adapstory_serp_pipeline.orchestration.paired_eval_receipt"],
    arguments=[
        "--paired-eval-request",
        "{{ ti.xcom_pull(task_ids='write_paired_eval_request')['artifactPath'] }}",
        "--paired-eval-request-version-id",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_eval_request')"
            "['requestEvidence']['artifactVersionId'] }}"
        ),
        "--evidence-output",
        (
            "{{ ti.xcom_pull(task_ids='write_improvement_spec')"
            "['payload']['artifact_paths']['paired_eval_receipt'] }}"
        ),
    ],
    env_vars=d19_evaluator_env_vars(),
    service_account_name=D19_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_EVALUATOR_WEB_IDENTITY_VOLUMES,
    volume_mounts=D19_EVALUATOR_WEB_IDENTITY_VOLUME_MOUNTS,
    labels=D19_EVALUATOR_WORKLOAD_LABELS,
    container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
    container_security_context=k8s.V1SecurityContext(
        allow_privilege_escalation=False,
        capabilities=k8s.V1Capabilities(drop=["ALL"]),
    ),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=0,
    retry_delay=timedelta(seconds=5),
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

validate_plan >> materialize_catalog >> load_catalog
validate_plan >> load_promotion
load_catalog >> write_spec
load_promotion >> write_spec
load_catalog >> write_request
load_promotion >> write_request
write_spec >> run_paired_evaluation
write_request >> run_paired_evaluation >> notify_governance
