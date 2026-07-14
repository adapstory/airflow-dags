# ruff: noqa: E501
"""Airflow graph for a governed, real BEIR/SciFact production retrieval run."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from dags.serp_scifact_benchmark_contracts import (
    activate_scifact_benchmark_pack,
    build_scifact_benchmark_plan,
    materialize_scifact_archive,
    prepare_scifact_benchmark_registry,
    seal_scifact_activation_evidence,
    submit_scifact_pipeline_state,
)
from dags.serp_web_seed_crawl_refresh import (
    SERP_PIPELINE_RUNNER_RESOURCES,
    current_airflow_runtime_image,
    pipeline_runner_runtime_env_vars,
)

SCIFACT_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-beir-scifact"
SCIFACT_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-evidence-evaluator"
SCIFACT_ACQUISITION_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-acquisition",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
SCIFACT_EVALUATOR_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-evaluator",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
SCIFACT_EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(name="base")],
            service_account_name=SCIFACT_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
            automount_service_account_token=True,
        )
    )
}


def build_scifact_plan_from_dag_run(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    supplied_conf = dict(getattr(dag_run, "conf", None) or {})
    return build_scifact_benchmark_plan(
        {
            "artifact_root_path": supplied_conf.get(
                "artifact_root_path", os.environ["ADAPSTORY_AIRFLOW_ARTIFACT_ROOT"]
            ),
            "generated_at": supplied_conf.get(
                "generated_at", datetime.now(UTC).isoformat().replace("+00:00", "Z")
            ),
        },
        bc21_base_url=os.environ["ADAPSTORY_SERP_BC21_BASE_URL"],
    )


def _store_name(name: str) -> str:
    value = os.environ.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required for live SciFact indexing")
    return value


default_args = {
    "owner": "serp-beir-scifact",
    "retries": 0,
    "start_date": datetime(2026, 7, 13, tzinfo=UTC),
}

dag = DAG(
    "serp_beir_scifact_live_benchmark",
    default_args=default_args,
    description="Version-bound SciFact indexing, BC-21 activation, live gateway evaluation, and WORM evidence",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["serp", "beir", "scifact", "benchmark", "bc21", "evidence"],
)

build_plan = PythonOperator(
    task_id="build_scifact_benchmark_plan",
    python_callable=build_scifact_plan_from_dag_run,
    executor_config=SCIFACT_EXECUTOR_CONFIG,
    dag=dag,
)

materialize_archive = PythonOperator(
    task_id="materialize_scifact_archive",
    python_callable=materialize_scifact_archive,
    op_args=["{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan') }}"],
    executor_config=SCIFACT_EXECUTOR_CONFIG,
    dag=dag,
)

prepare_registry = PythonOperator(
    task_id="prepare_scifact_benchmark_registry",
    python_callable=prepare_scifact_benchmark_registry,
    op_args=[
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan') }}",
        "{{ ti.xcom_pull(task_ids='materialize_scifact_archive') }}",
    ],
    executor_config=SCIFACT_EXECUTOR_CONFIG,
    dag=dag,
)

index_scifact = KubernetesPodOperator(
    task_id="index_scifact_live_dataset",
    name="serp-beir-scifact-index",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "adapstory_serp_pipeline.orchestration.scifact_live_runner"],
    arguments=[
        "--archive-artifact-uri",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['archive_artifact_uri'] }}",
        "--archive-version-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['archive_version_id'] }}",
        "--archive-sha256",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['archive_sha256'] }}",
        "--evidence-output",
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan')['artifact_paths']['index_evidence'] }}",
        "--clock-at",
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan')['generated_at'] }}",
        "--tenant-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['tenant_id'] }}",
        "--source-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['source_id'] }}",
        "--pack-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['pack_id'] }}",
        "--pack-version-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['pack_version_id'] }}",
        "--fetch-run-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['fetch_run_id'] }}",
        "--parse-run-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['parse_run_id'] }}",
        "--pipeline-run-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['pipeline_run_id'] }}",
        "--idempotency-key",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['idempotency_key'] }}",
        "--actor-id",
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan')['actor_id'] }}",
        "--qdrant-collection",
        _store_name("ADAPSTORY_SERP_PUBLIC_DOCS_QDRANT_COLLECTION"),
        "--opensearch-index",
        _store_name("ADAPSTORY_SERP_PUBLIC_DOCS_OPENSEARCH_INDEX"),
        "--neo4j-database",
        _store_name("ADAPSTORY_SERP_PUBLIC_DOCS_NEO4J_DATABASE"),
    ],
    env_vars=pipeline_runner_runtime_env_vars(),
    service_account_name=SCIFACT_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=True,
    labels=SCIFACT_ACQUISITION_WORKLOAD_LABELS,
    container_resources=SERP_PIPELINE_RUNNER_RESOURCES,
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
    retries=1,
    retry_delay=timedelta(seconds=5),
    dag=dag,
)

submit_pipeline_state = PythonOperator(
    task_id="submit_scifact_pipeline_state",
    python_callable=submit_scifact_pipeline_state,
    op_args=[
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan') }}",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry') }}",
    ],
    executor_config=SCIFACT_EXECUTOR_CONFIG,
    dag=dag,
)

activate_pack = PythonOperator(
    task_id="activate_scifact_benchmark_pack",
    python_callable=activate_scifact_benchmark_pack,
    op_args=[
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan') }}",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry') }}",
        "{{ ti.xcom_pull(task_ids='submit_scifact_pipeline_state') }}",
    ],
    executor_config=SCIFACT_EXECUTOR_CONFIG,
    dag=dag,
)

seal_activation = PythonOperator(
    task_id="seal_scifact_activation_evidence",
    python_callable=seal_scifact_activation_evidence,
    op_args=[
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan') }}",
        "{{ ti.xcom_pull(task_ids='activate_scifact_benchmark_pack') }}",
    ],
    executor_config=SCIFACT_EXECUTOR_CONFIG,
    dag=dag,
)

evaluate_scifact = KubernetesPodOperator(
    task_id="evaluate_scifact_live_gateway",
    name="serp-beir-scifact-evaluate",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "adapstory_serp_pipeline.orchestration.scifact_live_evaluator"],
    arguments=[
        "--archive-artifact-uri",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['archive_artifact_uri'] }}",
        "--archive-version-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['archive_version_id'] }}",
        "--archive-sha256",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['archive_sha256'] }}",
        "--activation-evidence-path",
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan')['artifact_paths']['activation_receipt'] }}",
        "--evidence-output",
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan')['artifact_paths']['run_evidence'] }}",
        "--operation-id",
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan')['operation_id'] }}",
        "--tenant-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['tenant_id'] }}",
        "--actor-id",
        "{{ ti.xcom_pull(task_ids='build_scifact_benchmark_plan')['gateway_actor_id'] }}",
        "--pack-version-id",
        "{{ ti.xcom_pull(task_ids='prepare_scifact_benchmark_registry')['pack_version_id'] }}",
    ],
    env_vars=pipeline_runner_runtime_env_vars(),
    service_account_name=SCIFACT_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    labels=SCIFACT_EVALUATOR_WORKLOAD_LABELS,
    container_resources=SERP_PIPELINE_RUNNER_RESOURCES,
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
    dag=dag,
)

(
    build_plan
    >> materialize_archive
    >> prepare_registry
    >> index_scifact
    >> submit_pipeline_state
    >> activate_pack
    >> seal_activation
    >> evaluate_scifact
)
