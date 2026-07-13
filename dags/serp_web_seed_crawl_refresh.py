from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from dags.serp_eval_contracts import (
    build_public_docs_seed_refresh_plan as build_public_docs_seed_refresh_plan_contract,
)
from dags.serp_eval_contracts import (
    default_public_docs_seed_refresh_conf,
    discover_public_docs_crawler_frontier,
    dispatch_public_docs_seed_refresh_handoff,
    governance_notification_pending,
    load_public_docs_crawl_state_conf,
    submit_public_docs_bc21_pipeline_state_artifact,
    write_airflow_plan_artifact,
    write_public_docs_publish_activation_trigger_conf_artifact,
    write_public_docs_seed_refresh_plan_artifact,
    write_public_docs_seed_registry_artifact,
)

_PIPELINE_RUNNER_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_SERP_EMBEDDING_BATCH_SIZE",
    "ADAPSTORY_SERP_EMBEDDING_DIMENSION",
    "ADAPSTORY_SERP_EMBEDDING_MAX_ATTEMPTS",
    "ADAPSTORY_SERP_EMBEDDING_MODEL_ID",
    "ADAPSTORY_SERP_EMBEDDING_MODEL_VERSION",
    "ADAPSTORY_SERP_EMBEDDING_PROFILE_VERSION",
    "ADAPSTORY_SERP_EMBEDDING_PROVIDER_MODEL",
    "ADAPSTORY_SERP_EMBEDDING_RETRY_DELAY_SECONDS",
    "ADAPSTORY_SERP_EMBEDDING_TIMEOUT_SECONDS",
    "ADAPSTORY_SERP_EMBEDDING_URL",
    "ADAPSTORY_SERP_NEO4J_HTTP_URL",
    "ADAPSTORY_SERP_NEO4J_MUTATION_BATCH_SIZE",
    "ADAPSTORY_SERP_NEO4J_TIMEOUT_SECONDS",
    "ADAPSTORY_SERP_NEO4J_USERNAME",
    "ADAPSTORY_SERP_OPENSEARCH_TIMEOUT_SECONDS",
    "ADAPSTORY_SERP_OPENSEARCH_URL",
    "ADAPSTORY_SERP_QDRANT_TIMEOUT_SECONDS",
    "ADAPSTORY_SERP_QDRANT_URL",
    "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL",
    "ADAPSTORY_SERP_PUBLIC_DOCS_RETRY_DELAY_SECONDS",
    "ADAPSTORY_SERP_SOURCE_CURL_FALLBACK_ENABLED",
    "ADAPSTORY_SERP_SOURCE_FETCH_TIMEOUT_SECONDS",
    "ADAPSTORY_SERP_SOURCE_PROXY_URL",
)
SERP_PIPELINE_RUNNER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "2Gi"},
)


def current_airflow_runtime_image() -> str:
    repository = conf.get("kubernetes_executor", "worker_container_repository").strip()
    tag = conf.get("kubernetes_executor", "worker_container_tag").strip()
    if not repository or not tag:
        raise ValueError("KubernetesExecutor worker image configuration is required")
    return f"{repository}:{tag}"


def pipeline_runner_env_vars(cli_spec_task_id: str) -> list[k8s.V1EnvVar]:
    values = pipeline_runner_runtime_env_vars()
    values.append(
        k8s.V1EnvVar(
            name="ADAPSTORY_SERP_PIPELINE_CLI_SPEC_URLENCODED",
            value=("{{ ti.xcom_pull(task_ids='" + cli_spec_task_id + "') | tojson | urlencode }}"),
        )
    )
    return values


def pipeline_runner_runtime_env_vars() -> list[k8s.V1EnvVar]:
    """Return the shared non-DAG-specific runtime and secret environment contract."""

    values: list[k8s.V1EnvVar] = []
    for name in _PIPELINE_RUNNER_ENV_NAMES:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"public docs pipeline runner environment is required: {name}")
        values.append(k8s.V1EnvVar(name=name, value=_native_template_safe_env_value(value)))
    values.extend(
        (
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-artifact-store",
                        key="access-key",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-artifact-store",
                        key="secret-key",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="ADAPSTORY_SERP_NEO4J_PASSWORD",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-serp-neo4j",
                        key="neo4j-password",
                    )
                ),
            ),
        )
    )
    return values


def _native_template_safe_env_value(value: str) -> str:
    """Return a NativeEnvironment literal that renders to an exact Kubernetes string."""

    return repr(value)


def validate_public_docs_seed_registry(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    conf = _public_docs_seed_refresh_conf_with_defaults(conf)
    conf = load_public_docs_crawl_state_conf(conf)
    return write_airflow_plan_artifact(
        build_public_docs_seed_refresh_plan_contract(
            conf,
            sitemap_frontier_discoverer=discover_public_docs_crawler_frontier,
        )
    )


def _public_docs_seed_refresh_conf_with_defaults(conf: dict[str, Any]) -> dict[str, Any]:
    generated_at = str(
        conf.get("generated_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    artifact_root_path = conf.get("artifact_root_path")
    defaults = default_public_docs_seed_refresh_conf(
        generated_at=generated_at,
        artifact_root_path=str(artifact_root_path) if artifact_root_path else None,
    )
    return {**defaults, **conf, "generated_at": generated_at}


def prepare_public_docs_d5_dispatch(**context: Any) -> dict[str, Any]:
    """Return the only D5 configuration D20 is permitted to dispatch."""

    task_instance = context.get("ti")
    if task_instance is None:
        raise ValueError("Airflow task instance is required for D5 dispatch")
    trigger_artifact = task_instance.xcom_pull(
        task_ids="write_public_docs_publish_activation_trigger_conf"
    )
    if not isinstance(trigger_artifact, Mapping):
        raise ValueError("D20 publish trigger artifact must be an object")
    payload = trigger_artifact.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("D20 publish trigger artifact payload must be an object")
    status = payload.get("status")
    if status == "no_change_active_pack_retained":
        raise AirflowSkipException("public docs no-op: D5 activation is not dispatched")
    if status != "ready_for_d5_publish_activation":
        raise ValueError(f"D20 publish trigger artifact is not dispatchable: status={status!r}")
    if payload.get("target_dag_id") != "serp_publish_signed_pack":
        raise ValueError("D20 publish trigger target DAG is invalid")
    target_conf = payload.get("target_dag_run_conf")
    if not isinstance(target_conf, Mapping) or not target_conf:
        raise ValueError("D20 publish trigger target configuration must be a non-empty object")
    return dict(target_conf)


default_args = {
    "owner": "serp-public-docs-refresh",
    "start_date": datetime(2026, 7, 8, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_web_seed_crawl_refresh",
    default_args=default_args,
    description="SERP D20 governed public-docs seed refresh handoff contract",
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["serp", "public-docs", "seed-refresh", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_public_docs_seed_registry",
    python_callable=validate_public_docs_seed_registry,
    dag=dag,
)

write_seed_registry = PythonOperator(
    task_id="write_public_docs_seed_registry",
    python_callable=write_public_docs_seed_registry_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

build_refresh_plan = PythonOperator(
    task_id="build_public_docs_seed_refresh_plan",
    python_callable=write_public_docs_seed_refresh_plan_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

dispatch_handoff = PythonOperator(
    task_id="dispatch_pipeline_seed_refresh_handoff",
    python_callable=dispatch_public_docs_seed_refresh_handoff,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

run_pipeline = KubernetesPodOperator(
    task_id="run_public_docs_seed_refresh_pipeline",
    name="serp-public-docs-seed-refresh",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "adapstory_serp_pipeline.orchestration.seed_refresh_remote_runner"],
    env_vars=pipeline_runner_env_vars("dispatch_pipeline_seed_refresh_handoff"),
    service_account_name="airflow-worker",
    automount_service_account_token=False,
    labels={
        "component": "worker",
        "release": "airflow",
        "tier": "airflow",
    },
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

submit_bc21_pipeline_state = PythonOperator(
    task_id="submit_public_docs_bc21_pipeline_state",
    python_callable=submit_public_docs_bc21_pipeline_state_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

write_publish_trigger_conf = PythonOperator(
    task_id="write_public_docs_publish_activation_trigger_conf",
    python_callable=write_public_docs_publish_activation_trigger_conf_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

prepare_d5_dispatch = PythonOperator(
    task_id="prepare_public_docs_d5_dispatch",
    python_callable=prepare_public_docs_d5_dispatch,
    dag=dag,
)

trigger_d5_publish_activation = TriggerDagRunOperator(
    task_id="trigger_public_docs_d5_publish_activation",
    trigger_dag_id="serp_publish_signed_pack",
    trigger_run_id="d5-from-{{ dag_run.run_id }}",
    conf="{{ ti.xcom_pull(task_ids='prepare_public_docs_d5_dispatch') }}",
    wait_for_completion=True,
    allowed_states=["success"],
    failed_states=["failed"],
    skip_when_already_exists=True,
    fail_when_dag_is_paused=True,
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

(
    validate_plan
    >> write_seed_registry
    >> build_refresh_plan
    >> dispatch_handoff
    >> run_pipeline
    >> submit_bc21_pipeline_state
    >> write_publish_trigger_conf
    >> prepare_d5_dispatch
    >> trigger_d5_publish_activation
    >> notify_governance
)
