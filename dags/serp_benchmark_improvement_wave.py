from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from dags.serp_eval_contracts import (
    build_benchmark_improvement_wave_plan,
    governance_notification_pending,
    write_airflow_plan_artifact,
    write_improvement_spec_artifact,
    write_paired_eval_request_artifact,
)
from dags.serp_web_seed_crawl_refresh import (
    SERP_PIPELINE_RUNNER_RESOURCES,
    current_airflow_runtime_image,
)

D19_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-evidence-evaluator"
D19_EVALUATOR_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-evaluator",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
_D19_EVALUATOR_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
)


def d19_evaluator_env_vars() -> list[k8s.V1EnvVar]:
    """Return the minimal S3-only runtime contract for paired evaluation."""

    values: list[k8s.V1EnvVar] = []
    for name in _D19_EVALUATOR_ENV_NAMES:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"D19 evaluator environment is required: {name}")
        values.append(k8s.V1EnvVar(name=name, value=repr(value)))
    values.extend(
        (
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-serp-evidence-store",
                        key="access-key",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-serp-evidence-store",
                        key="secret-key",
                    )
                ),
            ),
        )
    )
    return values


def validate_benchmark_improvement_wave_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))


def write_improvement_spec(plan_json: str) -> dict[str, Any]:
    return write_improvement_spec_artifact(plan_json)


def write_paired_eval_request(plan_json: str) -> dict[str, Any]:
    return write_paired_eval_request_artifact(plan_json)


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
    tags=["serp", "evals", "benchmark", "improvement"],
)

validate_plan = PythonOperator(
    task_id="validate_benchmark_improvement_wave_plan",
    python_callable=validate_benchmark_improvement_wave_plan,
    dag=dag,
)

write_spec = PythonOperator(
    task_id="write_improvement_spec",
    python_callable=write_improvement_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

write_request = PythonOperator(
    task_id="write_paired_eval_request",
    python_callable=write_paired_eval_request,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
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
    automount_service_account_token=True,
    labels=D19_EVALUATOR_WORKLOAD_LABELS,
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
    retry_delay=timedelta(seconds=5),
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

validate_plan >> write_spec >> write_request >> run_paired_evaluation >> notify_governance
