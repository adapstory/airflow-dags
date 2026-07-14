"""Daily immutable source, dataset, and licensing snapshots for all SERP suites."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_benchmark_catalog_workload import (
    BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    benchmark_catalog_acquisition_container_security_context,
    benchmark_catalog_acquisition_env_vars,
    benchmark_catalog_acquisition_pod_security_context,
)
from dags.serp_eval_contracts import (
    build_mandatory_benchmark_dataset_evidence_plan,
    write_airflow_plan_artifact,
)
from dags.serp_web_seed_crawl_refresh import current_airflow_runtime_image


def validate_mandatory_benchmark_dataset_evidence_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    supplied_conf = dict(getattr(dag_run, "conf", None) or {})
    return write_airflow_plan_artifact(
        build_mandatory_benchmark_dataset_evidence_plan(
            {
                "artifact_root_path": supplied_conf.get(
                    "artifact_root_path", os.environ["ADAPSTORY_AIRFLOW_ARTIFACT_ROOT"]
                ),
                "generated_at": supplied_conf.get(
                    "generated_at", datetime.now(UTC).isoformat().replace("+00:00", "Z")
                ),
            }
        )
    )


default_args = {
    "owner": "serp-benchmark-catalog",
    "retries": 0,
    "start_date": datetime(2026, 7, 13, tzinfo=UTC),
}

dag = DAG(
    "serp_mandatory_benchmark_dataset_evidence_snapshot",
    default_args=default_args,
    description="WORM dataset, source, and licensing evidence for every mandatory SERP suite",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "evidence", "dataset"],
)

validate_plan = PythonOperator(
    task_id="validate_mandatory_benchmark_dataset_evidence_plan",
    python_callable=validate_mandatory_benchmark_dataset_evidence_plan,
    dag=dag,
)

materialize_evidence = KubernetesPodOperator(
    task_id="materialize_mandatory_benchmark_dataset_evidence",
    name="serp-mandatory-benchmark-dataset-acquisition",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "dags.serp_benchmark_catalog_materializer"],
    arguments=[
        "--plan-json-urlencoded",
        (
            "{{ ti.xcom_pull(task_ids='validate_mandatory_benchmark_dataset_evidence_plan') "
            "| urlencode }}"
        ),
    ],
    env_vars=benchmark_catalog_acquisition_env_vars(),
    service_account_name=BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
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
    dag=dag,
)

validate_plan >> materialize_evidence
