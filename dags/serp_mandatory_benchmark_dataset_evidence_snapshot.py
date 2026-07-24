"""Daily immutable source, dataset, and licensing snapshots for all SERP suites."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowSkipException

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
from dags.serp_benchmark_runtime_prerequisite import (
    source_set_prerequisite_state,
)
from dags.serp_eval_contracts import (
    build_mandatory_benchmark_dataset_evidence_plan,
    write_airflow_plan_artifact,
)
from dags.serp_evidence_workload_identity import (
    kubernetes_pod_launcher_executor_config,
    minio_web_identity_executor_config,
)
from dags.serp_web_seed_crawl_refresh import current_airflow_runtime_image

BENCHMARK_EVALUATOR_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name="airflow-serp-benchmark-evaluator",
    labels={
        "adapstory.com/serp-evidence-workload": "true",
        "adapstory.com/serp-network-profile": "benchmark-evaluator",
        "component": "worker",
        "release": "airflow",
        "tier": "airflow",
    },
)


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


def wait_for_benchmark_substrate_source_set() -> dict[str, Any]:
    prerequisite = source_set_prerequisite_state(os.environ)
    if prerequisite is None:
        # Context: a daily catalog snapshot can race the immutable substrate
        # supply/runtime rollout. Decision: absence is skipped and retried by
        # the next schedule or the release chain; malformed identity still
        # fails closed. Reason: a missing ConfigMap key must never create a
        # doomed KPO pod that requires manual repair. Revisit when: Airflow has
        # an event-driven trigger bound directly to the GitOps source-set key.
        raise AirflowSkipException(
            "benchmark substrate source set is not published yet; "
            "the automated supply and runtime-promotion chain remains authoritative"
        )
    return prerequisite


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

wait_for_source_set = PythonOperator(
    task_id="wait_for_benchmark_substrate_source_set",
    python_callable=wait_for_benchmark_substrate_source_set,
    executor_config=BENCHMARK_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

validate_plan = PythonOperator(
    task_id="validate_mandatory_benchmark_dataset_evidence_plan",
    python_callable=validate_mandatory_benchmark_dataset_evidence_plan,
    executor_config=BENCHMARK_EVALUATOR_EXECUTOR_CONFIG,
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

wait_for_source_set >> validate_plan >> materialize_evidence
