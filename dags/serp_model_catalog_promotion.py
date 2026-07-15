"""D17 immutable exact-nine EvaluationRelease/v2 promotion authority."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_model_catalog_promotion_plan,
    governance_notification_pending,
    load_governed_model_releases,
    write_airflow_plan_artifact,
    write_model_catalog_promotion_receipt,
)
from dags.serp_evidence_workload_identity import minio_web_identity_executor_config

D17_MODEL_GOVERNANCE_SERVICE_ACCOUNT = "airflow-serp-benchmark-aggregator"
D17_MODEL_GOVERNANCE_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-aggregator",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name=D17_MODEL_GOVERNANCE_SERVICE_ACCOUNT,
    labels=D17_MODEL_GOVERNANCE_LABELS,
)


def validate_model_catalog_promotion_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_model_catalog_promotion_plan(conf))


def load_model_releases(plan_json: str) -> dict[str, Any]:
    return load_governed_model_releases(plan_json)


def write_promotion_receipt(plan_json: str, releases: dict[str, Any]) -> dict[str, Any]:
    return write_model_catalog_promotion_receipt(plan_json, releases)


default_args = {
    "owner": "serp-model-governance",
    "retries": 0,
    "start_date": datetime(2026, 7, 15, tzinfo=UTC),
}

dag = DAG(
    "serp_model_catalog_promotion",
    default_args=default_args,
    description="WORM-bound D17 exact-nine evaluation release promotion for D19",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    tags=["serp", "models", "governance", "evidence", "promotion"],
)

validate_plan = PythonOperator(
    task_id="validate_model_catalog_promotion_plan",
    python_callable=validate_model_catalog_promotion_plan,
    executor_config=D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG,
    dag=dag,
)

load_releases = PythonOperator(
    task_id="load_governed_model_releases",
    python_callable=load_model_releases,
    op_args=["{{ ti.xcom_pull(task_ids='validate_model_catalog_promotion_plan') }}"],
    executor_config=D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG,
    dag=dag,
)

write_receipt = PythonOperator(
    task_id="write_model_catalog_promotion_receipt",
    python_callable=write_promotion_receipt,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_model_catalog_promotion_plan') }}",
        "{{ ti.xcom_pull(task_ids='load_governed_model_releases') }}",
    ],
    executor_config=D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG,
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_model_catalog_promotion_plan') }}"],
    executor_config=D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG,
    dag=dag,
)

validate_plan >> load_releases >> write_receipt >> notify_governance
