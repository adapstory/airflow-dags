"""D17 immutable exact-nine EvaluationRelease/v4 promotion authority."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_d17_event_d6_trigger_conf,
    build_model_catalog_promotion_plan,
    verify_governed_model_releases_terminal_activation,
    write_airflow_plan_artifact,
    write_model_catalog_promotion_receipt,
)
from dags.serp_evidence_workload_identity import (
    evaluation_admission_verifier_executor_config,
    minio_web_identity_executor_config,
)

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
D17_ADMISSION_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "evaluation-admission-verifier",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D17_ADMISSION_EXECUTOR_CONFIG = evaluation_admission_verifier_executor_config(
    labels=D17_ADMISSION_LABELS,
)


def validate_model_catalog_promotion_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_model_catalog_promotion_plan(conf))


def verify_runtime_terminal_activation_admission(plan_json: str) -> dict[str, Any]:
    return verify_governed_model_releases_terminal_activation(plan_json)


def write_promotion_receipt(plan_json: str, releases: dict[str, Any]) -> dict[str, Any]:
    return write_model_catalog_promotion_receipt(plan_json, releases)


def build_event_d6_trigger_conf(
    plan_json: str,
    promotion_receipt_result: dict[str, Any],
    **context: Any,
) -> dict[str, Any]:
    """Bind the D17 WORM receipt to this authoritative D17 DagRun."""

    dag_run = context.get("dag_run")
    if dag_run is None:
        raise ValueError("D17 event-D6 trigger requires authoritative DagRun metadata")
    logical_date = context.get("logical_date") or getattr(dag_run, "logical_date", None)
    if isinstance(logical_date, datetime):
        logical_date_value = logical_date.astimezone(UTC).isoformat().replace("+00:00", "Z")
    elif isinstance(logical_date, str):
        logical_date_value = logical_date
    else:
        raise ValueError("D17 event-D6 trigger requires a timezone-aware logical_date")
    run_type = getattr(dag_run, "run_type", None)
    run_type_value = getattr(run_type, "value", run_type)
    return build_d17_event_d6_trigger_conf(
        plan_json,
        promotion_receipt_result,
        {
            "dagId": str(getattr(dag_run, "dag_id", "")),
            "logicalDate": logical_date_value,
            "runId": str(getattr(dag_run, "run_id", "")),
            "runType": str(run_type_value),
        },
    )


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
    # This is event-only (schedule=None), but must be runnable by the
    # release orchestrator as soon as the DAG is first registered.
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["serp", "models", "governance", "evidence", "promotion"],
)

validate_plan = PythonOperator(
    task_id="validate_model_catalog_promotion_plan",
    python_callable=validate_model_catalog_promotion_plan,
    executor_config=D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG,
    dag=dag,
)

verify_terminal_activation = PythonOperator(
    task_id="verify_runtime_terminal_activation_admission",
    python_callable=verify_runtime_terminal_activation_admission,
    op_args=["{{ ti.xcom_pull(task_ids='validate_model_catalog_promotion_plan') }}"],
    executor_config=D17_ADMISSION_EXECUTOR_CONFIG,
    dag=dag,
)

write_receipt = PythonOperator(
    task_id="write_model_catalog_promotion_receipt",
    python_callable=write_promotion_receipt,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_model_catalog_promotion_plan') }}",
        "{{ ti.xcom_pull(task_ids='verify_runtime_terminal_activation_admission') }}",
    ],
    executor_config=D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG,
    dag=dag,
)

build_event_d6_conf = PythonOperator(
    task_id="build_d17_event_d6_trigger_conf",
    python_callable=build_event_d6_trigger_conf,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_model_catalog_promotion_plan') }}",
        "{{ ti.xcom_pull(task_ids='write_model_catalog_promotion_receipt') }}",
    ],
    executor_config=D17_MODEL_GOVERNANCE_EXECUTOR_CONFIG,
    dag=dag,
)

trigger_event_d6 = TriggerDagRunOperator(
    task_id="trigger_model_promotion_regression_suite",
    trigger_dag_id="serp_model_promotion_regression_suite",
    trigger_run_id="{{ ti.xcom_pull(task_ids='build_d17_event_d6_trigger_conf')['eventD6RunId'] }}",
    logical_date="{{ ti.xcom_pull(task_ids='build_d17_event_d6_trigger_conf')['generated_at'] }}",
    conf="{{ ti.xcom_pull(task_ids='build_d17_event_d6_trigger_conf') }}",
    reset_dag_run=False,
    wait_for_completion=True,
    allowed_states=["success"],
    failed_states=["failed"],
    poke_interval=30,
    skip_when_already_exists=False,
    fail_when_dag_is_paused=True,
    deferrable=True,
    dag=dag,
)

(
    validate_plan
    >> verify_terminal_activation
    >> write_receipt
    >> build_event_d6_conf
    >> trigger_event_d6
)
