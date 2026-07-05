from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_nightly_regression_plan,
    governance_notification_pending,
    write_airflow_plan_artifact,
    write_nightly_benchmark_export_artifact,
    write_nightly_registry_receipts_artifact,
    write_nightly_registry_submissions_artifact,
    write_nightly_report_artifact,
)


def validate_nightly_regression_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_nightly_regression_plan(conf))


default_args = {
    "owner": "serp-eval-runner",
    "start_date": datetime(2026, 7, 5, tzinfo=timezone.utc),
    "retries": 0,
}

dag = DAG(
    "serp_nightly_regression_suite",
    default_args=default_args,
    description="SERP D6 nightly benchmark regression gate contract",
    schedule="@daily",
    catchup=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_nightly_regression_plan",
    python_callable=validate_nightly_regression_plan,
    dag=dag,
)

run_suites = PythonOperator(
    task_id="run_mandatory_benchmark_suites",
    python_callable=write_nightly_report_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

build_benchmark_export = PythonOperator(
    task_id="build_c1_benchmark_gate_export",
    python_callable=write_nightly_benchmark_export_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='run_mandatory_benchmark_suites') }}"],
    dag=dag,
)

build_submissions = PythonOperator(
    task_id="build_bc21_benchmark_run_submissions",
    python_callable=write_nightly_registry_submissions_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='build_c1_benchmark_gate_export') }}"],
    dag=dag,
)

submit_submissions = PythonOperator(
    task_id="submit_bc21_benchmark_run_submissions",
    python_callable=write_nightly_registry_receipts_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='build_bc21_benchmark_run_submissions') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

validate_plan >> run_suites >> build_benchmark_export >> build_submissions >> submit_submissions >> notify_governance
