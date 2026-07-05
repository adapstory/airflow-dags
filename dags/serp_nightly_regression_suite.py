from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from serp_eval_contracts import (
    build_nightly_benchmark_export_cli_spec,
    build_nightly_registry_cli_spec,
    build_nightly_regression_plan,
    build_nightly_runner_cli_spec,
    governance_notification_pending,
    write_airflow_plan_artifact,
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
    tags=["serp", "evals", "benchmark", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_nightly_regression_plan",
    python_callable=validate_nightly_regression_plan,
    dag=dag,
)

run_suites = PythonOperator(
    task_id="run_mandatory_benchmark_suites",
    python_callable=build_nightly_runner_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

build_benchmark_export = PythonOperator(
    task_id="build_c1_benchmark_gate_export",
    python_callable=build_nightly_benchmark_export_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

build_submissions = PythonOperator(
    task_id="build_bc21_benchmark_run_submissions",
    python_callable=build_nightly_registry_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

validate_plan >> run_suites >> build_benchmark_export >> build_submissions >> notify_governance
