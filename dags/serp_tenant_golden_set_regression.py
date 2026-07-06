from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_tenant_golden_registry_cli_spec,
    build_tenant_golden_regression_plan,
    build_tenant_golden_runner_cli_spec,
    governance_notification_pending,
    write_airflow_plan_artifact,
)


def validate_tenant_golden_regression_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_tenant_golden_regression_plan(conf))


default_args = {
    "owner": "serp-eval-runner",
    "start_date": datetime(2026, 7, 5, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_tenant_golden_set_regression",
    default_args=default_args,
    description="SERP D13 tenant golden-set regression gate contract",
    schedule=None,
    catchup=False,
    tags=["serp", "evals", "tenant-golden-set", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_tenant_golden_regression_plan",
    python_callable=validate_tenant_golden_regression_plan,
    dag=dag,
)

run_cases = PythonOperator(
    task_id="run_tenant_golden_set_cases",
    python_callable=build_tenant_golden_runner_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_tenant_golden_regression_plan') }}"],
    dag=dag,
)

build_submissions = PythonOperator(
    task_id="build_tenant_golden_registry_submissions",
    python_callable=build_tenant_golden_registry_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_tenant_golden_regression_plan') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_tenant_golden_regression_plan') }}"],
    dag=dag,
)

validate_plan >> run_cases >> build_submissions >> notify_governance
