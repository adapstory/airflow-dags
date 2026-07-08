from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_online_eval_registry_cli_spec,
    build_online_eval_rollup_cli_spec,
    build_online_eval_rollup_plan,
    execute_gateway_cli_spec,
    governance_notification_pending,
    write_airflow_plan_artifact,
    write_online_eval_rollup_plan_artifact,
)


def validate_online_eval_rollup_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_online_eval_rollup_plan(conf))


def build_online_eval_rollup(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_online_eval_rollup_cli_spec(plan_json))


def build_online_eval_registry_submissions(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_online_eval_registry_cli_spec(plan_json))


default_args = {
    "owner": "serp-eval-runner",
    "start_date": datetime(2026, 7, 8, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_online_eval_rollup",
    default_args=default_args,
    description="SERP D7 sampled online-eval rollup contract",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "online-eval", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_online_eval_rollup_plan",
    python_callable=validate_online_eval_rollup_plan,
    dag=dag,
)

write_rollup_plan = PythonOperator(
    task_id="write_online_eval_rollup_plan",
    python_callable=write_online_eval_rollup_plan_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_online_eval_rollup_plan') }}"],
    dag=dag,
)

build_rollup = PythonOperator(
    task_id="build_online_eval_rollup",
    python_callable=build_online_eval_rollup,
    op_args=["{{ ti.xcom_pull(task_ids='validate_online_eval_rollup_plan') }}"],
    dag=dag,
)

build_submissions = PythonOperator(
    task_id="build_online_eval_registry_submissions",
    python_callable=build_online_eval_registry_submissions,
    op_args=["{{ ti.xcom_pull(task_ids='validate_online_eval_rollup_plan') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_online_eval_rollup_plan') }}"],
    dag=dag,
)

validate_plan >> write_rollup_plan >> build_rollup >> build_submissions >> notify_governance
