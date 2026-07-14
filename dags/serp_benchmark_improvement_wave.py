from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_benchmark_improvement_wave_plan,
    build_paired_eval_executor_cli_spec,
    execute_pipeline_cli_spec,
    governance_notification_pending,
    write_airflow_plan_artifact,
    write_improvement_spec_artifact,
    write_paired_eval_request_artifact,
)


def validate_benchmark_improvement_wave_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))


def write_improvement_spec(plan_json: str) -> dict[str, Any]:
    return write_improvement_spec_artifact(plan_json)


def write_paired_eval_request(plan_json: str) -> dict[str, Any]:
    return write_paired_eval_request_artifact(plan_json)


def run_paired_benchmark_evaluation(plan_json: str) -> dict[str, Any]:
    return execute_pipeline_cli_spec(build_paired_eval_executor_cli_spec(plan_json))


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

run_paired_evaluation = PythonOperator(
    task_id="run_paired_benchmark_evaluation",
    python_callable=run_paired_benchmark_evaluation,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

validate_plan >> write_spec >> write_request >> run_paired_evaluation >> notify_governance
