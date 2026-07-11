from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_benchmark_improvement_decision_cli_spec,
    build_benchmark_improvement_scoreboard_cli_spec,
    build_benchmark_improvement_wave_plan,
    build_improvement_candidate_eval_cli_spec,
    execute_gateway_cli_spec,
    governance_notification_pending,
    write_airflow_plan_artifact,
    write_improvement_spec_artifact,
)


def validate_benchmark_improvement_wave_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))


def write_improvement_spec_and_candidate_eval(plan_json: str) -> dict[str, Any]:
    write_improvement_spec_artifact(plan_json)
    return execute_gateway_cli_spec(build_improvement_candidate_eval_cli_spec(plan_json))


def decide_keep_or_discard_candidate(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_benchmark_improvement_decision_cli_spec(plan_json))


def publish_improvement_scoreboard(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_benchmark_improvement_scoreboard_cli_spec(plan_json))


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

run_candidate_eval = PythonOperator(
    task_id="run_targeted_benchmark_eval_harness",
    python_callable=write_improvement_spec_and_candidate_eval,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

decide_candidate = PythonOperator(
    task_id="decide_keep_or_discard_candidate",
    python_callable=decide_keep_or_discard_candidate,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

publish_scoreboard = PythonOperator(
    task_id="publish_improvement_scoreboard",
    python_callable=publish_improvement_scoreboard,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    dag=dag,
)

(validate_plan >> run_candidate_eval >> decide_candidate >> publish_scoreboard >> notify_governance)
