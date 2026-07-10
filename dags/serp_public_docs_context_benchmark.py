from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_public_docs_context_benchmark_contracts import (
    enforce_context_benchmark_gate,
    execute_context_benchmark,
    publish_context_benchmark_github_status,
    submit_context_benchmark_bc21_runs,
    write_context_benchmark_plan,
)


def write_context_benchmark_plan_from_dag_run(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})
    conf.setdefault("generated_at", datetime.now(UTC).isoformat().replace("+00:00", "Z"))
    return write_context_benchmark_plan(conf)


default_args = {
    "owner": "serp-public-docs",
    "retries": 0,
    "start_date": datetime(2026, 7, 11, tzinfo=UTC),
}

dag = DAG(
    "serp_public_docs_context_benchmark",
    default_args=default_args,
    description="In-cluster PublicDocsGolden benchmark with BC-21 and GitHub status evidence",
    schedule="15 3 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["serp", "public-docs", "benchmark", "bc21"],
)

write_plan = PythonOperator(
    task_id="write_context_benchmark_plan",
    python_callable=write_context_benchmark_plan_from_dag_run,
    dag=dag,
)

execute_benchmark = PythonOperator(
    task_id="execute_context_benchmark",
    python_callable=execute_context_benchmark,
    op_args=["{{ ti.xcom_pull(task_ids='write_context_benchmark_plan') }}"],
    dag=dag,
)

submit_bc21 = PythonOperator(
    task_id="submit_context_benchmark_bc21_runs",
    python_callable=submit_context_benchmark_bc21_runs,
    op_args=[
        "{{ ti.xcom_pull(task_ids='write_context_benchmark_plan') }}",
        "{{ ti.xcom_pull(task_ids='execute_context_benchmark') }}",
    ],
    dag=dag,
)

publish_github_status = PythonOperator(
    task_id="publish_context_benchmark_github_status",
    python_callable=publish_context_benchmark_github_status,
    op_args=[
        "{{ ti.xcom_pull(task_ids='write_context_benchmark_plan') }}",
        "{{ ti.xcom_pull(task_ids='execute_context_benchmark') }}",
        "{{ ti.xcom_pull(task_ids='submit_context_benchmark_bc21_runs') }}",
    ],
    dag=dag,
)

enforce_gate = PythonOperator(
    task_id="enforce_context_benchmark_gate",
    python_callable=enforce_context_benchmark_gate,
    op_args=[
        "{{ ti.xcom_pull(task_ids='execute_context_benchmark') }}",
        "{{ ti.xcom_pull(task_ids='submit_context_benchmark_bc21_runs') }}",
        "{{ ti.xcom_pull(task_ids='publish_context_benchmark_github_status') }}",
    ],
    dag=dag,
)

write_plan >> execute_benchmark >> submit_bc21 >> publish_github_status >> enforce_gate
