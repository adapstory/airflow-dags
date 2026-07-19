"""Event-only D6 route from a sealed D17 promotion receipt into native D19."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_d17_event_d6_plan,
    validate_d17_event_d6_airflow_run,
    write_airflow_plan_artifact,
)
from dags.serp_evidence_workload_identity import minio_web_identity_executor_config

D17_EVENT_D6_EVALUATOR_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name="airflow-serp-benchmark-evaluator",
    labels={
        "adapstory.com/serp-evidence-workload": "true",
        "adapstory.com/serp-network-profile": "benchmark-evaluator",
        "component": "worker",
        "release": "airflow",
        "tier": "airflow",
    },
)


def validate_event_d6_plan(**context: Any) -> str:
    """Require D17's deterministic manual trigger before D19 can be invoked."""

    dag_run = context.get("dag_run")
    if dag_run is None:
        raise ValueError("event D6 requires authoritative DagRun metadata")
    conf = getattr(dag_run, "conf", None) or {}
    plan = build_d17_event_d6_plan(conf)
    logical_date = context.get("logical_date") or getattr(dag_run, "logical_date", None)
    if isinstance(logical_date, datetime):
        logical_date_value = logical_date.astimezone(UTC).isoformat().replace("+00:00", "Z")
    elif isinstance(logical_date, str):
        logical_date_value = logical_date
    else:
        raise ValueError("event D6 requires a timezone-aware logical_date")
    run_type = getattr(dag_run, "run_type", None)
    run_type_value = getattr(run_type, "value", run_type)
    validate_d17_event_d6_airflow_run(
        plan.to_canonical_json(),
        {
            "dagId": str(getattr(dag_run, "dag_id", "")),
            "logicalDate": logical_date_value,
            "runId": str(getattr(dag_run, "run_id", "")),
            "runType": str(run_type_value),
        },
    )
    return write_airflow_plan_artifact(plan)


default_args = {
    "owner": "serp-eval-runner",
    "retries": 0,
    # D17's v7 contract has supported releases since 2026-07-15.  A manually
    # triggered child with that logical date must still receive task instances.
    "start_date": datetime(2026, 7, 15, tzinfo=UTC),
}

dag = DAG(
    "serp_model_promotion_regression_suite",
    default_args=default_args,
    description="Event D6 parent for one D17-derived native D19 evaluation",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "d6", "d17", "d19", "event"],
)

validate_plan = PythonOperator(
    task_id="validate_d17_event_d6_plan",
    python_callable=validate_event_d6_plan,
    executor_config=D17_EVENT_D6_EVALUATOR_EXECUTOR_CONFIG,
    dag=dag,
)

trigger_d19 = TriggerDagRunOperator(
    task_id="trigger_benchmark_improvement_wave",
    trigger_dag_id="serp_benchmark_improvement_wave",
    trigger_run_id=(
        "{{ ti.xcom_pull(task_ids='validate_d17_event_d6_plan')['d19_trigger_run_id'] }}"
    ),
    logical_date=(
        "{{ ti.xcom_pull(task_ids='validate_d17_event_d6_plan')"
        "['d19_trigger_conf']['generated_at'] }}"
    ),
    conf="{{ ti.xcom_pull(task_ids='validate_d17_event_d6_plan')['d19_trigger_conf'] }}",
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

validate_plan >> trigger_d19
