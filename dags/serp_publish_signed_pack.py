from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_public_docs_publish_activation_cli_spec,
    build_public_docs_publish_activation_plan,
    execute_pipeline_cli_spec,
    governance_notification_pending,
    write_airflow_plan_artifact,
)


def validate_publish_signed_pack_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_public_docs_publish_activation_plan(conf))


default_args = {
    "owner": "serp-publish-signed-pack",
    "start_date": datetime(2026, 7, 8, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_publish_signed_pack",
    default_args=default_args,
    description="SERP D5 governed publish activation handoff contract",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["serp", "public-docs", "publish", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_publish_signed_pack_plan",
    python_callable=validate_publish_signed_pack_plan,
    dag=dag,
)

dispatch_handoff = PythonOperator(
    task_id="dispatch_publish_activation_handoff",
    python_callable=build_public_docs_publish_activation_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

run_handoff = PythonOperator(
    task_id="run_publish_activation_handoff",
    python_callable=execute_pipeline_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='dispatch_publish_activation_handoff') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

validate_plan >> dispatch_handoff >> run_handoff >> notify_governance
