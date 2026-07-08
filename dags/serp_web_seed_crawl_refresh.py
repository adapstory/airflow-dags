from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_public_docs_seed_refresh_plan as build_public_docs_seed_refresh_plan_contract,
)
from dags.serp_eval_contracts import (
    default_public_docs_seed_refresh_conf,
    dispatch_public_docs_seed_refresh_handoff,
    execute_pipeline_cli_spec,
    governance_notification_pending,
    submit_public_docs_bc21_pipeline_state_artifact,
    write_airflow_plan_artifact,
    write_public_docs_publish_activation_trigger_conf_artifact,
    write_public_docs_seed_refresh_plan_artifact,
    write_public_docs_seed_registry_artifact,
)


def validate_public_docs_seed_registry(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    conf = _public_docs_seed_refresh_conf_with_defaults(conf)
    return write_airflow_plan_artifact(build_public_docs_seed_refresh_plan_contract(conf))


def _public_docs_seed_refresh_conf_with_defaults(conf: dict[str, Any]) -> dict[str, Any]:
    generated_at = str(
        conf.get("generated_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    artifact_root_path = conf.get("artifact_root_path")
    defaults = default_public_docs_seed_refresh_conf(
        generated_at=generated_at,
        artifact_root_path=str(artifact_root_path) if artifact_root_path else None,
    )
    return {**defaults, **conf, "generated_at": generated_at}


default_args = {
    "owner": "serp-public-docs-refresh",
    "start_date": datetime(2026, 7, 8, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_web_seed_crawl_refresh",
    default_args=default_args,
    description="SERP D20 governed public-docs seed refresh handoff contract",
    schedule="@daily",
    catchup=False,
    render_template_as_native_obj=True,
    tags=["serp", "public-docs", "seed-refresh", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_public_docs_seed_registry",
    python_callable=validate_public_docs_seed_registry,
    dag=dag,
)

write_seed_registry = PythonOperator(
    task_id="write_public_docs_seed_registry",
    python_callable=write_public_docs_seed_registry_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

build_refresh_plan = PythonOperator(
    task_id="build_public_docs_seed_refresh_plan",
    python_callable=write_public_docs_seed_refresh_plan_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

dispatch_handoff = PythonOperator(
    task_id="dispatch_pipeline_seed_refresh_handoff",
    python_callable=dispatch_public_docs_seed_refresh_handoff,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

run_pipeline = PythonOperator(
    task_id="run_public_docs_seed_refresh_pipeline",
    python_callable=execute_pipeline_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='dispatch_pipeline_seed_refresh_handoff') }}"],
    dag=dag,
)

submit_bc21_pipeline_state = PythonOperator(
    task_id="submit_public_docs_bc21_pipeline_state",
    python_callable=submit_public_docs_bc21_pipeline_state_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

write_publish_trigger_conf = PythonOperator(
    task_id="write_public_docs_publish_activation_trigger_conf",
    python_callable=write_public_docs_publish_activation_trigger_conf_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_public_docs_seed_registry') }}"],
    dag=dag,
)

(
    validate_plan
    >> write_seed_registry
    >> build_refresh_plan
    >> dispatch_handoff
    >> run_pipeline
    >> submit_bc21_pipeline_state
    >> write_publish_trigger_conf
    >> notify_governance
)
