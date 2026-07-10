from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_public_docs_seed_refresh_plan as build_public_docs_seed_refresh_plan_contract,
)
from dags.serp_eval_contracts import (
    default_public_docs_seed_refresh_conf,
    discover_public_docs_crawler_frontier,
    dispatch_public_docs_seed_refresh_handoff,
    execute_pipeline_cli_spec,
    governance_notification_pending,
    load_public_docs_crawl_state_conf,
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
    conf = load_public_docs_crawl_state_conf(conf)
    return write_airflow_plan_artifact(
        build_public_docs_seed_refresh_plan_contract(
            conf,
            sitemap_frontier_discoverer=discover_public_docs_crawler_frontier,
        )
    )


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


def prepare_public_docs_d5_dispatch(**context: Any) -> dict[str, Any]:
    """Return the only D5 configuration D20 is permitted to dispatch."""

    task_instance = context.get("ti")
    if task_instance is None:
        raise ValueError("Airflow task instance is required for D5 dispatch")
    trigger_artifact = task_instance.xcom_pull(
        task_ids="write_public_docs_publish_activation_trigger_conf"
    )
    if not isinstance(trigger_artifact, Mapping):
        raise ValueError("D20 publish trigger artifact must be an object")
    payload = trigger_artifact.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("D20 publish trigger artifact payload must be an object")
    status = payload.get("status")
    if status == "no_change_active_pack_retained":
        raise AirflowSkipException("public docs no-op: D5 activation is not dispatched")
    if status != "ready_for_d5_publish_activation":
        raise ValueError(f"D20 publish trigger artifact is not dispatchable: status={status!r}")
    if payload.get("target_dag_id") != "serp_publish_signed_pack":
        raise ValueError("D20 publish trigger target DAG is invalid")
    target_conf = payload.get("target_dag_run_conf")
    if not isinstance(target_conf, Mapping) or not target_conf:
        raise ValueError("D20 publish trigger target configuration must be a non-empty object")
    return dict(target_conf)


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
    max_active_runs=1,
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
    retries=1,
    retry_delay=timedelta(seconds=5),
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

prepare_d5_dispatch = PythonOperator(
    task_id="prepare_public_docs_d5_dispatch",
    python_callable=prepare_public_docs_d5_dispatch,
    dag=dag,
)

trigger_d5_publish_activation = TriggerDagRunOperator(
    task_id="trigger_public_docs_d5_publish_activation",
    trigger_dag_id="serp_publish_signed_pack",
    trigger_run_id="d5-from-{{ dag_run.run_id }}",
    conf="{{ ti.xcom_pull(task_ids='prepare_public_docs_d5_dispatch') }}",
    wait_for_completion=True,
    allowed_states=["success"],
    failed_states=["failed"],
    skip_when_already_exists=True,
    fail_when_dag_is_paused=True,
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
    >> prepare_d5_dispatch
    >> trigger_d5_publish_activation
    >> notify_governance
)
