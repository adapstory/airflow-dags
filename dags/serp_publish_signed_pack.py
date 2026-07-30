from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule

from dags.serp_eval_contracts import (
    build_public_docs_canary_resolve_cli_spec,
    build_public_docs_publish_activation_cli_spec,
    build_public_docs_publish_activation_plan,
    build_public_docs_publish_activation_submit_cli_spec,
    build_public_docs_retired_pack_cleanup_cli_spec,
    execute_pipeline_cli_spec,
    execute_public_docs_canary_rollback,
    execute_public_docs_canary_transition,
    write_airflow_plan_artifact,
    write_public_docs_coverage_proof_artifact,
    write_public_docs_crawl_state_artifact,
    write_public_docs_post_activation_rollback_artifact,
    write_public_docs_retrieval_golden_artifact,
    write_public_docs_search_serve_smoke_artifact,
)
from dags.serp_web_seed_crawl_refresh import PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG


def validate_publish_signed_pack_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return write_airflow_plan_artifact(build_public_docs_publish_activation_plan(conf))


def rollback_public_docs_post_activation_failure(plan_json: dict[str, Any] | str) -> None:
    """Compensate a failed post-activation check without converting D5 to success."""

    artifact = write_public_docs_post_activation_rollback_artifact(plan_json)
    payload = artifact.get("payload")
    if isinstance(payload, dict) and payload.get("status") == "first_activation_no_restore_target":
        return
    raise AirflowException(
        "D5 post-activation validation failed; automatic rollback completed: "
        + str(artifact["artifactPath"])
    )


def select_public_docs_canary_path(plan_json: dict[str, Any] | str) -> str:
    """Select exactly one governed rollout path from the immutable D5 plan."""

    plan = json.loads(plan_json) if isinstance(plan_json, str) else dict(plan_json)
    rollout_mode = plan.get("canary_rollout_mode")
    if rollout_mode == "bootstrap":
        return "start_public_docs_bootstrap_shadow"
    if rollout_mode == "continuous":
        return "start_public_docs_canary_shadow"
    raise AirflowException("D5 plan canary_rollout_mode is unsupported")


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
    is_paused_upon_creation=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["serp", "public-docs", "publish", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_publish_signed_pack_plan",
    python_callable=validate_publish_signed_pack_plan,
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

dispatch_handoff = PythonOperator(
    task_id="dispatch_publish_activation_handoff",
    python_callable=build_public_docs_publish_activation_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

run_handoff = PythonOperator(
    task_id="run_publish_activation_handoff",
    python_callable=execute_pipeline_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='dispatch_publish_activation_handoff') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

dispatch_canary_resolve = PythonOperator(
    task_id="dispatch_public_docs_canary_resolve",
    python_callable=build_public_docs_canary_resolve_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

resolve_canary_approval = PythonOperator(
    task_id="resolve_publish_activation_approval",
    python_callable=execute_pipeline_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='dispatch_public_docs_canary_resolve') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

select_canary_path = BranchPythonOperator(
    task_id="select_public_docs_canary_path",
    python_callable=select_public_docs_canary_path,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

start_bootstrap_shadow = PythonOperator(
    task_id="start_public_docs_bootstrap_shadow",
    python_callable=execute_public_docs_canary_transition,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "bootstrap-shadow",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

mark_bootstrap_ready = PythonOperator(
    task_id="mark_public_docs_bootstrap_ready",
    python_callable=execute_public_docs_canary_transition,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "bootstrap-ready",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

start_canary_shadow = PythonOperator(
    task_id="start_public_docs_canary_shadow",
    python_callable=execute_public_docs_canary_transition,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "shadow",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

start_canary_5 = PythonOperator(
    task_id="start_public_docs_canary_5",
    python_callable=execute_public_docs_canary_transition,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "canary-5",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

wait_canary_5 = TimeDeltaSensor(
    task_id="wait_public_docs_canary_5_window",
    delta=timedelta(minutes=30),
    deferrable=True,
    dag=dag,
)

run_canary_5_golden = PythonOperator(
    task_id="run_public_docs_canary_5_golden",
    python_callable=write_public_docs_retrieval_golden_artifact,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "canary-5",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

rollback_canary_5 = PythonOperator(
    task_id="rollback_public_docs_canary_5_failure",
    python_callable=execute_public_docs_canary_rollback,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "canary-5",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

start_canary_25 = PythonOperator(
    task_id="start_public_docs_canary_25",
    python_callable=execute_public_docs_canary_transition,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "canary-25",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

wait_canary_25 = TimeDeltaSensor(
    task_id="wait_public_docs_canary_25_window",
    delta=timedelta(hours=2),
    deferrable=True,
    dag=dag,
)

run_canary_25_golden = PythonOperator(
    task_id="run_public_docs_canary_25_golden",
    python_callable=write_public_docs_retrieval_golden_artifact,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "canary-25",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

rollback_canary_25 = PythonOperator(
    task_id="rollback_public_docs_canary_25_failure",
    python_callable=execute_public_docs_canary_rollback,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "canary-25",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

mark_canary_ready = PythonOperator(
    task_id="mark_public_docs_canary_ready",
    python_callable=execute_public_docs_canary_transition,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}",
        "ready",
    ],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

dispatch_submit = PythonOperator(
    task_id="dispatch_publish_activation_submit",
    python_callable=build_public_docs_publish_activation_submit_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

submit_activation = PythonOperator(
    task_id="submit_publish_activation_to_bc21",
    python_callable=execute_pipeline_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='dispatch_publish_activation_submit') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

verify_search_serve = PythonOperator(
    task_id="verify_public_docs_search_serve",
    python_callable=write_public_docs_search_serve_smoke_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

run_retrieval_golden = PythonOperator(
    task_id="run_public_docs_retrieval_golden",
    python_callable=write_public_docs_retrieval_golden_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

rollback_post_activation_failure = PythonOperator(
    task_id="rollback_public_docs_post_activation_failure",
    python_callable=rollback_public_docs_post_activation_failure,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=2,
    retry_delay=timedelta(seconds=30),
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

write_coverage_proof = PythonOperator(
    task_id="write_public_docs_coverage_proof",
    python_callable=write_public_docs_coverage_proof_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

commit_crawl_state = PythonOperator(
    task_id="commit_public_docs_crawl_state",
    python_callable=write_public_docs_crawl_state_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    dag=dag,
)

build_retired_pack_cleanup = PythonOperator(
    task_id="build_retired_public_docs_pack_cleanup",
    python_callable=build_public_docs_retired_pack_cleanup_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

cleanup_retired_pack_versions = PythonOperator(
    task_id="cleanup_retired_public_docs_pack_versions",
    python_callable=execute_pipeline_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='build_retired_public_docs_pack_cleanup') }}"],
    executor_config=PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG,
    retries=1,
    dag=dag,
)

(
    validate_plan
    >> dispatch_handoff
    >> run_handoff
    >> dispatch_canary_resolve
    >> resolve_canary_approval
    >> select_canary_path
)

(
    select_canary_path
    >> start_canary_shadow
    >> start_canary_5
    >> wait_canary_5
    >> run_canary_5_golden
    >> start_canary_25
    >> wait_canary_25
    >> run_canary_25_golden
    >> mark_canary_ready
)

select_canary_path >> start_bootstrap_shadow >> mark_bootstrap_ready
mark_canary_ready >> dispatch_submit
mark_bootstrap_ready >> dispatch_submit

(
    dispatch_submit
    >> submit_activation
    >> verify_search_serve
    >> run_retrieval_golden
    >> write_coverage_proof
    >> commit_crawl_state
    >> build_retired_pack_cleanup
    >> cleanup_retired_pack_versions
)

# The compensation task has both validation steps as direct parents: Airflow
# evaluates ONE_FAILED only across direct upstream tasks.
verify_search_serve >> rollback_post_activation_failure
run_retrieval_golden >> rollback_post_activation_failure
run_canary_5_golden >> rollback_canary_5
run_canary_25_golden >> rollback_canary_25
