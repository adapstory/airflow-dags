from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_public_docs_publish_activation_cli_spec,
    build_public_docs_publish_activation_plan,
    build_public_docs_publish_activation_submit_cli_spec,
    build_public_docs_retired_pack_cleanup_cli_spec,
    execute_pipeline_cli_spec,
    governance_notification_pending,
    write_airflow_plan_artifact,
    write_public_docs_coverage_proof_artifact,
    write_public_docs_crawl_state_artifact,
    write_public_docs_retrieval_golden_artifact,
    write_public_docs_search_serve_smoke_artifact,
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
    max_active_runs=1,
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

dispatch_submit = PythonOperator(
    task_id="dispatch_publish_activation_submit",
    python_callable=build_public_docs_publish_activation_submit_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

submit_activation = PythonOperator(
    task_id="submit_publish_activation_to_bc21",
    python_callable=execute_pipeline_cli_spec,
    op_args=["{{ ti.xcom_pull(task_ids='dispatch_publish_activation_submit') }}"],
    dag=dag,
)

verify_search_serve = PythonOperator(
    task_id="verify_public_docs_search_serve",
    python_callable=write_public_docs_search_serve_smoke_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

run_retrieval_golden = PythonOperator(
    task_id="run_public_docs_retrieval_golden",
    python_callable=write_public_docs_retrieval_golden_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

write_coverage_proof = PythonOperator(
    task_id="write_public_docs_coverage_proof",
    python_callable=write_public_docs_coverage_proof_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

commit_crawl_state = PythonOperator(
    task_id="commit_public_docs_crawl_state",
    python_callable=write_public_docs_crawl_state_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
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
    retries=1,
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_publish_signed_pack_plan') }}"],
    dag=dag,
)

(
    validate_plan
    >> dispatch_handoff
    >> run_handoff
    >> dispatch_submit
    >> submit_activation
    >> verify_search_serve
    >> run_retrieval_golden
    >> write_coverage_proof
    >> commit_crawl_state
    >> build_retired_pack_cleanup
    >> cleanup_retired_pack_versions
    >> notify_governance
)
