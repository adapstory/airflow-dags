from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_benchmark_catalog import mandatory_benchmark_adapters_ready
from dags.serp_benchmark_catalog_workload import (
    BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    benchmark_catalog_acquisition_container_security_context,
    benchmark_catalog_acquisition_env_vars,
    benchmark_catalog_acquisition_pod_security_context,
    benchmark_catalog_acquisition_web_identity_volume_mounts,
    benchmark_catalog_acquisition_web_identity_volumes,
)
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    build_nightly_benchmark_export_cli_spec,
    build_nightly_registry_cli_spec,
    build_nightly_registry_submit_cli_spec,
    build_nightly_regression_plan,
    build_nightly_runner_cli_spec,
    default_nightly_regression_conf,
    execute_gateway_cli_spec,
    governance_notification_pending,
    load_materialized_benchmark_catalog_snapshot,
    nightly_regression_runtime_ready,
    write_airflow_plan_artifact,
    write_nightly_suite_plan_artifact,
)
from dags.serp_web_seed_crawl_refresh import (
    current_airflow_runtime_image,
)


def validate_nightly_regression_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    conf = _nightly_regression_conf_with_defaults(getattr(dag_run, "conf", None) or {})
    return write_airflow_plan_artifact(build_nightly_regression_plan(conf))


def _nightly_regression_conf_with_defaults(supplied_conf: dict[str, Any]) -> dict[str, Any]:
    """Accept only a canonical suite assertion; D6 runtime context is deployment-owned."""

    conf = dict(supplied_conf)
    allowed_fields = {"generated_at", "selected_suite_ids"}
    unexpected_fields = sorted(set(conf).difference(allowed_fields))
    if unexpected_fields:
        raise ValueError(
            "D6 runtime context is GitOps-owned; dag_run.conf may only set generated_at "
            "or assert the canonical selected_suite_ids"
        )
    selected_suite_ids = conf.get("selected_suite_ids")
    if selected_suite_ids is not None and selected_suite_ids != list(
        MANDATORY_SERP_BENCHMARK_SUITES
    ):
        raise ValueError("selected_suite_ids must include every mandatory suite")
    generated_at = str(
        conf.get("generated_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    return default_nightly_regression_conf(generated_at=generated_at)


def run_mandatory_benchmark_suites(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_nightly_runner_cli_spec(plan_json))


def load_materialized_benchmark_catalog(plan_json: str) -> dict[str, Any]:
    return load_materialized_benchmark_catalog_snapshot(plan_json)


def build_c1_benchmark_gate_export(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_nightly_benchmark_export_cli_spec(plan_json))


def build_bc21_benchmark_run_submissions(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_nightly_registry_cli_spec(plan_json))


def submit_bc21_benchmark_run_submissions(plan_json: str) -> dict[str, Any]:
    return execute_gateway_cli_spec(build_nightly_registry_submit_cli_spec(plan_json))


default_args = {
    "owner": "serp-eval-runner",
    "start_date": datetime(2026, 7, 5, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_nightly_regression_suite",
    default_args=default_args,
    description="SERP D6 nightly benchmark regression gate contract",
    schedule=(
        "@daily"
        if mandatory_benchmark_adapters_ready() and nightly_regression_runtime_ready()
        else None
    ),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "bc21"],
)

validate_plan = PythonOperator(
    task_id="validate_nightly_regression_plan",
    python_callable=validate_nightly_regression_plan,
    dag=dag,
)

materialize_catalog = KubernetesPodOperator(
    task_id="materialize_live_benchmark_catalog",
    name="serp-mandatory-benchmark-catalog-acquisition",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "dags.serp_benchmark_catalog_materializer"],
    arguments=[
        "--plan-json-urlencoded",
        "{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') | urlencode }}",
    ],
    env_vars=benchmark_catalog_acquisition_env_vars(),
    service_account_name=BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=benchmark_catalog_acquisition_web_identity_volumes(),
    volume_mounts=benchmark_catalog_acquisition_web_identity_volume_mounts(),
    labels=BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    container_resources=BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    security_context=benchmark_catalog_acquisition_pod_security_context(),
    container_security_context=benchmark_catalog_acquisition_container_security_context(),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=1,
    retry_delay=timedelta(seconds=BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS),
    dag=dag,
)

load_catalog = PythonOperator(
    task_id="load_materialized_benchmark_catalog",
    python_callable=load_materialized_benchmark_catalog,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

write_suite_plan = PythonOperator(
    task_id="write_nightly_suite_plan",
    python_callable=write_nightly_suite_plan_artifact,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog') }}",
    ],
    dag=dag,
)

run_suites = PythonOperator(
    task_id="run_mandatory_benchmark_suites",
    python_callable=run_mandatory_benchmark_suites,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

build_benchmark_export = PythonOperator(
    task_id="build_c1_benchmark_gate_export",
    python_callable=build_c1_benchmark_gate_export,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

build_submissions = PythonOperator(
    task_id="build_bc21_benchmark_run_submissions",
    python_callable=build_bc21_benchmark_run_submissions,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

submit_submissions = PythonOperator(
    task_id="submit_bc21_benchmark_run_submissions",
    python_callable=submit_bc21_benchmark_run_submissions,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_nightly_regression_plan') }}"],
    dag=dag,
)

(
    validate_plan
    >> materialize_catalog
    >> load_catalog
    >> write_suite_plan
    >> run_suites
    >> build_benchmark_export
    >> build_submissions
    >> submit_submissions
    >> notify_governance
)
