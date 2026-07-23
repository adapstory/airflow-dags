from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from dags.serp_evidence_workload_identity import (
    bc21_authorized_minio_executor_config,
    minio_web_identity_executor_config,
)
from dags.serp_kubernetes_executor import task_secret_env_var
from dags.serp_public_docs_context_benchmark_contracts import (
    enforce_context_benchmark_gate,
    execute_context_benchmark,
    publish_context_benchmark_github_status,
    submit_context_benchmark_bc21_runs,
    write_context_benchmark_plan,
)

_CONTEXT_BENCHMARK_SERVICE_ACCOUNT = "airflow-serp-public-docs-acquisition"
_CONTEXT_BENCHMARK_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "public-docs-acquisition",
}
CONTEXT_EVIDENCE_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name=_CONTEXT_BENCHMARK_SERVICE_ACCOUNT,
    labels=_CONTEXT_BENCHMARK_LABELS,
)
CONTEXT_BC21_EXECUTOR_CONFIG = bc21_authorized_minio_executor_config(
    service_account_name=_CONTEXT_BENCHMARK_SERVICE_ACCOUNT,
    labels=_CONTEXT_BENCHMARK_LABELS,
)
GITHUB_STATUS_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name=_CONTEXT_BENCHMARK_SERVICE_ACCOUNT,
    labels=_CONTEXT_BENCHMARK_LABELS,
    additional_env_vars=[
        task_secret_env_var(
            name="ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN",
            secret_name="airflow-serp-github-status",
            secret_key="token",
        ),
        k8s.V1EnvVar(
            name="HTTP_PROXY",
            value="http://forward-proxy.forward-proxy.svc.cluster.local:3128",
        ),
        k8s.V1EnvVar(
            name="HTTPS_PROXY",
            value="http://forward-proxy.forward-proxy.svc.cluster.local:3128",
        ),
        k8s.V1EnvVar(
            name="NO_PROXY",
            value="localhost,127.0.0.1,::1,.svc,.svc.cluster.local,cluster.local",
        ),
    ],
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
    executor_config=CONTEXT_EVIDENCE_EXECUTOR_CONFIG,
    dag=dag,
)

execute_benchmark = PythonOperator(
    task_id="execute_context_benchmark",
    python_callable=execute_context_benchmark,
    op_args=["{{ ti.xcom_pull(task_ids='write_context_benchmark_plan') }}"],
    executor_config=CONTEXT_EVIDENCE_EXECUTOR_CONFIG,
    dag=dag,
)

submit_bc21 = PythonOperator(
    task_id="submit_context_benchmark_bc21_runs",
    python_callable=submit_context_benchmark_bc21_runs,
    op_args=[
        "{{ ti.xcom_pull(task_ids='write_context_benchmark_plan') }}",
        "{{ ti.xcom_pull(task_ids='execute_context_benchmark') }}",
    ],
    executor_config=CONTEXT_BC21_EXECUTOR_CONFIG,
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
    executor_config=GITHUB_STATUS_EXECUTOR_CONFIG,
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
