"""Daily immutable source, dataset, and licensing snapshots for all SERP suites."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from dags.serp_eval_contracts import (
    build_mandatory_benchmark_dataset_evidence_plan,
    materialize_live_benchmark_catalog_artifact,
    write_airflow_plan_artifact,
)


def validate_mandatory_benchmark_dataset_evidence_plan(**context: Any) -> str:
    dag_run = context.get("dag_run")
    supplied_conf = dict(getattr(dag_run, "conf", None) or {})
    return write_airflow_plan_artifact(
        build_mandatory_benchmark_dataset_evidence_plan(
            {
                "artifact_root_path": supplied_conf.get(
                    "artifact_root_path", os.environ["ADAPSTORY_AIRFLOW_ARTIFACT_ROOT"]
                ),
                "generated_at": supplied_conf.get(
                    "generated_at", datetime.now(UTC).isoformat().replace("+00:00", "Z")
                ),
            }
        )
    )


default_args = {
    "owner": "serp-benchmark-catalog",
    "retries": 0,
    "start_date": datetime(2026, 7, 13, tzinfo=UTC),
}

dag = DAG(
    "serp_mandatory_benchmark_dataset_evidence_snapshot",
    default_args=default_args,
    description="WORM dataset, source, and licensing evidence for every mandatory SERP suite",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "evidence", "dataset"],
)

validate_plan = PythonOperator(
    task_id="validate_mandatory_benchmark_dataset_evidence_plan",
    python_callable=validate_mandatory_benchmark_dataset_evidence_plan,
    dag=dag,
)

materialize_evidence = PythonOperator(
    task_id="materialize_mandatory_benchmark_dataset_evidence",
    python_callable=materialize_live_benchmark_catalog_artifact,
    op_args=["{{ ti.xcom_pull(task_ids='validate_mandatory_benchmark_dataset_evidence_plan') }}"],
    dag=dag,
)

validate_plan >> materialize_evidence
