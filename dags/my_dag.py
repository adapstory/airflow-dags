from datetime import UTC, datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def report_airflow_smoke() -> str:
    timestamp = datetime.now(UTC).isoformat()
    print(f"Airflow smoke DAG executed at {timestamp}")
    return timestamp


default_args = {
    "owner": "platform",
    "start_date": datetime(2026, 7, 1, tzinfo=UTC),
    "retries": 1,
}

dag = DAG(
    "platform_airflow_smoke",
    default_args=default_args,
    description="Minimal production-safe smoke DAG for the Adapstory Airflow runtime",
    schedule="@daily",
    catchup=False,
    tags=["platform", "smoke"],
)

start = EmptyOperator(
    task_id="start",
    dag=dag,
)

report = PythonOperator(
    task_id="report_airflow_smoke",
    python_callable=report_airflow_smoke,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    dag=dag,
)

start >> report >> end
