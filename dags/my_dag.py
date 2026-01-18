from datetime import datetime
from time import sleep

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def my_task():
    print("Hello, Airflow!")
    sleep(10)
    print("Task completed.")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 29),
    'retries': 1,
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',  # "0 9 * * *" – cron expression for daily at midnight
)

task = PythonOperator(
    task_id='my_task',
    python_callable=my_task,
    dag=dag,
)