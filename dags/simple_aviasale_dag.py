from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowException
import sys
import os

# Добавляем путь к скрипту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from aviasales_mongo_postgres import run_etl

default_args = {
    'owner': 'aviasales_etl',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['data-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

def run_etl_task(**context):
    """Задача для запуска ETL"""
    try:
        requests, tickets = run_etl()
        
        # Можно передать результаты в XCom
        context['ti'].xcom_push(key='requests_processed', value=requests)
        context['ti'].xcom_push(key='tickets_processed', value=tickets)
        
        if requests == 0 and tickets == 0:
            context['ti'].xcom_push(key='result_message', value='No new data to process')
        else:
            context['ti'].xcom_push(key='result_message', 
                                   value=f'Processed {requests} requests and {tickets} tickets')
        
        return f"Success: {requests} requests, {tickets} tickets"
        
    except Exception as e:
        raise AirflowException(f"ETL failed: {str(e)}")

def send_success_email(**context):
    """Отправка email при успешном выполнении"""
    ti = context['ti']
    requests = ti.xcom_pull(key='requests_processed', task_ids='run_etl')
    tickets = ti.xcom_pull(key='tickets_processed', task_ids='run_etl')
    message = ti.xcom_pull(key='result_message', task_ids='run_etl')
    
    email_content = f"""
    <h3>✅ Aviasales ETL Completed Successfully</h3>
    
    <p><b>Execution Time:</b> {context['execution_date']}</p>
    <p><b>Result:</b> {message}</p>
    <p><b>Details:</b></p>
    <ul>
        <li>Requests processed: {requests}</li>
        <li>Tickets processed: {tickets}</li>
    </ul>
    
    <hr>
    <p><small>This is an automated message from Airflow ETL pipeline</small></p>
    """
    
    email_operator = EmailOperator(
        task_id='send_success_email',
        to='data-team@example.com',
        subject=f'✅ Aviasales ETL Success - {context["execution_date"].strftime("%Y-%m-%d")}',
        html_content=email_content,
        dag=context['dag']
    )
    
    return email_operator.execute(context)

with DAG(
    'simple_aviasales_etl',
    default_args=default_args,
    description='Simple ETL from MongoDB to PostgreSQL for Aviasales tickets',
    schedule_interval='@daily',  # Каждые 4 часа
    catchup=False,
    max_active_runs=1,
    tags=['simple', 'etl', 'aviasales'],
) as dag:
    
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )
    
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl_task,
        provide_context=True,
        dag=dag,
    )
    
    success_email = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_email,
        provide_context=True,
        dag=dag,
        trigger_rule='all_success',
    )
    
    end = DummyOperator(
        task_id='end',
        dag=dag,
        trigger_rule='all_done',
    )
    
    start >> etl_task >> success_email >> end