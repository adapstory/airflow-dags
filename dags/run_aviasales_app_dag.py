from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from loguru import logger

# Host FastAPI (если Airflow в Docker → host.docker.internal)
HOST = "host.docker.internal"

# Пример параметров
ORIGIN = "MOW"
DEST = "DXB"


def task_trigger_aviasales_prices():
    """
    Вызывает FastAPI сервис Aviasales:
    POST http://{HOST}:8082/prices

    Параметры:
    - origin: MOW
    - destination: DXB
    - departure_at: сегодня + i дней
    - return_at: через i+5 дней
    """

    logger.info("Запуск Aviasales DAG")

    for i in range(1, 4):  # пример: 3 дня вперёд
        departure = datetime.now() + timedelta(days=i)
        return_at = departure + timedelta(days=5)

        json_data = {
            "origin": ORIGIN,
            "destination": DEST,
            "departure_at": departure.strftime("%Y-%m-%d"),
            "return_at": return_at.strftime("%Y-%m-%d"),
            "limit": 20
        }

        logger.info(f"Отправка данных в FastAPI Aviasales: {json_data}")

        response = requests.post(
            f"http://{HOST}:8082/prices",
            json=json_data,
            timeout=60
        )

        if response.status_code != 200:
            logger.error(
                f"Ошибка получения данных Aviasales "
                f"(дата вылета: {json_data['departure_at']}) — {response.text}"
            )
            return False

        logger.info(
            f"Aviasales ответ (дата вылета {json_data['departure_at']}): "
            f"{response.json()}"
        )

    return True


# -------------------------
# DAG НАСТРОЙКА
# -------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 10),
    'retries': 0,
}

dag = DAG(
    'aviasales_api',
    default_args=default_args,
    description='Triggers Aviasales FastAPI service',
    schedule_interval='*/30 * * * *',  # каждые 30 минут
)

with dag:
    task_prices = PythonOperator(
        task_id='task_trigger_aviasales_prices',
        python_callable=task_trigger_aviasales_prices,
    )

    task_prices
