from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from loguru import logger
# ---------------------------------------------------------
# HOST FastAPI
HOST = "host.docker.internal"

# ---------------------------------------------------------
# Направления Россия ↔ Дубай
ROUTES = [
    ("MOW", "DXB"),
    ("LED", "DXB"),
    ("SVX", "DXB"),
    ("KUF", "DXB"),
    ("KZN", "DXB"),
    ("UFA", "DXB"),
    ("IKT", "DXB"),
    ("OVB", "DXB"),

    ("DXB", "MOW"),
    ("DXB", "LED"),
    ("DXB", "SVX"),
    ("DXB", "KUF"),
    ("DXB", "KZN"),
    ("DXB", "UFA"),
    ("DXB", "IKT"),
    ("DXB", "OVB"),
]

# ---------------------------------------------------------
# Параметры периода
MONTHS_BACK = 1      # сколько месяцев назад
MONTHS_FORWARD = 4      # сколько месяцев вперёд
RETURN_DELTA = 5       # сколько дней между вылетом и возвратом


def generate_dates():
    """Генерирует даты на нужный период."""
    today = datetime.now()

    start_date = today - timedelta(days=MONTHS_BACK * 30)
    end_date = today + timedelta(days=MONTHS_FORWARD * 30)

    delta = timedelta(days=1)

    while start_date <= end_date:
        yield start_date
        start_date += delta


def task_trigger_aviasales_prices():
    logger.info("Запуск DAG Aviasales")

    for origin, destination in ROUTES:
        logger.info(f"Маршрут: {origin} → {destination}")

        for departure in generate_dates():
            return_at = departure + timedelta(days=RETURN_DELTA)

            json_data = {
                "origin": origin,
                "destination": destination,
                "departure_at": departure.strftime("%Y-%m-%d"),
                "return_at": return_at.strftime("%Y-%m-%d"),
                "limit": 20
            }

            logger.info(f"Запрос к FastAPI: {json_data}")

            try:
                response = requests.post(
                    f"http://{HOST}:8081/prices",
                    json=json_data,
                    timeout=60
                )
            except Exception as e:
                logger.error(f"Ошибка соединения: {e}")
                continue

            if response.status_code != 200:
                logger.error(f"Ошибка API ({origin}->{destination}): {response.text}")
                continue

            logger.info(f"Ответ Aviasales: {response.json()}")

    return True


# ---------------------------------------------------------
# DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 10),
    'retries': 0,
}

dag = DAG(
    'aviasales_full_parser',
    default_args=default_args,
    description='Парсинг Aviasales Россия ↔ Дубай, даты назад + вперёд',
    schedule_interval='@daily',   # запускаем 1 раз в день
)

with dag:
    task_prices = PythonOperator(
        task_id='task_trigger_aviasales_prices',
        python_callable=task_trigger_aviasales_prices,
    )
