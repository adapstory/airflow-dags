import os
import json
from bson import ObjectId
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Text
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

load_dotenv()

def get_config():
    """Получение конфигурации"""
    return {
        "mongo": {
            "username": os.getenv("MONGO_INITDB_ROOT_USERNAME"),
            "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD"),
            "host": os.getenv("MONGO_HOST"),
            "port": int(os.getenv("MONGO_PORT")),
            "authSource": "admin",
        },
        "postgres": {
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
            "url": f"postgresql://"
                   f"{os.getenv('POSTGRES_USER')}:"
                   f"{os.getenv('POSTGRES_PASSWORD')}@"
                   f"{os.getenv('POSTGRES_HOST')}:"
                   f"{os.getenv('POSTGRES_PORT')}/"
                   f"{os.getenv('POSTGRES_DB')}"
        }
    }


def get_mongo_connection():
    """Создание подключения к MongoDB"""
    try:
        config = get_config()["mongo"]
        client = MongoClient(
            host=config["host"],
            port=config["port"],
            username=config["username"],
            password=config["password"],
            authSource=config["authSource"]
        )
        logger.info(f"Connected to MongoDB at {config['host']}:{config['port']}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def get_postgres_engine():
    """Создание подключения к PostgreSQL"""
    try:
        config = get_config()["postgres"]
        engine = create_engine(config["url"])
        logger.info(f"Connected to PostgreSQL at {config['host']}:{config['port']}")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def create_tables_if_not_exist(engine):
    """Создание таблиц если они не существуют"""
    try:
        with engine.connect() as conn:
            # Создаем схему если не существует
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS aviasales;"))
            
            # Создаем таблицу для логов запросов
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS aviasales.request_logs (
                    mongo_id VARCHAR PRIMARY KEY,
                    origin VARCHAR(3),
                    destination VARCHAR(3),
                    departure_date DATE,
                    return_date DATE,
                    limit_results INTEGER,
                    currency VARCHAR(3),
                    success BOOLEAN,
                    status_code INTEGER,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    mongo_data JSONB,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Создаем таблицу для билетов
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS aviasales.tickets (
                    id SERIAL PRIMARY KEY,
                    request_id VARCHAR REFERENCES aviasales.request_logs(mongo_id),
                    flight_number VARCHAR(20),
                    link TEXT,
                    origin_airport VARCHAR(3),
                    destination_airport VARCHAR(3),
                    departure_at TIMESTAMP,
                    airline VARCHAR(10),
                    destination VARCHAR(3),
                    return_at TIMESTAMP,
                    origin VARCHAR(3),
                    price DECIMAL(10, 2),
                    return_transfers INTEGER,
                    duration INTEGER,
                    duration_to INTEGER,
                    duration_back INTEGER,
                    transfers INTEGER,
                    currency VARCHAR(3),
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(request_id, flight_number, departure_at, airline)
                );
            """))
            
            # Создаем индексы для быстрого поиска
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_tickets_route 
                ON aviasales.tickets(origin, destination, departure_at);
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_tickets_price 
                ON aviasales.tickets(price);
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_tickets_airline 
                ON aviasales.tickets(airline);
            """))
            
            logger.info("Tables created or already exist in PostgreSQL")
            
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise


def extract_data_from_mongo(since_date: datetime = None) -> List[Dict[str, Any]]:
    """Извлечение данных из MongoDB"""
    client = None
    try:
        client = get_mongo_connection()
        db = client["aviasales_db"]
        collection = db["prices_logs"]
        
        # Определяем фильтр
        filter_query = {}
        if since_date:
            filter_query["created_at"] = {"$gte": since_date}
            logger.info(f"Extracting data since {since_date}")
        else:
            logger.info("Extracting all data")
        
        # Получаем данные
        data = list(collection.find(filter_query).sort("created_at", 1))
        logger.info(f"Extracted {len(data)} documents from MongoDB")
        
        return data
        
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise
    finally:
        if client:
            client.close()


def mongo_to_json(data: Any) -> Any:
    if isinstance(data, ObjectId):
        return str(data)
    if isinstance(data, datetime):
        return data.isoformat()
    if isinstance(data, dict):
        return {k: mongo_to_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [mongo_to_json(v) for v in data]
    return data


def transform_request_data(mongo_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Трансформация данных запроса"""
    try:
        mongo_id = str(mongo_doc["_id"])
        request_data = mongo_doc.get("request", {})
        response_data = mongo_doc.get("response", {})
        
        # Преобразуем строки дат в объекты datetime
        departure_date = None
        return_date = None
        
        if request_data.get("departure_at"):
            try:
                departure_date = datetime.fromisoformat(request_data["departure_at"].replace("Z", "+00:00")).date()
            except:
                pass
        
        if request_data.get("return_at"):
            try:
                return_date = datetime.fromisoformat(request_data["return_at"].replace("Z", "+00:00")).date()
            except:
                pass
        
        transformed = {
            "mongo_id": mongo_id,
            "origin": request_data.get("origin"),
            "destination": request_data.get("destination"),
            "departure_date": departure_date,
            "return_date": return_date,
            "limit_results": request_data.get("limit"),
            "currency": response_data.get("currency", "rub"),
            "success": response_data.get("success", False),
            "status_code": mongo_doc.get("status_code", 200),
            "created_at": mongo_doc.get("created_at"),
            "updated_at": mongo_doc.get("updated_at"),
            "mongo_data": json.dumps(mongo_to_json(mongo_doc))
        }
        
        return transformed
        
    except Exception as e:
        logger.error(f"Error transforming request data: {e}")
        raise


def transform_ticket_data(ticket: Dict[str, Any], request_id: str) -> Dict[str, Any]:
    """Трансформация данных билета"""
    try:
        # Преобразуем строки дат в timestamp
        departure_at = None
        return_at = None
        
        if ticket.get("departure_at"):
            try:
                departure_at = datetime.fromisoformat(ticket["departure_at"].replace("Z", "+00:00"))
            except:
                pass
        
        if ticket.get("return_at"):
            try:
                return_at = datetime.fromisoformat(ticket["return_at"].replace("Z", "+00:00"))
            except:
                pass
        
        transformed = {
            "request_id": request_id,
            "flight_number": ticket.get("flight_number"),
            "link": ticket.get("link"),
            "origin_airport": ticket.get("origin_airport"),
            "destination_airport": ticket.get("destination_airport"),
            "departure_at": departure_at,
            "airline": ticket.get("airline"),
            "destination": ticket.get("destination"),
            "return_at": return_at,
            "origin": ticket.get("origin"),
            "price": float(ticket.get("price", 0)) if ticket.get("price") else None,
            "return_transfers": ticket.get("return_transfers", 0),
            "duration": ticket.get("duration", 0),
            "duration_to": ticket.get("duration_to", 0),
            "duration_back": ticket.get("duration_back", 0),
            "transfers": ticket.get("transfers", 0),
            "currency": ticket.get("currency", "rub")
        }
        
        return transformed
        
    except Exception as e:
        logger.error(f"Error transforming ticket data: {e}")
        raise


def load_data_to_postgres(data: List[Dict[str, Any]]):
    """Загрузка данных в PostgreSQL"""
    engine = None
    try:
        engine = get_postgres_engine()
        create_tables_if_not_exist(engine)
        
        total_tickets = 0
        total_requests = 0
        
        with engine.begin() as conn:  # begin() автоматически коммитит или откатывает
            for doc in data:
                # 1. Загружаем данные запроса
                request_data = transform_request_data(doc)
                
                # Проверяем, существует ли уже этот запрос
                check_query = text("""
                    SELECT COUNT(*) 
                    FROM aviasales.request_logs 
                    WHERE mongo_id = :mongo_id
                """)
                exists = conn.execute(check_query, {"mongo_id": request_data["mongo_id"]}).scalar()
                
                if exists == 0:
                    insert_request_query = text("""
                        INSERT INTO aviasales.request_logs 
                        (mongo_id, origin, destination, departure_date, return_date, 
                         limit_results, currency, success, status_code, created_at, 
                         updated_at, mongo_data)
                        VALUES 
                        (:mongo_id, :origin, :destination, :departure_date, :return_date,
                         :limit_results, :currency, :success, :status_code, :created_at,
                         :updated_at, :mongo_data)
                    """)
                    conn.execute(insert_request_query, request_data)
                    total_requests += 1
                    logger.info(f"Inserted request: {request_data['mongo_id']}")
                else:
                    logger.info(f"Request already exists: {request_data['mongo_id']}")
                
                # 2. Загружаем данные билетов
                tickets = doc.get("response", {}).get("data", [])
                request_id = request_data["mongo_id"]
                
                for ticket in tickets:
                    ticket_data = transform_ticket_data(ticket, request_id)
                    
                    # Проверяем уникальность билета
                    check_ticket_query = text("""
                        SELECT COUNT(*) 
                        FROM aviasales.tickets 
                        WHERE request_id = :request_id 
                        AND flight_number = :flight_number 
                        AND departure_at = :departure_at 
                        AND airline = :airline
                    """)
                    
                    ticket_exists = conn.execute(
                        check_ticket_query,
                        {
                            "request_id": request_id,
                            "flight_number": ticket_data.get("flight_number"),
                            "departure_at": ticket_data.get("departure_at"),
                            "airline": ticket_data.get("airline")
                        }
                    ).scalar()
                    
                    if ticket_exists == 0:
                        insert_ticket_query = text("""
                            INSERT INTO aviasales.tickets 
                            (request_id, flight_number, link, origin_airport, 
                             destination_airport, departure_at, airline, destination,
                             return_at, origin, price, return_transfers, duration,
                             duration_to, duration_back, transfers, currency)
                            VALUES 
                            (:request_id, :flight_number, :link, :origin_airport,
                             :destination_airport, :departure_at, :airline, :destination,
                             :return_at, :origin, :price, :return_transfers, :duration,
                             :duration_to, :duration_back, :transfers, :currency)
                        """)
                        conn.execute(insert_ticket_query, ticket_data)
                        total_tickets += 1
        
        logger.info(f"✅ Loaded {total_requests} requests and {total_tickets} tickets to PostgreSQL")
        return total_requests, total_tickets
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {e}")
        raise
    finally:
        if engine:
            engine.dispose()


def get_last_processed_date(engine) -> datetime:
    """Получение даты последней обработки"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT MAX(created_at) 
                FROM aviasales.request_logs
            """)
            result = conn.execute(query).scalar()
            
            if result:
                logger.info(f"Last processed date: {result}")
                return result
            else:
                logger.info("No previous data found, starting from beginning")
                return None
                
    except Exception as e:
        logger.error(f"Error getting last processed date: {e}")
        # Если таблицы нет, возвращаем None
        return None


def run_etl():
    """Основная функция ETL"""
    logger.info("🚀 Starting ETL process from MongoDB to PostgreSQL")
    
    postgres_engine = None
    try:
        # 1. Получаем дату последней обработки
        postgres_engine = get_postgres_engine()
        last_date = get_last_processed_date(postgres_engine)
        
        # 2. Извлекаем данные из MongoDB
        mongo_data = extract_data_from_mongo(last_date)
        
        if not mongo_data:
            logger.info("No new data to process")
            return 0, 0
        
        # 3. Загружаем данные в PostgreSQL
        requests_loaded, tickets_loaded = load_data_to_postgres(mongo_data)
        
        logger.info(f"🎉 ETL completed successfully!")
        logger.info(f"📊 Statistics: {requests_loaded} requests, {tickets_loaded} tickets")
        
        return requests_loaded, tickets_loaded
        
    except Exception as e:
        logger.error(f"❌ ETL process failed: {e}")
        raise
    finally:
        if postgres_engine:
            postgres_engine.dispose()


if __name__ == "__main__":
    # Для локального тестирования
    run_etl()