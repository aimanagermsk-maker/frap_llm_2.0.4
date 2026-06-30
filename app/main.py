# app/main.py
import asyncio
import logging
from contextlib import asynccontextmanager
import socket
import os
import psutil

from fastapi import FastAPI
from app.config.app_config import get_config
from app.routers import hello_router
from app.services.kafka_client import KafkaClient
from app.services.db_client import DatabaseClient
from app.services.processor import process_message
from app.services.logging_service import LoggingService, log_service
from app.utils.logging_handler import setup_logging_with_db
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)

# Глобальные переменные
kafka_client = None
db_client = None
logging_service = None
config = get_config()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    global kafka_client, db_client, logging_service
    
    # Настройка логирования
    setup_logging()
    setup_logging_with_db()
    
    # Логируем активную конфигурацию
    logger.info("Запуск сервиса frap-llm-helper", extra={'extra_fields': {
        'service_name': 'frap-llm-helper',
        'version': '1.0.0',
        'active_profile': os.getenv('PYTHON_PROFILES_ACTIVE', 'sandbox'),
        'hostname': socket.gethostname(),
        'pid': os.getpid()
    }})
    
    try:
        # 1. Инициализация клиента PostgreSQL (основной)
        db_client = DatabaseClient(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        await db_client.connect()
        logger.info("PostgreSQL клиент инициализирован")
        
        # 2. Инициализация сервиса логирования (использует ту же БД)
        await log_service.initialize(db_client)
        logging_service = log_service
        
        # 3. Инициализация клиента Kafka
        kafka_client = KafkaClient(
            bootstrap_servers=config.kafka.bootstrap_servers,
            consumer_group_id=config.kafka.consumer_group_id,
            input_topic=config.kafka.input_topic,
            output_topic=config.kafka.output_topic
        )
        await kafka_client.start()
        logger.info("Kafka клиент инициализирован")
        
        # 4. Запуск фоновой задачи для чтения Kafka
        consumer_task = asyncio.create_task(
            consume_kafka_messages(kafka_client, db_client, config)
        )
        logger.info("Фоновый Consumer Kafka запущен")
        
        # Передаем управление приложению
        yield
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации сервиса: {e}", exc_info=True)
        raise
    finally:
        # 5. Очистка ресурсов
        if 'consumer_task' in locals():
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        
        if kafka_client:
            await kafka_client.stop()
        
        if logging_service:
            await logging_service.shutdown()
        
        if db_client:
            await db_client.disconnect()
        
        logger.info("Ресурсы освобождены")

app = FastAPI(
    title="FRAP LLM Helper",
    description="Сервис для обработки данных через LLM",
    version="1.0.0",
    lifespan=lifespan
)

# Подключаем роутеры
app.include_router(hello_router.router)

async def consume_kafka_messages(kafka_client: KafkaClient, db_client: DatabaseClient, config):
    """Фоновая задача для чтения сообщений из Kafka"""
    async for message in kafka_client.consume():
        try:
            # Вызываем основной обработчик с логированием
            await process_message(message, db_client, kafka_client, config)
        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
