import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
from collections import deque
from contextlib import asynccontextmanager

import asyncpg
from asyncpg import Pool

from app.config.app_config import get_config
from app.services.db_client import DatabaseClient

logger = logging.getLogger(__name__)

class LoggingService:
    """Сервис для асинхронного сохранения логов в PostgreSQL"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.config = get_config().logging
            self.db_client: Optional[DatabaseClient] = None
            self.log_queue: deque = deque()
            self.is_running = False
            self._flush_task: Optional[asyncio.Task] = None
            self._initialized = True
    
    async def initialize(self, db_client: DatabaseClient):
        """Инициализация сервиса логирования"""
        if not self.config.enabled:
            logger.info("Логирование в БД отключено в конфигурации")
            return
        
        self.db_client = db_client
        self.is_running = True
        
        # Создаем таблицу, если она не существует
        await self._ensure_table_exists()
        
        # Запускаем фоновую задачу для периодической записи
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info(f"Сервис логирования инициализирован (таблица: {self.config.table_name})")
    
    async def _ensure_table_exists(self):
        """Создание таблицы для логов, если она не существует"""
        table_name = self.config.table_name
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            level VARCHAR(20) NOT NULL,
            logger VARCHAR(100) NOT NULL,
            module VARCHAR(100),
            function VARCHAR(100),
            line_number INTEGER,
            message TEXT NOT NULL,
            correlation_id UUID,
            record_id VARCHAR(50),
            request_type VARCHAR(50),
            request_date DATE,
            uri VARCHAR(255),
            processing_start_time TIMESTAMP WITH TIME ZONE,
            total_processing_time_ms INTEGER,
            db_query_time_ms INTEGER,
            llm_processing_time_ms INTEGER,
            response_time_ms INTEGER,
            service_name VARCHAR(100) DEFAULT 'frap-llm-helper',
            version VARCHAR(20),
            active_profile VARCHAR(50),
            hostname VARCHAR(255),
            pid INTEGER,
            cpu_usage_percent DECIMAL(5,2),
            memory_usage_mb INTEGER,
            uptime_seconds INTEGER,
            kafka_topic VARCHAR(100),
            kafka_partition INTEGER,
            kafka_offset BIGINT,
            message_size_bytes INTEGER,
            db_host VARCHAR(255),
            db_port INTEGER,
            db_name VARCHAR(100),
            table_name VARCHAR(100),
            file_path VARCHAR(500),
            file_size_bytes INTEGER,
            file_operation VARCHAR(20),
            error_code VARCHAR(50),
            error_type VARCHAR(100),
            stack_trace TEXT,
            retry_count INTEGER DEFAULT 0,
            extra_data JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_level ON {table_name}(level);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_correlation_id ON {table_name}(correlation_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_record_id ON {table_name}(record_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_uri ON {table_name}(uri);
        """
        
        try:
            async with self.db_client.acquire() as conn:
                await conn.execute(create_table_sql)
                logger.info(f"Таблица {table_name} проверена/создана")
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы логов: {e}")
    
    async def log(self, **kwargs) -> None:
        """
        Асинхронная запись лога.
        
        Пример использования:
        await log_service.log(
            level="INFO",
            logger="app.services.processor",
            message="Обработка запроса",
            correlation_id=correlation_id,
            record_id=record_id,
            total_processing_time_ms=1250
        )
        """
        if not self.config.enabled:
            return
        
        # Добавляем timestamp, если не указан
        if 'timestamp' not in kwargs:
            kwargs['timestamp'] = datetime.utcnow()
        
        # Добавляем в очередь
        self.log_queue.append(kwargs)
        
        # Если очередь достигла batch_size, немедленно записываем
        if len(self.log_queue) >= self.config.batch_size:
            await self._flush()
    
    async def _flush(self):
        """Запись всех накопленных логов в БД"""
        if not self.log_queue:
            return
        
        # Извлекаем все логи из очереди
        logs_to_insert = list(self.log_queue)
        self.log_queue.clear()
        
        if not logs_to_insert:
            return
        
        try:
            await self._insert_logs(logs_to_insert)
        except Exception as e:
            logger.error(f"Ошибка при записи логов в БД: {e}")
            # Возвращаем логи обратно в очередь
            self.log_queue.extendleft(reversed(logs_to_insert))
    
    async def _insert_logs(self, logs: List[Dict[str, Any]]):
        """Вставка логов в БД (пакетная)"""
        if not logs or not self.db_client:
            return
        
        table_name = self.config.table_name
        
        # Построение SQL-запроса для пакетной вставки
        columns = list(logs[0].keys())
        placeholders = ', '.join([f"${i+1}" for i in range(len(columns))])
        
        query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        """
        
        try:
            async with self.db_client.acquire() as conn:
                async with conn.transaction():
                    # Пакетная вставка
                    for log in logs:
                        values = [log.get(col) for col in columns]
                        await conn.execute(query, *values)
        except Exception as e:
            logger.error(f"Ошибка при пакетной вставке логов: {e}")
            raise
    
    async def _periodic_flush(self):
        """Фоновая задача для периодической записи логов"""
        while self.is_running:
            try:
                await asyncio.sleep(self.config.flush_interval_seconds)
                await self._flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в периодической записи логов: {e}")
    
    async def shutdown(self):
        """Остановка сервиса логирования"""
        self.is_running = False
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Сохраняем оставшиеся логи
        await self._flush()
        logger.info("Сервис логирования остановлен")

# Глобальный экземпляр сервиса
log_service = LoggingService()
