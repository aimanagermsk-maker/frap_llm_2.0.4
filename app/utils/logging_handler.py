# app/utils/logging_handler.py
import logging
import asyncio
from typing import Dict, Any
from datetime import datetime

from app.services.logging_service import log_service

class DatabaseLogHandler(logging.Handler):
    """Хендлер для отправки логов в PostgreSQL через LoggingService"""
    
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
    
    def emit(self, record: logging.LogRecord):
        """Отправка лога в БД"""
        try:
            # Пропускаем логи самого сервиса логирования для избежания циклов
            if record.name == 'app.services.logging_service':
                return
            
            # Формируем данные для записи
            log_data = {
                'timestamp': datetime.fromtimestamp(record.created),
                'level': record.levelname,
                'logger': record.name,
                'module': record.module,
                'function': record.funcName,
                'line_number': record.lineno,
                'message': record.getMessage(),
                'stack_trace': record.exc_text if record.exc_text else None,
            }
            
            # Добавляем дополнительные поля, если они есть в record
            if hasattr(record, 'extra_fields'):
                log_data.update(record.extra_fields)
            
            # Асинхронная отправка в БД
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(log_service.log(**log_data))
            except RuntimeError:
                # Если цикл событий не запущен, используем синхронный метод
                # (для совместимости с синхронным кодом)
                asyncio.run(log_service.log(**log_data))
                
        except Exception as e:
            # Ошибки логирования не должны ломать основное приложение
            print(f"Ошибка в DatabaseLogHandler: {e}")

def setup_logging_with_db():
    """Настройка логирования с сохранением в БД"""
    root_logger = logging.getLogger()
    
    # Добавляем хендлер для БД
    db_handler = DatabaseLogHandler()
    root_logger.addHandler(db_handler)
    
    return root_logger
