#!/usr/bin/env python3
"""
Простой продюсер для отправки тестовых сообщений в Kafka.
Использование: python produce_test_message.py
"""

import json
import asyncio
from aiokafka import AIOKafkaProducer
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = 'gitlab-ci.ru:9092'
TOPIC = 'frap-llm-helper-in'

# Тестовое сообщение
TEST_MESSAGE = {
    "id": "3",
    "type": "frapclaims",
    "date": "2024-10-17",
    "uri": "010052079762-18fa25b3-4c15-42a0-ab2e-4b468ec26b2e"
}

async def produce_message():
    """Отправка тестового сообщения в Kafka"""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        # Запуск продюсера
        await producer.start()
        logger.info(f"Подключено к Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        
        # Отправка сообщения
        await producer.send(
            topic=TOPIC,
            value=TEST_MESSAGE
        )
        
        logger.info(f"✅ Сообщение успешно отправлено в топик '{TOPIC}'")
        logger.info(f"📝 Содержимое: {json.dumps(TEST_MESSAGE, indent=2, ensure_ascii=False)}")
        
        # Дожидаемся подтверждения отправки
        await producer.flush()
        logger.info("✅ Сообщение доставлено на брокер")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке сообщения: {e}")
        raise
    finally:
        # Закрытие продюсера
        await producer.stop()
        logger.info("Продюсер остановлен")

async def produce_multiple_messages(count: int = 5):
    """Отправка нескольких тестовых сообщений"""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        await producer.start()
        logger.info(f"Подключено к Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        
        for i in range(count):
            message = {
                "id": str(10 + i),
                "type": "frapclaims",
                "date": "2024-10-17",
                "uri": f"a005e224-f0a5-4092-82c6-{i:012d}"
            }
            
            await producer.send(topic=TOPIC, value=message)
            logger.info(f"📤 Отправлено сообщение {i+1}/{count}: id={message['id']}")
        
        await producer.flush()
        logger.info(f"✅ Все {count} сообщений доставлены на брокер")
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise
    finally:
        await producer.stop()

if __name__ == '__main__':
    # Отправка одного сообщения
    asyncio.run(produce_message())
    
    # Или отправка нескольких сообщений (раскомментировать для использования)
    # asyncio.run(produce_multiple_messages(5))
