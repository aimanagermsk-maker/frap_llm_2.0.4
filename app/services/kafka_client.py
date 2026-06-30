import json
import logging
from typing import Any, Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self, bootstrap_servers: str, group_id: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        
    async def start(self):
        """Запускает продюсера и консьюмера"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        logger.info("Kafka producer started")
        
        if self.group_id:
            self.consumer = AIOKafkaConsumer(
                "frap-llm-helper-in",
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            await self.consumer.start()
            logger.info("Kafka consumer started")
    
    async def send_message(self, topic: str, value: dict, key: Optional[str] = None):
        """Отправляет сообщение в Kafka"""
        if not self.producer:
            raise RuntimeError("Kafka producer not started")
        await self.producer.send(topic, value=value, key=key.encode('utf-8') if key else None)
        logger.info(f"Message sent to topic {topic}")
    
    async def consume_messages(self):
        """Потребляет сообщения из топика"""
        if not self.consumer:
            raise RuntimeError("Kafka consumer not started")
        
        async for msg in self.consumer:
            logger.info(f"Received message: {msg.value}")
            yield msg.value
    
    async def stop(self):
        """Останавливает клиенты"""
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()
        logger.info("Kafka clients stopped")
