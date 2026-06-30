import json
import logging
from typing import Dict, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.services.processor import Processor
from app.config.app_config import get_config

logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.processor = Processor(config)
        self.consumer = None
        self.producer = None
        # ... остальной код инициализации ...

    async def start_consuming(self):
        """Запускает цикл потребления сообщений."""
        # ... (код получения consumer и producer) ...

        try:
            async for msg in self.consumer:
                logger.info(f"Received message from Kafka: {msg.topic}, offset: {msg.offset}")
                try:
                    # 1. Декодируем сообщение
                    raw_data = json.loads(msg.value.decode('utf-8'))
                    logger.debug(f"Raw message data: {raw_data}")

                    # 2. Обрабатываем через Processor
                    result = await self.processor.process_message(raw_data)

                    # 3. Отправляем результат
                    await self._send_result(result.dict(), msg.key)
                    logger.info(f"Message processed and sent. Original id: {raw_data.get('id')}")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON from message: {e}", exc_info=True)
                    # Отправляем ошибку в out-топик
                    await self._send_error_result(f"Invalid JSON: {str(e)}", msg.key)
                except Exception as e:
                    logger.error(f"Unexpected error processing message: {e}", exc_info=True)
                    await self._send_error_result(f"Processing error: {str(e)}", msg.key)

        except Exception as e:
            logger.critical(f"Consumer loop crashed: {e}", exc_info=True)
            # Здесь можно реализовать перезапуск consumer

    async def _send_result(self, result_data: Dict[str, Any], key: bytes = None):
        """Отправляет результат в out-топик."""
        # ... (код отправки) ...
        pass

    async def _send_error_result(self, error_message: str, key: bytes = None):
        """Отправляет сообщение об ошибке в out-топик."""
        # ... (код отправки) ...
        pass
