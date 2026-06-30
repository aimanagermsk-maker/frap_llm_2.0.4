# app/services/processor.py
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from app.models.config_models import IncomingMessage
from app.services.file_reader import FileReader
from app.services.llm_client import LLMClient  # Предположим, что есть клиент для LLM
from app.services.kafka_client import KafkaClient

logger = logging.getLogger(__name__)

class Processor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.file_reader = FileReader(config['file_storage']['path'])
        self.llm_client = LLMClient(config.get('llm', {}))  # Заглушка
        self.kafka_client = KafkaClient(config.get('kafka', {}))

    async def process_message(self, raw_message: str):
        try:
            # 1. Парсим входящее сообщение
            message_data = json.loads(raw_message)
            incoming = IncomingMessage(**message_data)
            logger.info(f"Received message: {incoming.id}")

            # 2. Читаем файл
            file_content = await self.file_reader.read_file(
                date=str(incoming.date),
                file_type=incoming.type,
                uri=incoming.uri
            )

            if not file_content:
                # Если файл не найден, отправляем ошибку
                await self._send_error_response(incoming.id, "File not found")
                return

            # 3. Отправляем содержимое в LLM и получаем структурированный ответ
            llm_response = await self.llm_client.analyze(file_content, incoming.type)

            # 4. Формируем выходное сообщение по новой структуре
            output_message = self._build_output_response(llm_response)

            # 5. Отправляем результат в выходной топик
            await self.kafka_client.send_message(
                topic="frap-llm-helper-out",
                key=incoming.id,
                value=json.dumps(output_message)
            )
            logger.info(f"Processed message {incoming.id} successfully")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Можно отправить сообщение об ошибке в отдельный топик
            await self._send_error_response(
                getattr(incoming, 'id', 'unknown'),
                str(e)
            )

    def _build_output_response(self, llm_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует ответ от LLM в требуемый формат JSON.
        """
        # Это заглушка. В реальности llm_data будет содержать все поля.
        # Можно добавить валидацию через Pydantic.
        return {
            "product_type": llm_data.get("product_type", "unknown"),
            "etiketka_ocr": {
                "status": llm_data.get("ocr_status", "success"),
                "extracted_text_sample": llm_data.get("extracted_text", "")
            },
            "field_verification": llm_data.get("field_verification", []),
            "mandatory_elements_check": llm_data.get("mandatory_elements_check", []),
            "discrepancies_summary": {
                "errors_count": llm_data.get("errors_count", 0),
                "warnings_count": llm_data.get("warnings_count", 0),
                "blocking_issues": llm_data.get("blocking_issues", [])
            },
            "verdict": {
                "etiketka_compliant": llm_data.get("compliant", False),
                "decision": llm_data.get("decision", "reject"),
                "recommendations": llm_data.get("recommendations", [])
            },
            "normative_references_applied": llm_data.get("normative_references", [])
        }

    async def _send_error_response(self, message_id: str, error_message: str):
        """Отправка сообщения об ошибке"""
        error_response = {
            "id": message_id,
            "status": "error",
            "error": error_message,
            "timestamp": datetime.utcnow().isoformat()
        }
        await self.kafka_client.send_message(
            topic="frap-llm-helper-errors",  # Можно создать отдельный топик для ошибок
            key=message_id,
            value=json.dumps(error_response)
        )