# app/services/processor.py
import os
import json
import logging
from app.services.xml_extractor import XMLDataExtractor
from typing import Dict, Any
from datetime import date
from pathlib import Path
from app.services.kafka_client import KafkaClient
from app.models.config_models import IncomingKafkaMessage
from app.models.response_models import (
    OutgoingKafkaMessage,
    EtiketkaOcr,
    StatusEnum,
    # ... и все остальные модели
)
from app.config.app_config import get_config # Предположим, функция получения конфига

logger = logging.getLogger(__name__)

class DataProcessor:
    """Основной класс для обработки входящих данных."""

    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация процессора.

        Args:
            config: Глобальная конфигурация приложения.
        """
        self.config = config
        self.xml_extractor = XMLDataExtractor(config)
        logger.info("DataProcessor инициализирован.")

    async def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обрабатывает входящее сообщение (например, из Kafka).

        Args:
            message: Словарь с данными сообщения. Ожидается, что в нем есть ключ 'xml_content'.

        Returns:
            Словарь с результатом обработки.
        """
        xml_content = message.get('xml_content')
        if not xml_content:
            logger.warning("Сообщение не содержит 'xml_content'.")
            return {"error": "No XML content provided"}

        logger.info("Начата обработка XML сообщения.")
        result = self.xml_extractor.process_xml(xml_content)

        # Здесь может быть дополнительная логика, например, сохранение в БД
        # await self.db_client.save_extraction_result(result)

        logger.info("Обработка сообщения завершена.")
        return result

class Processor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Получаем базовый путь из конфига
        self.base_path = Path(self.config.get("file_storage", {}).get("base_path", "./data"))
        logger.info(f"Processor initialized. File base path: {self.base_path}")

    async def process_message(self, raw_message: Dict[str, Any]) -> OutgoingKafkaMessage:
        """Основной метод обработки входящего сообщения из Kafka."""
        logger.info(f"Processing message with id: {raw_message.get('id')}")

        try:
            # 1. Валидируем и парсим входящее сообщение
            incoming = IncomingKafkaMessage(**raw_message)
            logger.debug(f"Parsed incoming message: {incoming.json()}")

            # 2. Строим путь к файлу и читаем его
            file_content = await self._read_file_from_message(incoming)
            logger.info(f"File read successfully. URI: {incoming.uri}, Size: {len(file_content)} chars")

            # 3. Вызываем LLM (заглушка) с содержимым файла
            llm_result = await self._call_llm(file_content)
            logger.info(f"LLM processing completed for URI: {incoming.uri}")

            # 4. Формируем исходящее сообщение
            # На данный момент все поля - заглушки, кроме тех, что мы можем заполнить из llm_result
            verification_result = self._generate_test_result(input_message)
            logger.info(f"Outgoing message built for id: {raw_message.get('id')}")

            await self.kafka_client.send_message(
                topic="frap-llm-helper-out",
                value=verification_result.dict()
            )
            
            logger.info("Verification result sent to Kafka")
            return verification_result.dict()

        except Exception as e:
            logger.error(f"Error processing message {raw_message.get('id')}: {str(e)}", exc_info=True)
            # В случае ошибки возвращаем сообщение с FAILED статусом
            return self._build_error_outgoing_message(str(e))

    async def _read_file_from_message(self, incoming: IncomingKafkaMessage) -> str:
        """Строит путь и читает файл."""
        # Формируем путь: [Path]/[date]/[type]/[uri]
        # Преобразуем date в строку формата YYYY-MM-DD
        date_str = incoming.date.isoformat()
        file_path = self.base_path / date_str / incoming.type / incoming.uri

        logger.debug(f"Attempting to read file from: {file_path}")

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            return content
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    async def _call_llm(self, content: str) -> Dict[str, Any]:
        """
        Заглушка для вызова LLM.
        В реальности здесь был бы асинхронный HTTP-запрос к модели.
        """
        logger.info("Calling LLM (stub)...")
        # Имитируем асинхронную работу
        # В реальном коде: return await self.llm_client.analyze(content)

        # Пока возвращаем пустой результат, который потом заполним заглушками
        # В будущем сюда придет реальный ответ от LLM
        return {
            "etiketka_ocr": {"status": "success", "extracted_text_sample": content[:100]},
            "field_verification": [],
            "mandatory_elements_check": [],
            "discrepancies_summary": {"errors_count": 0, "warnings_count": 0, "blocking_issues": []},
            "verdict": {"etiketka_compliant": True, "decision": "accept", "recommendations": []},
            "normative_references_applied": []
        }

    # def _build_outgoing_message(self, llm_result: Dict[str, Any]) -> OutgoingKafkaMessage:
    #     """Строит исходящее сообщение, заполняя обязательные поля."""
    #     # Здесь мы должны смаппить llm_result в нашу строгую структуру.
    #     # Пока заполняем все поля явно, используя заглушки.
    #     logger.debug("Building outgoing message with stub data")

    #     # Пример заполнения. В будущем это будет основано на реальном ответе LLM.
    #     return OutgoingKafkaMessage(
    #         product_type="sparkling_wine_import",
    #         etiketka_ocr=EtiketkaOcr(
    #             status=StatusEnum.SUCCESS, # или llm_result.get("ocr_status", "success")
    #             extracted_text_sample=llm_result.get("etiketka_ocr", {}).get("extracted_text_sample", "Sample OCR text")
    #         ),
    #         field_verification=[
    #             # Заглушка: создаем один элемент
    #             FieldVerificationItem(
    #                 xml_field="SampleField",
    #                 xml_value="Sample XML Value",
    #                 etiketka_value="Sample Etiketka Value",
    #                 match=False,
    #                 normative_reference="Sample Normative Ref",
    #                 severity=SeverityEnum.INFO
    #             )
    #         ],
    #         mandatory_elements_check=[
    #             MandatoryElementsCheckItem(
    #                 element="Sample Element",
    #                 present=True,
    #                 normative_requirement="Must be present",
    #                 found_text="Present"
    #             )
    #         ],
    #         discrepancies_summary=DiscrepanciesSummary(
    #             errors_count=0,
    #             warnings_count=1,
    #             blocking_issues=["No blocking issues"]
    #         ),
    #         verdict=Verdict(
    #             etiketka_compliant=True,
    #             decision="accept",
    #             recommendations=["Check sample field"]
    #         ),
    #         normative_references_applied=[
    #             NormativeReferenceApplied(
    #                 act="Sample Act",
    #                 clause="1.1",
    #                 applied_to=["SampleField"]
    #             )
    #         ]
    #     )

    # def _build_error_outgoing_message(self, error_message: str) -> OutgoingKafkaMessage:
    #     """Формирует исходящее сообщение об ошибке с FAILED статусом."""
    #     logger.warning(f"Building error outgoing message: {error_message}")
    #     return OutgoingKafkaMessage(
    #         product_type="sparkling_wine_import",
    #         etiketka_ocr=EtiketkaOcr(
    #             status=StatusEnum.FAILED,
    #             extracted_text_sample=f"Error: {error_message[:100]}"
    #         ),
    #         field_verification=[],
    #         mandatory_elements_check=[],
    #         discrepancies_summary=DiscrepanciesSummary(
    #             errors_count=1,
    #             warnings_count=0,
    #             blocking_issues=[error_message[:100]]
    #         ),
    #         verdict=Verdict(
    #             etiketka_compliant=False,
    #             decision="reject",
    #             recommendations=["Check input data and file availability"]
    #         ),
    #         normative_references_applied=[]
    #     )
        
    def _generate_test_result(self, input_data: dict) -> VerificationResult:
        """
        Генерирует тестовый результат верификации
        """
        # Тестовые данные для поля field_verification
        field_verification = [
            FieldVerificationItem(
                xml_field="product_name",
                xml_value="Sparkling Wine Import",
                etiketka_value="Sparkling Wine",
                match=False,
                normative_reference="ГОСТ Р 51157-98 п. 4.1.2",
                severity=MatchStatus.ERROR
            ),
            FieldVerificationItem(
                xml_field="alcohol_content",
                xml_value="12.5%",
                etiketka_value="12.5%",
                match=True,
                normative_reference="ГОСТ Р 51157-98 п. 4.3.1",
                severity=MatchStatus.INFO
            ),
            FieldVerificationItem(
                xml_field="volume",
                xml_value="750 ml",
                etiketka_value="750 ml",
                match=True,
                normative_reference="ГОСТ Р 51157-98 п. 4.2.3",
                severity=MatchStatus.INFO
            )
        ]
        
        # Тестовые данные для обязательных элементов
        mandatory_elements = [
            MandatoryElementItem(
                element="Название производителя",
                present=True,
                normative_requirement="ГОСТ Р 51157-98 п. 5.1",
                found_text="ООО 'Винный Дом'"
            ),
            MandatoryElementItem(
                element="Дата розлива",
                present=False,
                normative_requirement="ГОСТ Р 51157-98 п. 5.2",
                found_text=None
            ),
            MandatoryElementItem(
                element="Состав",
                present=True,
                normative_requirement="ГОСТ Р 51157-98 п. 5.3",
                found_text="Виноград, сахар, антиоксидант"
            )
        ]
        
        # Сводка по расхождениям
        discrepancies = DiscrepanciesSummary(
            errors_count=1,
            warnings_count=0,
            blocking_issues=["Несоответствие названия продукта"]
        )
        
        # Вердикт
        verdict = Verdict(
            etiketka_compliant=False,
            decision=Decision.REJECT,
            recommendations=[
                "Исправить название продукта согласно XML данным",
                "Добавить дату розлива"
            ]
        )
        
        # Нормативные ссылки
        normative_refs = [
            NormativeReference(
                act="ГОСТ Р 51157-98",
                clause="п. 4.1.2",
                applied_to=["product_name"]
            ),
            NormativeReference(
                act="ГОСТ Р 51157-98",
                clause="п. 5.1-5.3",
                applied_to=["Название производителя", "Дата розлива", "Состав"]
            )
        ]
        
        # Формируем полный результат
        return VerificationResult(
            product_type="sparkling_wine_import",
            etiketka_ocr={
                "status": OCRStatus.PARTIAL.value,
                "extracted_text_sample": "Sparkling Wine 12.5% 750 ml ООО Винный Дом"
            },
            field_verification=field_verification,
            mandatory_elements_check=mandatory_elements,
            discrepancies_summary=discrepancies,
            verdict=verdict,
            normative_references_applied=normative_refs
        )
    
    def _generate_error_result(self, error_message: str) -> VerificationResult:
        """
        Генерирует результат с ошибкой
        """
        return VerificationResult(
            product_type="sparkling_wine_import",
            etiketka_ocr={
                "status": OCRStatus.FAILED.value,
                "extracted_text_sample": ""
            },
            field_verification=[],
            mandatory_elements_check=[],
            discrepancies_summary=DiscrepanciesSummary(
                errors_count=1,
                warnings_count=0,
                blocking_issues=[f"Ошибка обработки: {error_message}"]
            ),
            verdict=Verdict(
                etiketka_compliant=False,
                decision=Decision.REJECT,
                recommendations=["Проверить корректность входных данных"]
            ),
            normative_references_applied=[]
        )
