# app/models/config_models.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
from datetime import date
from uuid import UUID


from pydantic import BaseModel
from typing import List, Optional
from enum import Enum
from datetime import date


class IncomingKafkaMessage(BaseModel):
    id: str
    type: str  # например, "frapclaims"
    date: date # формат "2024-10-17"
    uri: str   # имя файла, например "a005e224-f0a5-4092-82c6-002adc0f4d95"


class VerificationStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"

class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

class FieldVerificationItem(BaseModel):
    xml_field: str
    xml_value: Optional[str]
    etiketka_value: Optional[str]
    match: bool
    normative_reference: str
    severity: Severity

class MandatoryElementCheck(BaseModel):
    element: str
    present: bool
    normative_requirement: str
    found_text: Optional[str]

class VerificationResult(BaseModel):
    product_type: str
    etiketka_ocr: dict
    field_verification: List[FieldVerificationItem]
    mandatory_elements_check: List[MandatoryElementCheck]
    discrepancies_summary: dict
    verdict: dict
    normative_references_applied: List[dict]


class IncomingMessage(BaseModel):
    id: str
    type: str
    date: date  # èëè str, íî ëó÷øå èñïîëüçîâàòü date
    uri: str

class PostgresConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str
    table_name: str

class KafkaConfig(BaseModel):
    bootstrap_servers: str
    consumer_group_id: str
    input_topic: str
    output_topic: str

class FileStorageConfig(BaseModel):
    output_folder: str
    file_prefix: Optional[str] = "result_"
    cleanup_after_send: Optional[bool] = False

class LoggingConfig(BaseModel):
    """ÐšÐ¾Ð½Ñ„Ð¸Ð³ÑƒÑ€Ð°Ñ†Ð¸Ñ Ð»Ð¾Ð³Ð¸Ñ€Ð¾Ð²Ð°Ð½Ð¸Ñ Ð² Ð‘Ð”"""
    enabled: bool = True
    table_name: str = "service_logs"
    batch_size: int = 10
    flush_interval_seconds: int = 5
    async_mode: bool = True
    log_levels: list[str] = ["ERROR", "WARNING", "INFO", "DEBUG"]

class AppConfig(BaseModel):
    postgres: PostgresConfig
    kafka: KafkaConfig
    file_storage: FileStorageConfig
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

class ExtractionOutputConfig(BaseModel):
    json_file_name: str = "extracted_data.json"
    pdf_directory: str = "pdf_files"
    label_directory: str = "label_images"

class ExtractionRulesConfig(BaseModel):
    tags_to_extract: List[str] = []
    output: ExtractionOutputConfig = ExtractionOutputConfig()

class FileStorageConfig(BaseModel):
    base_output_dir: str = "./output"

# ... существующие модели ...
