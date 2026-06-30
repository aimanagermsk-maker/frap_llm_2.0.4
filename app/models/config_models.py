# app/models/config_models.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
from datetime import date
from uuid import UUID

class IncomingMessage(BaseModel):
    id: str
    type: str
    date: date  # или str, но лучше использовать date
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
    """РљРѕРЅС„РёРіСѓСЂР°С†РёСЏ Р»РѕРіРёСЂРѕРІР°РЅРёСЏ РІ Р‘Р”"""
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
