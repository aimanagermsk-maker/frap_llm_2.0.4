# app/config/app_config.py
import os
import yaml
from pathlib import Path
from typing import Any, Dict
from app.models.config_models import AppConfig

_config: Optional[AppConfig] = None

def load_config() -> Dict[str, Any]:
    """Загрузка и слияние YAML-конфигураций"""
    settings_dir = Path(__file__).parent.parent.parent / "settings"
    
    # Загрузка базовой конфигурации
    base_config_path = settings_dir / "application.yaml"
    with open(base_config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Загрузка профильной конфигурации
    profile = os.getenv("PYTHON_PROFILES_ACTIVE", "sandbox")
    profile_config_path = settings_dir / f"application-{profile}.yaml"
    
    if profile_config_path.exists():
        with open(profile_config_path, 'r') as f:
            profile_config = yaml.safe_load(f)
            # Рекурсивное слияние словарей
            merge_dicts(config, profile_config)
    
    # Переопределение через переменные окружения (можно реализовать)
    # config = override_from_env(config)
    
    return config

def get_config() -> AppConfig:
    """Возвращает объект конфигурации"""
    global _config
    if _config is None:
        raw_config = load_config()
        _config = AppConfig(**raw_config)
    return _config

def merge_dicts(base: Dict, override: Dict) -> Dict:
    """Рекурсивное слияние словарей"""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            merge_dicts(base[key], value)
        else:
            base[key] = value
    return base

def log_app_config(config: AppConfig):
    """Логирование конфигурации (без sensitive-данных)"""
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Active profile: {os.getenv('PYTHON_PROFILES_ACTIVE', 'sandbox')}")
    logger.info(f"PostgreSQL table: {config.postgres.table_name}")
    logger.info(f"Kafka input topic: {config.kafka.input_topic}")
    logger.info(f"Output folder: {config.file_storage.output_folder}")