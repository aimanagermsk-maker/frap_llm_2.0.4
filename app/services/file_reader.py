import os
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class FileReader:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    async def read_file(self, date: str, file_type: str, uri: str) -> Optional[str]:
        """
        Читает файл по пути: base_path/date/file_type/uri
        """
        try:
            # Формируем путь
            file_path = self.base_path / date / file_type / uri
            
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return None
            
            # Читаем файл (предполагаем, что это текстовый файл)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            logger.info(f"Successfully read file: {file_path}")
            return content
        except Exception as e:
            logger.error(f"Error reading file {uri}: {e}")
            return None