import xml.etree.ElementTree as ET
import base64
import json
import yaml
import os
from pathlib import Path
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class XMLDataExtractor:
    """Класс для извлечения данных из XML по заданным правилам."""

    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация экстрактора.

        Args:
            config: Словарь с конфигурацией из app_config.
                   Должен содержать разделы 'extraction_rules' и 'file_storage'.
        """
        self.config = config
        self.namespaces = {
            'rpp': 'http://fsrar.ru/WEGAIS/FrapClaims',
            'ns': 'http://fsrar.ru/WEGAIS/WB_DOC_SINGLE_01'
        }
        self.tags_to_extract = config.get('extraction_rules', {}).get('tags_to_extract', [])
        self.base_output_dir = Path(config.get('file_storage', {}).get('base_output_dir', './output'))

        # Пути для сохранения файлов
        self.json_output_path = self.base_output_dir / config.get('extraction_rules', {}).get('output', {}).get('json_file_name', 'extracted_data.json')
        self.pdf_output_dir = self.base_output_dir / config.get('extraction_rules', {}).get('output', {}).get('pdf_directory', 'pdf_files')
        self.label_output_dir = self.base_output_dir / config.get('extraction_rules', {}).get('output', {}).get('label_directory', 'label_images')

        # Создаем директории
        self._create_directories()

    def _create_directories(self) -> None:
        """Создает необходимые директории для сохранения файлов."""
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_output_dir.mkdir(parents=True, exist_ok=True)
        self.label_output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Директории для сохранения созданы: {self.base_output_dir}")

    def process_xml(self, xml_content: str) -> Dict[str, Any]:
        """
        Основной метод обработки XML содержимого.

        Args:
            xml_content: Содержимое XML файла в виде строки.

        Returns:
            Словарь с результатами обработки.
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.error(f"Ошибка парсинга XML: {e}")
            return {"error": f"Invalid XML: {e}"}

        # 1. Извлечение данных по тегам
        extracted_data = self._extract_tag_values(root)

        # 2. Сохранение JSON
        json_path = self._save_json(extracted_data)

        # 3. Извлечение и сохранение PDF/TDElectronicView
        pdf_paths = self._save_encoded_files(root, './/rpp:TDElectronicView', self.pdf_output_dir, 'electronic_view')

        # 4. Извлечение и сохранение LabelFoto
        label_paths = self._save_encoded_files(root, './/rpp:LabelFoto', self.label_output_dir, 'label_foto')

        # Формируем результат
        result = {
            "extracted_data": extracted_data,
            "json_file_path": str(json_path),
            "pdf_files": [str(p) for p in pdf_paths],
            "label_files": [str(p) for p in label_paths],
        }
        logger.info(f"Обработка XML завершена. Сохранено {len(pdf_paths)} PDF и {len(label_paths)} LabelFoto.")
        return result

    def _extract_tag_values(self, root: ET.Element) -> Dict[str, str]:
        """Извлекает значения тегов из корневого элемента XML."""
        data = {}
        for tag in self.tags_to_extract:
            tag_parts = tag.split(':', 1)
            if len(tag_parts) == 2:
                prefix, local_tag = tag_parts[0], tag_parts[1]
                element = root.find(f'.//{prefix}:{local_tag}', self.namespaces)
            else:
                element = root.find(f'.//{tag}', self.namespaces)

            data[tag] = element.text if element is not None and element.text else ""
        return data

    def _save_json(self, data: Dict[str, str]) -> Path:
        """Сохраняет извлеченные данные в JSON файл."""
        try:
            with open(self.json_output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info(f"JSON файл сохранен: {self.json_output_path}")
        except Exception as e:
            logger.error(f"Ошибка сохранения JSON: {e}")
        return self.json_output_path

    def _save_encoded_files(self, root: ET.Element, xpath: str, output_dir: Path, file_prefix: str) -> List[Path]:
        """
        Извлекает закодированные данные по XPath, декодирует и сохраняет в файлы.

        Args:
            root: Корневой элемент XML.
            xpath: XPath для поиска элементов.
            output_dir: Директория для сохранения.
            file_prefix: Префикс для имени файла.

        Returns:
            Список путей к сохраненным файлам.
        """
        saved_files = []
        elements = root.findall(xpath, self.namespaces)
        for idx, element in enumerate(elements):
            encoded_data = element.text
            if not encoded_data:
                continue
            try:
                file_bytes = base64.b64decode(encoded_data)
                file_path = output_dir / f"{file_prefix}_{idx + 1}.pdf"
                with open(file_path, 'wb') as f:
                    f.write(file_bytes)
                saved_files.append(file_path)
                logger.debug(f"Файл сохранен: {file_path}")
            except Exception as e:
                logger.error(f"Ошибка сохранения файла {file_prefix}_{idx + 1}: {e}")
        return saved_files
