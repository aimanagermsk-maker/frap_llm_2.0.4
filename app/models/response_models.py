from pydantic import BaseModel
from typing import List, Optional
from enum import Enum

# Для полей со строгим набором значений используем Enum
class StatusEnum(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"

class SeverityEnum(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

class MatchEnum(str, Enum):
    TRUE = "true"
    FALSE = "false"

class EtiketkaOcr(BaseModel):
    status: StatusEnum
    extracted_text_sample: str  # В реальности сюда пойдет текст от OCR/LLM

class FieldVerificationItem(BaseModel):
    xml_field: str
    xml_value: Optional[str] = None
    etiketka_value: Optional[str] = None
    match: bool = False  # Проще оставить bool, чем Enum
    normative_reference: str
    severity: SeverityEnum

class MandatoryElementsCheckItem(BaseModel):
    element: str
    present: bool = False
    normative_requirement: str
    found_text: Optional[str] = None

class DiscrepanciesSummary(BaseModel):
    errors_count: int = 0
    warnings_count: int = 0
    blocking_issues: List[str] = []

class Verdict(BaseModel):
    etiketka_compliant: bool = False
    decision: str  # "accept", "reject", "needs_correction"
    recommendations: List[str] = []

class NormativeReferenceApplied(BaseModel):
    act: str
    clause: str
    applied_to: List[str] = []

class OutgoingKafkaMessage(BaseModel):
    product_type: str = "sparkling_wine_import"  # Заглушка
    etiketka_ocr: EtiketkaOcr
    field_verification: List[FieldVerificationItem]
    mandatory_elements_check: List[MandatoryElementsCheckItem]
    discrepancies_summary: DiscrepanciesSummary
    verdict: Verdict
    normative_references_applied: List[NormativeReferenceApplied]
