from typing import List, Optional
from pydantic import BaseModel, Field
from enum import Enum

class OCRStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"

class MatchStatus(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

class Decision(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"
    NEEDS_CORRECTION = "needs_correction"

class FieldVerificationItem(BaseModel):
    xml_field: str
    xml_value: Optional[str] = None
    etiketka_value: Optional[str] = None
    match: bool
    normative_reference: str
    severity: MatchStatus

class MandatoryElementItem(BaseModel):
    element: str
    present: bool
    normative_requirement: str
    found_text: Optional[str] = None

class DiscrepanciesSummary(BaseModel):
    errors_count: int = 0
    warnings_count: int = 0
    blocking_issues: List[str] = Field(default_factory=list)

class Verdict(BaseModel):
    etiketka_compliant: bool
    decision: Decision
    recommendations: List[str] = Field(default_factory=list)

class NormativeReference(BaseModel):
    act: str
    clause: str
    applied_to: List[str] = Field(default_factory=list)

class VerificationResult(BaseModel):
    product_type: str = "sparkling_wine_import"
    etiketka_ocr: dict = Field(..., description="OCR status and extracted text")
    field_verification: List[FieldVerificationItem] = Field(default_factory=list)
    mandatory_elements_check: List[MandatoryElementItem] = Field(default_factory=list)
    discrepancies_summary: DiscrepanciesSummary = Field(default_factory=DiscrepanciesSummary)
    verdict: Verdict
    normative_references_applied: List[NormativeReference] = Field(default_factory=list)
