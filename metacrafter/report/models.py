"""Pydantic models of scan report version 3.

Version 3 is a superset of the version 2 interchange format (``data``/``fields``,
``source``, ``complete``, ``errors``, ``sampling``) and adds registry provenance,
the scoring model, per-field PII summaries and typed, registry-enriched matches.
Unknown keys are allowed so that newer producers do not break older readers.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

REPORT_VERSION = 3


class _Open(BaseModel):
    model_config = ConfigDict(extra="allow")


class RegistryRef(_Open):
    """Registry metadata of a matched datatype."""

    id: Optional[str] = None
    name: Optional[str] = None
    url: Optional[str] = None
    status: Optional[str] = None
    is_pii: Optional[bool] = None
    classification: Optional[str] = None
    kind: Optional[str] = None
    categories: List[str] = Field(default_factory=list)
    country: List[str] = Field(default_factory=list)
    mappings: Dict[str, Union[str, List[str]]] = Field(default_factory=dict)
    replaced_by: Optional[str] = None


class Evidence(_Open):
    """How a confidence value was reached (see scoring model v1)."""

    scoring_model: Optional[str] = None
    hit_rate: Optional[float] = None
    checksum_rate: Optional[float] = None
    specificity: Optional[float] = None
    name_score: Optional[float] = None
    matched_count: Optional[int] = None
    considered_count: Optional[int] = None
    empty_count: Optional[int] = None


class Match(_Open):
    """One datatype detected on a field."""

    ruleid: str
    dataclass: str = Field(description="Rule key as written in the rule (kept for older consumers)")
    datatype: Optional[str] = Field(default=None, description="Canonical registry id, null when unknown")
    rule_key: Optional[str] = None
    confidence: float = Field(ge=0, le=100)
    ruletype: str
    format: Optional[str] = None
    pattern: Optional[str] = Field(default=None, description="Registry pattern record of a date format")
    classurl: Optional[str] = None
    is_pii: bool = False
    pii_source: Optional[Literal["rule", "registry", "context"]] = None
    replaced_by: Optional[str] = None
    evidence: Optional[Evidence] = None
    registry: Optional[RegistryRef] = None


class FieldPII(_Open):
    is_pii: bool = False
    datatypes: List[str] = Field(default_factory=list)


class FieldReport(_Open):
    field: str
    matches: List[Match] = Field(default_factory=list)
    ftype: Optional[str] = None
    tags: Any = None
    datatype_url: Optional[str] = None
    stats: Optional[Dict[str, Any]] = None
    pii: FieldPII = Field(default_factory=FieldPII)


class ToolInfo(_Open):
    name: str = "metacrafter"
    version: Optional[str] = None


class RegistryProvenance(_Open):
    source: str
    base_url: Optional[str] = None
    version: Optional[str] = None
    git_sha: Optional[str] = None
    generated_at: Optional[str] = None


class ScanReport(_Open):
    """Report of one scanned source (file, table, collection or in-memory data)."""

    report_version: Literal[3] = REPORT_VERSION
    tool: ToolInfo = Field(default_factory=ToolInfo)
    source: str = "memory"
    complete: bool = True
    errors: List[Any] = Field(default_factory=list)
    data: List[FieldReport] = Field(default_factory=list)
    registry: Optional[RegistryProvenance] = None
    scoring_model: Optional[str] = None
    rules_version: Optional[str] = None
    sampling: Optional[Dict[str, Any]] = None
    samples_included: Optional[bool] = None


class AggregateReport(_Open):
    """Report of several sources (SQL databases, MongoDB, bulk scans)."""

    report_version: Literal[3] = REPORT_VERSION
    sources: List[ScanReport]
    complete: bool = True
    errors: List[Any] = Field(default_factory=list)


def report_json_schema() -> Dict[str, Any]:
    """JSON Schema of report version 3 (a single report or an aggregate)."""
    from pydantic import TypeAdapter

    schema = TypeAdapter(Union[ScanReport, AggregateReport]).json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://apicrafter.github.io/metacrafter/schema/report-v3.schema.json"
    schema["title"] = "Metacrafter scan report (version 3)"
    return schema


def validate_report(payload: Dict[str, Any]) -> Union[ScanReport, AggregateReport]:
    """Validate a report dict and return the typed model (raises pydantic.ValidationError)."""
    if isinstance(payload, dict) and "sources" in payload:
        return AggregateReport.model_validate(payload)
    return ScanReport.model_validate(payload)
