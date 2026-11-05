"""Scan report version 3: typed models, schema, legacy and Markdown renderings."""

from metacrafter.report.build import finalize_report, redact_samples
from metacrafter.report.legacy import to_legacy
from metacrafter.report.markdown import render_markdown
from metacrafter.report.models import (
    REPORT_VERSION,
    AggregateReport,
    FieldReport,
    Match,
    RegistryRef,
    ScanReport,
    report_json_schema,
    validate_report,
)

__all__ = [
    "REPORT_VERSION",
    "AggregateReport",
    "FieldReport",
    "Match",
    "RegistryRef",
    "ScanReport",
    "finalize_report",
    "redact_samples",
    "render_markdown",
    "report_json_schema",
    "to_legacy",
    "validate_report",
]
