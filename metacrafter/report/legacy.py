"""Pre-1.0 report structure (``--output-format legacy``)."""
from __future__ import annotations

from typing import Any, Dict

LEGACY_MATCH_KEYS = ("ruleid", "dataclass", "confidence", "ruletype", "format", "classurl", "is_pii")
LEGACY_FIELD_KEYS = ("field", "matches", "tags", "ftype", "datatype_url", "stats")


def _legacy_single(report: Dict[str, Any]) -> Dict[str, Any]:
    data = []
    for field in report.get("data") or report.get("fields") or []:
        entry = {k: field.get(k) for k in LEGACY_FIELD_KEYS if k in field}
        entry["matches"] = [{k: m.get(k) for k in LEGACY_MATCH_KEYS} for m in field.get("matches", [])]
        data.append(entry)
    return {
        "results": report.get("results", []),
        "data": data,
        "stats": report.get("stats", {}),
        "stats_table": report.get("stats_table", []),
    }


def to_legacy(report: Any) -> Any:
    """Convert a version 3 report (or aggregate) to the pre-1.0 structure."""
    if isinstance(report, dict) and isinstance(report.get("sources"), list):
        return [
            dict(_legacy_single(source), table=source.get("table") or source.get("source"))
            for source in report["sources"]
        ]
    if isinstance(report, list):
        return [to_legacy(item) for item in report]
    return _legacy_single(report)
