"""Helpers shared by the HTTP API and the MCP server."""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List


def columns_to_rows(columns: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Turn ``[{name, values}]`` column samples into row dicts for ``scan_data``."""
    columns = list(columns)
    names = [str(c["name"]) for c in columns]
    if len(set(names)) != len(names):
        raise ValueError("Column names must be unique")
    depth = max((len(c.get("values") or []) for c in columns), default=0)
    rows: List[Dict[str, Any]] = []
    for index in range(depth):
        row = {}
        for column in columns:
            values = column.get("values") or []
            if index < len(values):
                row[str(column["name"])] = values[index]
        rows.append(row)
    return rows


def compact_match(match: Dict[str, Any]) -> Dict[str, Any]:
    reg = match.get("registry") or {}
    result = {
        "datatype": match.get("datatype") or match.get("dataclass"),
        "rule_key": match.get("rule_key") or match.get("dataclass"),
        "name": reg.get("name"),
        "confidence": round(float(match.get("confidence", 0)), 2),
        "is_pii": bool(match.get("is_pii")),
        "pii_source": match.get("pii_source"),
        "status": reg.get("status"),
        "url": match.get("classurl"),
        "ruleid": match.get("ruleid"),
        "ruletype": match.get("ruletype"),
    }
    if match.get("pattern"):
        result["pattern"] = match["pattern"]
    if match.get("replaced_by"):
        result["replaced_by"] = match["replaced_by"]
    return {k: v for k, v in result.items() if v is not None}


def compact_source(report: Dict[str, Any]) -> Dict[str, Any]:
    """Field labels of one report without statistics or scanned values."""
    return {
        "source": report.get("source"),
        "complete": report.get("complete", True),
        "errors": report.get("errors", []),
        "fields": [
            {
                "field": field.get("field"),
                "ftype": field.get("ftype"),
                "is_pii": bool((field.get("pii") or {}).get("is_pii")),
                "matches": [compact_match(m) for m in field.get("matches", [])],
            }
            for field in report.get("data") or []
        ],
    }


def compact_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """Agent-friendly summary of a single or aggregate report."""
    first = report["sources"][0] if report.get("sources") else report
    summary = {
        "report_version": report.get("report_version"),
        "registry": first.get("registry") if isinstance(first, dict) else None,
        "scoring_model": first.get("scoring_model") if isinstance(first, dict) else None,
    }
    if isinstance(report.get("sources"), list):
        summary["complete"] = report.get("complete", True)
        summary["errors"] = report.get("errors", [])
        summary["sources"] = [compact_source(s) for s in report["sources"]]
    else:
        summary.update(compact_source(report))
    return summary


def json_safe(payload: Any) -> Any:
    return json.loads(json.dumps(payload, default=str, allow_nan=False))
