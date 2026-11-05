"""Versioned scan interchange with legacy readers and named statistics."""

import hashlib
import json
import math
from typing import Any, Dict, List

REPORT_VERSION = 3  # see metacrafter.report.models
STATS_HEADERS = [
    "key",
    "ftype",
    "is_dictkey",
    "is_uniq",
    "n_uniq",
    "share_uniq",
    "minlen",
    "maxlen",
    "avglen",
    "tags",
    "has_digit",
    "has_alphas",
    "has_special",
    "minval",
    "maxval",
    "has_any_digit",
    "has_any_alphas",
    "has_any_special",
    "dictvalues",
]


def statistics_dict(rows):
    result = {}
    for row in rows or []:
        if len(row) != len(STATS_HEADERS):
            raise ValueError("Invalid statistics row: expected 19 values")
        result[row[0]] = dict(zip(STATS_HEADERS, row))
    return result


def validate_options(limit, confidence):
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not math.isfinite(confidence)
        or not 0 <= confidence <= 100
    ):
        raise ValueError("confidence must be finite and between 0 and 100")


def normalize_report(report):
    """Accept SDK data and saved fields aliases; reject incompatible payloads."""
    if not isinstance(report, dict):
        raise ValueError("A report must be an object")
    version = report.get("report_version", 1)
    if version not in (1, 2, REPORT_VERSION):
        raise ValueError("Unsupported report version: %s" % version)
    fields = report.get("data", report.get("fields"))
    if fields is None or not isinstance(fields, list):
        raise ValueError("Report requires a data or fields array")
    for field in fields:
        if not isinstance(field, dict) or not isinstance(field.get("field"), str):
            raise ValueError("Report fields require a string field path")
        for match in field.get("matches", []):
            if not isinstance(match, dict) or not match.get("dataclass"):
                raise ValueError("Match requires a dataclass")
            validate_options(1, match.get("confidence", 0))
    result = dict(report)
    result.update(report_version=REPORT_VERSION, data=fields, fields=fields)
    result.setdefault("source", report.get("table", "memory"))
    result.setdefault("complete", True)
    result.setdefault("errors", [])
    return result


def report_sources(payload):
    if isinstance(payload, list):
        return [normalize_report(r) for r in payload]
    if isinstance(payload, dict) and "sources" in payload:
        sources = [normalize_report(r) for r in payload["sources"]]
        for source in sources:
            source["complete"] = source["complete"] and payload.get("complete", True)
            source["errors"] = source["errors"] + payload.get("errors", [])
        return sources
    return [normalize_report(payload)]


def aggregate_reports(reports, errors=None):
    errors = errors or []
    return {
        "report_version": REPORT_VERSION,
        "sources": [normalize_report(r) for r in reports],
        "complete": not errors and all(r.get("complete", True) for r in reports),
        "errors": errors,
    }


def rules_fingerprint(processor):
    rules = processor.field_rules + processor.data_rules
    payload = [
        {
            k: v
            for k, v in rule.items()
            if k in ("id", "key", "rule", "validator", "priority", "is_pii")
        }
        for rule in rules
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()
