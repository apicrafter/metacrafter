"""Build report version 3 from scan results."""
from __future__ import annotations

import copy
from typing import Any, Dict

from metacrafter.report.models import REPORT_VERSION


def _tool_version() -> str:
    try:
        from importlib.metadata import version

        return version("metacrafter")
    except Exception:  # pragma: no cover - running from a source tree without metadata
        return "unknown"


def finalize_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """Tag a scan report as version 3: tool info and per-field PII summaries."""
    report["report_version"] = REPORT_VERSION
    report.setdefault("tool", {"name": "metacrafter", "version": _tool_version()})
    for field in report.get("data") or []:
        pii_types = sorted(
            {m.get("datatype") or m.get("dataclass") for m in field.get("matches", []) if m.get("is_pii")}
        )
        field["pii"] = {"is_pii": bool(pii_types), "datatypes": pii_types}
    if report.get("data") is not None and "field_groups" not in report:
        from metacrafter.report.groups import field_groups

        report["field_groups"] = field_groups(f.get("field") for f in report["data"] if isinstance(f, dict))
    return report


_SAMPLE_STAT_KEYS = ("dictvalues",)


def redact_samples(report: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy without raw values from the scanned data.

    Removes dictionary values and string minimum/maximum values from field statistics.
    Applies to single reports and aggregates.
    """
    report = copy.deepcopy(report)
    sources = report.get("sources") if isinstance(report.get("sources"), list) else [report]
    for source in sources:
        if not isinstance(source, dict):
            continue
        stats_maps = [source.get("stats") or {}]
        stats_maps += [f.get("stats") or {} for f in source.get("data") or [] if isinstance(f, dict)]
        for stats in stats_maps:
            entries = stats.values() if stats and all(isinstance(v, dict) for v in stats.values()) else [stats]
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                for key in _SAMPLE_STAT_KEYS:
                    if key in entry:
                        entry[key] = None
                for key in ("minval", "maxval"):
                    if isinstance(entry.get(key), str):
                        entry[key] = None
        table = source.get("stats_table")
        if isinstance(table, list):
            from metacrafter.reports import STATS_HEADERS

            redacted = []
            for row in table:
                row = list(row)
                for key in _SAMPLE_STAT_KEYS + ("minval", "maxval"):
                    idx = STATS_HEADERS.index(key)
                    if idx < len(row) and (key in _SAMPLE_STAT_KEYS or isinstance(row[idx], str)):
                        row[idx] = None
                redacted.append(row)
            source["stats_table"] = redacted
        source["samples_included"] = False
    return report
