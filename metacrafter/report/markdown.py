"""Markdown rendering of scan reports for human review and AI agents."""
from __future__ import annotations

from typing import Any, Dict, List


def _escape(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "\\|").replace("\n", " ")


def _source_md(report: Dict[str, Any], include_samples: bool) -> List[str]:
    lines = [f"## {_escape(report.get('source') or report.get('table') or 'data')}", ""]
    if not report.get("complete", True):
        lines += ["**Incomplete scan.** Errors: " + _escape(report.get("errors")), ""]
    lines += ["| Field | Datatype | Confidence | PII | Registry |", "|---|---|---|---|---|"]
    pii_fields = []
    for field in report.get("data") or report.get("fields") or []:
        matches = field.get("matches") or []
        if not matches:
            lines.append(f"| {_escape(field.get('field'))} | — | | | |")
            continue
        for index, match in enumerate(matches):
            name = _escape(field.get("field")) if index == 0 else ""
            datatype = match.get("datatype") or match.get("dataclass")
            reg = match.get("registry") or {}
            label = _escape(reg.get("name") or datatype)
            status = " (draft)" if reg.get("status") == "draft" else ""
            link = f"[{_escape(datatype)}]({match.get('classurl')})" if match.get("classurl") else _escape(datatype)
            lines.append(
                f"| {name} | {label}{status} | {float(match.get('confidence', 0)):.1f} | "
                f"{'yes' if match.get('is_pii') else ''} | {link} |"
            )
        if any(m.get("is_pii") for m in matches):
            pii_fields.append(field)
    lines.append("")
    if pii_fields:
        lines.append("**PII fields:**")
        lines.append("")
        for field in pii_fields:
            types = sorted({m.get("datatype") or m.get("dataclass") for m in field["matches"] if m.get("is_pii")})
            lines.append(f"- `{_escape(field.get('field'))}`: {', '.join(types)}")
        lines.append("")
    else:
        lines += ["No PII fields detected.", ""]
    groups = report.get("field_groups") or []
    if groups:
        lines += ["**Field groups:**", ""]
        for group in groups:
            label = "prefix" if group.get("kind") == "prefix" else "suffix"
            fields = ", ".join(f"`{_escape(f)}`" for f in group.get("fields") or [])
            lines.append(f"- {label} `{_escape(group.get('name'))}`: {fields}")
        lines.append("")
    if include_samples:
        lines += ["### Statistics", "", "```", _escape(report.get("stats")), "```", ""]
    return lines


def render_markdown(report: Any, include_samples: bool = False) -> str:
    """Render a report (single or aggregate) as Markdown without scanned values by default."""
    sources = report.get("sources") if isinstance(report, dict) and "sources" in report else [report]
    lines = ["# Metacrafter scan report", ""]
    first = sources[0] if sources else {}
    reg = first.get("registry") or {}
    if reg:
        lines += [
            f"Registry: {reg.get('source')} (version {reg.get('version')}), "
            f"scoring model: {first.get('scoring_model', 'v1')}",
            "",
        ]
    for source in sources:
        lines += _source_md(source, include_samples)
    return "\n".join(lines).rstrip() + "\n"
