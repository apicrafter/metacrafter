"""Map report matches to labels of other classification systems via registry mappings."""
from __future__ import annotations

from typing import Any, Dict, List

from metacrafter.integrations.labels import match_datatype
from metacrafter.reports import report_sources

SYSTEMS = ("presidio", "google_dlp", "dpv", "purview", "schema_org", "wikidata")


def export_labels(payload: Any, system: str, min_confidence: float = 0.0) -> Dict[str, Any]:
    """Return ``{"system", "labels": [...], "unmapped": [...]}`` for a saved report.

    Each label row names the source, field, datatype, label(s) and confidence. Matches
    whose registry record has no mapping for ``system`` are listed in ``unmapped``.
    """
    if system not in SYSTEMS:
        raise ValueError(f"system must be one of: {', '.join(SYSTEMS)}")
    labels: List[Dict[str, Any]] = []
    unmapped: List[Dict[str, Any]] = []
    for source in report_sources(payload):
        for field in source.get("data", []):
            for match in field.get("matches", []):
                if float(match.get("confidence", 0)) < min_confidence:
                    continue
                row = {
                    "source": source.get("source"),
                    "field": field.get("field"),
                    "datatype": match_datatype(match),
                    "confidence": match.get("confidence"),
                }
                mapping = ((match.get("registry") or {}).get("mappings") or {}).get(system)
                if mapping:
                    row["label"] = mapping
                    labels.append(row)
                else:
                    unmapped.append(row)
    return {"system": system, "labels": labels, "unmapped": unmapped}
