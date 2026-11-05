"""Frictionless Table Schema export with semantic annotations from the registry."""
from __future__ import annotations

from typing import Any, Dict, List

from metacrafter.integrations.labels import match_datatype
from metacrafter.reports import report_sources

_FTYPES = {
    "str": "string",
    "int": "integer",
    "float": "number",
    "bool": "boolean",
    "date": "date",
    "datetime": "datetime",
    "dict": "object",
    "list": "array",
}


def _rdf_type(match: Dict[str, Any]) -> str | None:
    mappings = (match.get("registry") or {}).get("mappings") or {}
    schema_org = mappings.get("schema_org")
    if schema_org:
        value = schema_org[0] if isinstance(schema_org, list) else schema_org
        return value if "://" in str(value) else f"https://schema.org/{value}"
    wikidata = mappings.get("wikidata")
    if wikidata:
        value = wikidata[0] if isinstance(wikidata, list) else wikidata
        return value if "://" in str(value) else f"http://www.wikidata.org/entity/{value}"
    return None


def table_schema(source: Dict[str, Any], min_confidence: float = 0.0) -> Dict[str, Any]:
    fields: List[Dict[str, Any]] = []
    for field in source.get("data", []):
        entry: Dict[str, Any] = {
            "name": field.get("field"),
            "type": _FTYPES.get(str(field.get("ftype")), "any"),
        }
        matches = [m for m in field.get("matches", []) if float(m.get("confidence", 0)) >= min_confidence]
        if matches:
            best = max(matches, key=lambda m: float(m.get("confidence", 0)))
            rdf = _rdf_type(best)
            if rdf:
                entry["rdfType"] = rdf
            entry["metacrafter:datatype"] = match_datatype(best)
            if best.get("classurl"):
                entry["metacrafter:url"] = best["classurl"]
            entry["metacrafter:confidence"] = best.get("confidence")
            entry["metacrafter:pii"] = bool(any(m.get("is_pii") for m in matches))
            name = (best.get("registry") or {}).get("name")
            if name:
                entry["description"] = name
        fields.append(entry)
    return {"fields": fields}


def export_frictionless(payload: Any, min_confidence: float = 0.0) -> Dict[str, Any]:
    """A Frictionless Data Package descriptor with one resource schema per source."""
    resources = []
    for index, source in enumerate(report_sources(payload)):
        name = str(source.get("source") or f"resource{index}")
        resources.append({
            "name": "".join(c if c.isalnum() or c in "-_." else "_" for c in name.lower()) or f"resource{index}",
            "path": name,
            "schema": table_schema(source, min_confidence),
        })
    return {"profile": "data-package", "resources": resources}
