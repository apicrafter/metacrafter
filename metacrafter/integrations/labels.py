"""Shared label logic for catalog exporters: datatype ids, PII and registry properties.

Exporters never keep their own lists of datatypes: PII and names come from the match
data and its ``registry`` reference (see ``metacrafter.registry.adapter``).
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def match_datatype(match: Dict[str, Any]) -> Optional[str]:
    """Canonical registry id of a match, falling back to the rule key."""
    return match.get("datatype") or match.get("dataclass")


def registry_properties(match: Dict[str, Any], prefix: str = "metacrafter_") -> Dict[str, str]:
    """String properties describing the registry record of a match."""
    reg = match.get("registry") or {}
    props: Dict[str, str] = {}
    if reg.get("name"):
        props[prefix + "datatype_name"] = str(reg["name"])
    if reg.get("status"):
        props[prefix + "datatype_status"] = str(reg["status"])
    if reg.get("categories"):
        props[prefix + "datatype_categories"] = ",".join(reg["categories"])
    if match.get("pii_source"):
        props[prefix + "pii_source"] = str(match["pii_source"])
    if match.get("rule_key") and match.get("rule_key") != match_datatype(match):
        props[prefix + "rule_key"] = str(match["rule_key"])
    for system, value in sorted((reg.get("mappings") or {}).items()):
        props[prefix + "mapping_" + system] = ",".join(value) if isinstance(value, list) else str(value)
    return props
