# -*- coding: utf-8 -*-
"""Rule key checks against metacrafter-registry (``metacrafter rules check``)."""
from __future__ import annotations

import json
from typing import Any, Dict, List

from tabulate import tabulate


def check_rule_keys(processor, registry) -> Dict[str, List[Dict[str, Any]]]:
    """Classify every loaded rule by how its ``key`` resolves in the registry.

    Returns a dict with ``unknown``, ``alias`` and ``deprecated`` lists. Each entry
    names the rule id, key, source file and, where relevant, the canonical id or
    replacement.
    """
    result: Dict[str, List[Dict[str, Any]]] = {"unknown": [], "alias": [], "deprecated": []}
    rules = list(processor.field_rules) + list(processor.data_rules)
    for rule in sorted(rules, key=lambda r: (str(r.get("key")), str(r.get("id")))):
        source = str(rule.get("source") or "")
        entry = {"rule": rule.get("id"), "key": rule.get("key"), "file": "/".join(source.replace("\\", "/").split("/")[-3:])}
        resolution = registry.resolve(rule.get("key", ""))
        if not resolution.known:
            result["unknown"].append(entry)
            continue
        if resolution.via_alias:
            result["alias"].append({**entry, "canonical": resolution.canonical_id})
        if resolution.status == "deprecated":
            result["deprecated"].append({**entry, "replaced_by": resolution.replaced_by})
    result["summary"] = [
        {
            "rules": len(rules),
            "unknown": len(result["unknown"]),
            "alias": len(result["alias"]),
            "deprecated": len(result["deprecated"]),
            "registry": registry.provenance.asdict(),
        }
    ]
    return result


def format_rule_check(result: Dict[str, Any], output_format: str = "table") -> str:
    if output_format == "json":
        return json.dumps(result, indent=2, ensure_ascii=False)
    summary = result["summary"][0]
    lines = [
        f"Rules: {summary['rules']}, unknown keys: {summary['unknown']}, "
        f"alias keys: {summary['alias']}, deprecated targets: {summary['deprecated']}",
        f"Registry: {summary['registry'].get('source')} (version {summary['registry'].get('version')})",
    ]
    for name in ("unknown", "alias", "deprecated"):
        rows = result[name]
        if rows:
            lines.append("")
            lines.append(f"{name.capitalize()} ({len(rows)}):")
            lines.append(tabulate(rows, headers="keys"))
    return "\n".join(lines)
