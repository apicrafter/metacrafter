"""PII flags of value-only matches outranked by a datatype the field name names.

Values of one datatype can be valid strings of another: OKPD2 codes such as
``21.20.10.226`` are IPv4 addresses. When the field name names a datatype that matches the
values and ranks at or above the value-only match (field ``okpd2``: ``okpd`` 100, ``ipv4``
99), the value-only match stays in the report as a secondary match, but it does not make
the field PII (apicrafter/metacrafter#62).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional

# Match types that carry value evidence (rule values, registry-derived detectors).
VALUE_RULETYPES = ("data", "registry")


def _name_score(match: Dict[str, Any]) -> float:
    evidence = match.get("evidence")
    if not isinstance(evidence, dict):
        return 0.0
    try:
        return float(evidence.get("name_score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _confidence(match: Dict[str, Any]) -> float:
    try:
        return float(match.get("confidence") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _datatype(match: Dict[str, Any]) -> Optional[str]:
    return match.get("datatype") or match.get("dataclass")


def demote_outranked_pii(
    matches: List[Dict[str, Any]],
    ancestors: Optional[Callable[[str], Iterable[str]]] = None,
) -> List[Dict[str, Any]]:
    """Clear ``is_pii`` of value-only matches that a named non-PII datatype outranks.

    A PII match is demoted when all of these hold:

    * it is a value match (``data`` or ``registry``) without field-name evidence, and no
      match of the same datatype has field-name evidence;
    * another value match has field-name evidence, is not PII, is a different datatype
      and has the same or a higher confidence;
    * the two datatypes are not related through the registry ``parent`` chain
      (``ancestors``), so a specialization does not hide its broader PII type.

    The demoted match keeps its registry metadata (``registry.is_pii``) and gets
    ``pii_outranked_by`` with the datatype that outranks it. Matches are changed in place.
    A field-name-only match (``ruletype: field``) never outranks a value match, so a field
    ``login`` with email values stays PII.
    """
    named = [
        m for m in matches
        if m.get("ruletype") in VALUE_RULETYPES and _name_score(m) > 0 and not m.get("is_pii")
    ]
    if not named:
        return matches
    name_supported = {_datatype(m) for m in matches if _name_score(m) > 0}
    for match in matches:
        datatype = _datatype(match)
        if (
            not match.get("is_pii")
            or match.get("ruletype") not in VALUE_RULETYPES
            or datatype in name_supported
        ):
            continue
        for winner in named:
            other = _datatype(winner)
            if other == datatype or _confidence(winner) < _confidence(match):
                continue
            if ancestors is not None and datatype and other and (
                datatype in set(ancestors(other)) or other in set(ancestors(datatype))
            ):
                continue
            match["is_pii"] = False
            match["pii_source"] = None
            match["pii_outranked_by"] = other
            break
    return matches
