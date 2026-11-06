"""Bind ``match: registry`` rules and derive detectors from registry records."""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from metacrafter.engine.background import GENERIC_THRESHOLD, background_broadness
from metacrafter.engine.checksums import checksum_validator

logger = logging.getLogger(__name__)

DETECTOR_MODES = ("off", "stable", "all")
MIN_SPECIFICITY = 0.8
DERIVED_MAX_LEN = 256
# Tokens too common in field names to count as evidence for a datatype on their own.
NAME_STOP_TOKENS = frozenset(
    "code codes number num no nr id ids identifier key value name type data info the of and by for".split()
)
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_TOKEN = re.compile(r"[a-z0-9]+")


def name_tokens(text: str) -> List[str]:
    """Lower-case alphanumeric tokens of a name; camelCase and separators split tokens."""
    return _TOKEN.findall(_CAMEL.sub(" ", str(text)).lower())


def detector_name_keys(record: Dict[str, Any], aliases: Iterable[str], countries: Sequence[str]) -> List[Tuple[str, ...]]:
    """Token sets a field name must contain to count as evidence for a registry datatype.

    One key per label (the record id and each alias), without stop tokens and the record's
    own country codes: ``am_postal_code`` gives ``("postal",)``.
    """
    skip = NAME_STOP_TOKENS | {c.lower() for c in countries}
    keys = []
    for label in [record["id"], *aliases]:
        tokens = tuple(t for t in name_tokens(label) if t not in skip)
        if tokens and tokens not in keys:
            keys.append(tokens)
    return keys


def detector_name_evidence(rule: Dict[str, Any], field_name: str) -> bool:
    """True when ``field_name`` names the detector's datatype (see :func:`detector_name_keys`).

    Every key token must be a token of the field name, or (for tokens of 4+ characters) a
    substring of the field name with separators removed: ``postal`` matches ``postalcode``.
    """
    keys = rule.get("name_keys")
    if not keys:
        return False
    tokens = name_tokens(field_name)
    present = set(tokens)
    joined = "".join(tokens)
    return any(all(t in present or (len(t) >= 4 and t in joined) for t in key) for key in keys)


def bind_registry_rules(processor, adapter) -> None:
    """Compile ``match: registry`` rules from their registry records.

    A rule whose record is missing, has no regexp, or whose ``key`` resolves to a
    different record is disabled with a diagnostic.
    """
    for rules in (processor.field_rules, processor.data_rules):
        kept: List[Dict[str, Any]] = []
        for rule in rules:
            if rule.get("match") != "registry":
                kept.append(rule)
                continue
            record = adapter.record(rule["registry_id"]) if adapter.available else None
            error = None
            if record is None:
                error = "Registry record unavailable"
            elif not record.get("regexp"):
                error = "Registry record has no regexp"
            elif adapter.resolve(rule["key"]).canonical_id != record["id"]:
                error = "Rule key does not match the registry record"
            if error is None:
                try:
                    rule["compiled"] = re.compile(record["regexp"])
                except re.error:
                    error = "Registry regexp does not compile"
            if error:
                processor.diagnostics.append({"ruleid": rule.get("id"), "source": rule.get("source"), "error": error})
                logger.warning("Disabling registry rule '%s': %s", rule.get("id"), error)
                continue
            if "vfunc" not in rule and record.get("checksum"):
                vfunc = checksum_validator("checksum:" + record["checksum"])
                if vfunc is not None:
                    rule["vfunc"] = vfunc
            rule["registry_status"] = record.get("status")
            kept.append(rule)
        rules[:] = kept


def add_registry_detectors(processor, adapter, mode: str = "stable") -> int:
    """Add data detectors for registry datatypes that no data rule targets.

    ``stable``: stable records with a checksum or a regexp specificity >= 0.8.
    ``all``: every record with a regexp; records outside the ``stable`` criteria are
    imprecise (skipped unless imprecise rules are included).

    Each detector records its background broadness (:mod:`metacrafter.engine.background`).
    A detector whose regexp is generic and has no checksum is marked ``needs_name_evidence``:
    it reports a match only when the field name names its datatype.
    Returns the number of detectors added.
    """
    if mode not in DETECTOR_MODES:
        raise ValueError(f"registry detectors mode must be one of {', '.join(DETECTOR_MODES)}")
    if mode == "off" or not adapter.available:
        return 0
    covered = set()
    for rule in processor.data_rules:
        resolution = adapter.resolve(rule.get("key", ""))
        covered.add(resolution.canonical_id or rule.get("key"))
    candidates = [
        record
        for record in adapter.registry
        if record.get("regexp")
        and record.get("kind", "datatype") == "datatype"
        and record.get("status") != "deprecated"
        and record["id"] not in covered
    ]
    if mode == "stable":
        candidates = [r for r in candidates if r.get("status") == "stable"]
    need_specificity = [r["id"] for r in candidates if not r.get("checksum") or mode == "all"]
    specificity = adapter.specificities(need_specificity)
    version = adapter.provenance.version or "unknown"
    aliases_of: Dict[str, List[str]] = {}
    for alias, canonical in (getattr(adapter.registry, "aliases", None) or {}).items():
        aliases_of.setdefault(canonical, []).append(alias)
    added = 0
    for record in candidates:
        rid = record["id"]
        vfunc = checksum_validator("checksum:" + record["checksum"]) if record.get("checksum") else None
        spec = specificity.get(rid, 1.0)
        qualifies = record.get("status") == "stable" and (vfunc is not None or spec >= MIN_SPECIFICITY)
        if mode == "stable" and not qualifies:
            continue
        try:
            compiled = re.compile(record["regexp"])
        except re.error:
            continue
        countries = [c if isinstance(c, str) else c.get("id") for c in record.get("country") or []]
        countries = [c for c in countries if c]
        broadness, broad_class = background_broadness(record["regexp"])
        rule = {
            "id": f"registry:{rid}",
            "key": rid,
            "name": record.get("name"),
            "type": "data",
            "match": "registry",
            "rule": rid,
            "registry_id": rid,
            "compiled": compiled,
            "minlen": 1,
            "maxlen": DERIVED_MAX_LEN,
            "priority": 3,
            "imprecise": not qualifies,
            "deprecated": False,
            # PII comes from the registry record (context "pii"); enrichment reports
            # pii_source "registry".
            "is_pii": False,
            "context": ["registry"] + (["pii"] if record.get("is_pii") else []),
            "lang": "common",
            "group": "registry",
            "group_desc": "Detectors derived from metacrafter-registry records",
            "country_code": None,
            "source": f"registry:{version}",
            "specificity": spec,
            "registry_status": record.get("status"),
            "registry_country": countries,
            "ruletype": "registry",
            "derived": True,
            "background_broadness": broadness,
            "background_class": broad_class,
            "needs_name_evidence": vfunc is None and broadness >= GENERIC_THRESHOLD,
            "name_keys": detector_name_keys(record, aliases_of.get(rid, []), countries),
        }
        if vfunc is not None:
            rule["vfunc"] = vfunc
        processor.register_rule(rule)
        added += 1
    logger.info("Added %d registry-derived detectors (mode=%s)", added, mode)
    return added
