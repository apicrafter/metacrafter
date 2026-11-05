"""Bind ``match: registry`` rules and derive detectors from registry records."""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

from metacrafter.engine.checksums import checksum_validator

logger = logging.getLogger(__name__)

DETECTOR_MODES = ("off", "stable", "all")
MIN_SPECIFICITY = 0.8
DERIVED_MAX_LEN = 256


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
            "registry_country": [c if isinstance(c, str) else c.get("id") for c in record.get("country") or []],
            "ruletype": "registry",
        }
        if vfunc is not None:
            rule["vfunc"] = vfunc
        processor.register_rule(rule)
        added += 1
    logger.info("Added %d registry-derived detectors (mode=%s)", added, mode)
    return added
