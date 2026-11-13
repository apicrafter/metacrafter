"""Confidence scoring model (``scoring_model: v1``).

For a candidate datatype on one field:

* value evidence: ``validated_hit_rate x specificity`` where the validated hit rate
  counts values that match the pattern and pass the rule validator or checksum,
  and specificity is 1 for curated rules and ``1 - broadness`` for registry
  patterns;
* ``confidence = 100 x value_evidence + name bonus + country bonus - draft penalty``,
  clamped to 0..100;
* a field-name-only match scores ``60 x priority weight`` (1 -> 1.0, 2 -> 0.8,
  3 -> 0.6), moved by how well the values fit the datatype (``value_fit``): up to
  ``FIT_BONUS`` (30) more when a specific check of the datatype passes the values, up to
  ``MISFIT_PENALTY`` (10) less when the values fail even a broad check of its shape. A
  match without such checks (``name``, ``category``) keeps ``60 x priority weight``. The
  penalty is small because the checks of a datatype do not cover all its forms (phone
  numbers with brackets, postcodes, country names where the registry has country codes):
  a priority 1 name match stays at the default threshold of 50, and ``--confidence 60``
  drops the ones whose values do not fit.

Value fit of a field-name match for datatype T, from the checks of T: its value rules
(curated and registry-derived, imprecise ones included), the regexps of its registry
record and pattern records (with their checksums), and the validator of the field rule.
Each check has a pass rate on the sampled values and a background broadness ``b``
(:func:`metacrafter.engine.background.check_broadness`: how much ordinary data it passes).

* ``support = max(rate x (1 - b))``: a specific check that passes the values;
* ``misfit = (1 - rate of passing any shape check) x max(b of the shape checks)``:
  values that even a broad check rejects (word positions ``58``, ``226`` named ``year``).
  Dictionary rules (``match: text``) give support only: a dictionary that misses a name
  says little against it.

``--legacy-scoring`` restores the pre-1.0 behaviour: the share of matching values,
and 100 for field-name matches.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

SCORING_MODEL = "v1"
LEGACY_MODEL = "legacy"
FIELD_ONLY_CAP = 60.0
NAME_BONUS = 10.0
COUNTRY_BONUS = 5.0
COUNTRY_PENALTY = 10.0
DRAFT_PENALTY = 5.0
FIT_BONUS = 30.0
MISFIT_PENALTY = 10.0
PRIORITY_WEIGHTS = {1: 1.0, 2: 0.8, 3: 0.6}


def priority_weight(priority: Optional[int]) -> float:
    return PRIORITY_WEIGHTS.get(int(priority or 3), 0.6)


def field_score(priority: Optional[int], legacy: bool = False, fit: Optional[Dict[str, Any]] = None) -> float:
    """Confidence of a field-name match: ``60 x priority weight``, moved by ``fit`` (the
    ``support`` and ``misfit`` of the values, see the module docstring)."""
    if legacy:
        return 100.0
    score = FIELD_ONLY_CAP * priority_weight(priority)
    if fit:
        score += FIT_BONUS * float(fit.get("support") or 0.0) - MISFIT_PENALTY * float(fit.get("misfit") or 0.0)
    return round(max(0.0, min(100.0, score)), 2)


def data_score(
    *,
    matched: int,
    pattern_hits: int,
    considered: int,
    specificity: float = 1.0,
    name_weight: float = 0.0,
    country_match: Optional[bool] = None,
    draft: bool = False,
    legacy: bool = False,
    has_validator: bool = False,
    empty: int = 0,
    sampled: int = 0,
) -> Dict[str, Any]:
    """Score value evidence for one rule and return ``confidence`` plus ``evidence``."""
    hit_rate = pattern_hits / considered if considered else 0.0
    validated_rate = matched / considered if considered else 0.0
    evidence: Dict[str, Any] = {
        "scoring_model": LEGACY_MODEL if legacy else SCORING_MODEL,
        "sampled_count": sampled,
        "considered_count": considered,
        "empty_count": empty,
        "matched_count": matched,
        "hit_rate": round(hit_rate, 4),
    }
    if has_validator:
        evidence["checksum_rate"] = round(matched / pattern_hits, 4) if pattern_hits else 0.0
    if legacy:
        evidence["confidence_components"] = {"value": round(100.0 * validated_rate, 4)}
        return {"confidence": 100.0 * validated_rate, "evidence": evidence}

    value = validated_rate * specificity
    name_bonus = NAME_BONUS * name_weight if matched else 0.0
    country_bonus = 0.0
    if country_match is True:
        country_bonus = COUNTRY_BONUS
    elif country_match is False:
        country_bonus = -COUNTRY_PENALTY
    penalty = DRAFT_PENALTY if draft else 0.0
    confidence = max(0.0, min(100.0, 100.0 * value + name_bonus + country_bonus - penalty))
    evidence.update(
        specificity=round(specificity, 4),
        name_score=round(name_weight, 4),
        country_bonus=country_bonus,
        draft_penalty=penalty,
    )
    return {"confidence": confidence, "evidence": evidence}
