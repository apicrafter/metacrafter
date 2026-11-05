# -*- coding: utf-8 -*-
"""Registry-driven LLM classification (``update-llm-classification``).

Pipeline per field:

1. **Shortlist** candidate datatypes from the registry: value identification (regexp,
   checksum, field-name similarity) plus field-name search. Raw values are used only
   locally for this step.
2. **Disclosure**: decide what the prompt may contain (field name only, masked value
   shapes, or raw values) from the sample policy, the provider location and whether the
   shortlist contains personal data.
3. **Prompt** with the shortlisted records, their positive examples and, separately,
   their negative examples as counter-examples.
4. **Constrained answer**: the JSON schema allows only shortlist ids or null; the answer
   is validated again, aliases resolved, and anything else rejected.
5. **Cache** by field name, a hash of the values and the registry version.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_SHORTLIST = 15
MAX_SAMPLES = 20
PROMPT_SAMPLES = 10
DOC_LIMIT = 300
_TOKEN = re.compile(r"[a-z0-9]+")
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def _tokens(text: str) -> set:
    return set(_TOKEN.findall(_CAMEL.sub(" ", str(text)).lower().replace("_", " ")))


def mask_value(value: Any, limit: int = 64) -> str:
    """Shape of a value: letters become ``a``/``A``, digits ``9``; punctuation is kept."""
    text = str(value)[:limit]
    return "".join(
        "9" if ch.isdigit() else ("A" if ch.isupper() else "a") if ch.isalpha() else ch for ch in text
    )


def _examples(record: Dict[str, Any], valid: bool, limit: int) -> List[str]:
    out = []
    for example in record.get("examples") or []:
        if isinstance(example, dict) and bool(example.get("valid", True)) is valid and example.get("value") is not None:
            out.append(str(example["value"]))
        if len(out) >= limit:
            break
    return out


def shortlist(adapter, field_name: str, values: List[Any], country: Optional[str] = None, k: int = DEFAULT_SHORTLIST) -> List[Dict[str, Any]]:
    """Candidate registry records for one field, best first (at most ``k``)."""
    if adapter is None or not adapter.available:
        return []
    samples = [str(v).strip() for v in values if v is not None and str(v).strip()][:MAX_SAMPLES]
    ids: List[str] = []

    def add(rid):
        canonical = adapter.resolve(rid).canonical_id
        if canonical and canonical not in ids:
            ids.append(canonical)

    try:
        for cand in adapter.identify(samples, field_name=field_name, country=country, limit=k):
            add(cand["id"])
    except Exception as exc:  # noqa: BLE001 - shortlist is best effort
        logger.debug("Registry identify failed: %s", exc)
    if field_name:
        try:
            # Search requires every token to match, so also try each name token alone
            # ("contact_email" -> "contact email", "contact", "email").
            name_tokens = sorted(_tokens(field_name))
            for query in [" ".join(name_tokens)] + [t for t in name_tokens if len(t) >= 3]:
                for cand in adapter.search(query, limit=k):
                    add(cand["id"])
        except Exception as exc:  # noqa: BLE001
            logger.debug("Registry search failed: %s", exc)
    return [adapter.record(rid) for rid in ids[:k] if adapter.record(rid)]


def classification_schema(candidate_ids: List[str]) -> Dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "datatype_id": {"anyOf": [{"type": "string", "enum": candidate_ids}, {"type": "null"}]} if candidate_ids else {"type": "null"},
            "confidence": {"type": "number"},
            "reason": {"type": "string"},
        },
        "required": ["datatype_id", "confidence", "reason"],
        "additionalProperties": False,
    }


def build_prompt(field_name: str, samples: List[str], disclosure: str, candidates: List[Dict[str, Any]]) -> str:
    lines = [
        "Identify the semantic datatype of one column of a dataset.",
        "",
        f"Column name: {field_name}",
    ]
    if disclosure == "raw" and samples:
        lines += ["Sample values:"] + [f"  - {v}" for v in samples]
    elif disclosure == "masked" and samples:
        lines += ["Sample value shapes (letters shown as a/A, digits as 9):"] + [f"  - {v}" for v in samples]
    else:
        lines.append("Sample values: not shared. Decide from the column name and the candidates.")
    lines += ["", "Candidate datatypes from the Metacrafter Registry:", ""]
    for record in candidates:
        lines.append(f"- id: {record['id']}")
        lines.append(f"  name: {record.get('name', '')}")
        doc = str(record.get("doc") or "").replace("\n", " ")
        if doc:
            lines.append(f"  description: {doc[:DOC_LIMIT]}")
        if record.get("status") == "draft":
            lines.append("  status: draft (not reviewed)")
        positives = _examples(record, True, 3)
        if positives:
            lines.append("  examples of this type: " + "; ".join(positives))
        negatives = _examples(record, False, 2)
        if negatives:
            lines.append("  NOT this type (counter-examples): " + "; ".join(negatives))
    lines += [
        "",
        "Answer with JSON: datatype_id must be one of the candidate ids above, or null when",
        "none fits. confidence is between 0 and 1. reason is one short sentence.",
    ]
    return "\n".join(lines)


class RegistryLLMClassifier:
    """LLM classification constrained to registry candidates."""

    accepts_raw_values = True

    def __init__(
        self,
        registry,
        provider,
        sample_policy: str = "names-only",
        send_pii_values: bool = False,
        max_requests: int = 100,
        shortlist_size: int = DEFAULT_SHORTLIST,
        neighbours=None,
    ) -> None:
        if sample_policy not in ("names-only", "redacted", "raw"):
            raise ValueError("sample_policy must be names-only, redacted or raw")
        self.registry = registry
        self.provider = provider
        self.sample_policy = sample_policy
        self.send_pii_values = send_pii_values
        self.max_requests = max_requests
        self.shortlist_size = shortlist_size
        self.neighbours = neighbours  # optional callable(field_name, samples) -> list of ids
        self.request_count = 0
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _cache_key(self, field_name: str, values: List[Any]) -> str:
        prov = self.registry.provenance if self.registry is not None else None
        payload = json.dumps(
            [field_name, [str(v) for v in values], getattr(prov, "version", None), getattr(prov, "git_sha", None), self.sample_policy],
            ensure_ascii=False,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def disclosure_for(self, candidates: List[Dict[str, Any]]) -> str:
        if self.sample_policy == "names-only":
            return "none"
        if self.sample_policy == "redacted":
            return "masked"
        pii_candidate = any(c.get("is_pii") for c in candidates)
        if pii_candidate and not getattr(self.provider, "is_local", False) and not self.send_pii_values:
            return "masked"
        return "raw"

    def classify_field(
        self,
        field_name: str,
        sample_values: Optional[Iterable[Any]] = None,
        country: Optional[Any] = None,
        langs=None,
        categories=None,
    ) -> Dict[str, Any]:
        values = [v for v in (sample_values or []) if v is not None][:MAX_SAMPLES]
        key = self._cache_key(field_name, values)
        if key in self._cache:
            return dict(self._cache[key], cached=True)
        country_code = country[0] if isinstance(country, (list, tuple)) and country else country
        candidates = shortlist(self.registry, field_name, values, country_code, self.shortlist_size)
        if self.neighbours is not None:
            known = {c["id"] for c in candidates}
            for rid in self.neighbours(field_name, values) or []:
                record = self.registry.record(rid)
                if record and record["id"] not in known and len(candidates) < self.shortlist_size + 5:
                    candidates.append(record)
                    known.add(record["id"])
        candidate_ids = [c["id"] for c in candidates]
        disclosure = self.disclosure_for(candidates)
        prompt_samples = [str(v)[:256] for v in values[:PROMPT_SAMPLES]]
        if disclosure == "masked":
            prompt_samples = [mask_value(v) for v in prompt_samples]
        evidence: Dict[str, Any] = {
            "shortlist": candidate_ids,
            "disclosure": disclosure,
            "provider": type(self.provider).__name__,
            "model": getattr(self.provider, "model", None),
        }
        if not candidates:
            result = {"field": field_name, "datatype_id": None, "confidence": 0.0, "reason": "No registry candidates", "is_pii": False, "evidence": evidence}
            self._cache[key] = result
            return result
        if self.request_count >= self.max_requests:
            raise ValueError("LLM request budget exceeded")
        self.request_count += 1
        prompt = build_prompt(field_name, prompt_samples, disclosure, candidates)
        answer = self.provider.classify(prompt, temperature=0.0, max_tokens=500, schema=classification_schema(candidate_ids))
        datatype_id = answer.get("datatype_id")
        confidence = answer.get("confidence", 0.0)
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not math.isfinite(confidence):
            raise ValueError("Invalid LLM confidence")
        confidence = min(1.0, max(0.0, float(confidence)))
        if datatype_id is not None:
            canonical = self.registry.resolve(str(datatype_id)).canonical_id
            if canonical not in candidate_ids:
                evidence["rejected"] = {"datatype_id": datatype_id, "reason": "Not in the registry shortlist"}
                logger.warning("LLM answer '%s' for field '%s' is not a shortlisted registry id; rejected", datatype_id, field_name)
                datatype_id, confidence = None, 0.0
            else:
                datatype_id = canonical
        record = self.registry.record(datatype_id) if datatype_id else None
        result = {
            "field": field_name,
            "datatype_id": datatype_id,
            "confidence": confidence,
            "reason": str(answer.get("reason", ""))[:500],
            "is_pii": bool(record and record.get("is_pii")),
            "evidence": evidence,
        }
        self._cache[key] = result
        return result
