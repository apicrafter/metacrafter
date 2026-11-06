# -*- coding: utf-8 -*-
"""LLM verification of rule matches (``--llm-verify``).

Rules and registry detectors propose datatypes from values; some proposals are plausible by
pattern but wrong in context (a CKAN ``state`` column is a record status, not a state; a
``package_id`` UUID is not a package name). The verifier sends one request per table: every
column path (siblings tell the domain), and for each column with value matches its type,
distinct-value count, value samples under the sample policy and the candidate datatypes with
their registry names and descriptions. The answer may only keep candidate ids; matches it
does not keep are removed. Field-name matches are never sent or removed.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Iterable, List, Optional

from metacrafter.classify.llm.registry_classifier import mask_value

logger = logging.getLogger(__name__)

PROMPT_SAMPLES = 5
DOC_LIMIT = 160

PROMPT = """You review automatic semantic-type labels for the columns of one dataset.
All column paths of the dataset (use them to infer its domain): {columns}

For each column below, keep only the candidate datatypes that are plausible given the column path,
its siblings, the value samples and the number of distinct values. Reject a candidate when the
column name or the dataset context contradicts it (a count is not a code, a status is not a
region, a local row id is not an external identifier). Reject country-specific types unless the
context supports that country. Value samples: {disclosure}.

Answer with a JSON object that maps every column below to the list of candidate ids to keep
(an empty list keeps none). Use only the candidate ids given for that column.

Columns:
{items}"""

DISCLOSURE_TEXT = {
    "names-only": "not shown",
    "masked": "letters are shown as a/A and digits as 9",
    "raw": "shown as they are",
}


def verification_schema(candidates: Dict[str, List[str]]) -> Dict[str, Any]:
    """JSON schema of the answer: one array of allowed candidate ids per column."""
    return {
        "type": "object",
        "properties": {
            field: {"type": "array", "items": {"type": "string", "enum": ids}}
            for field, ids in candidates.items()
        },
        "required": list(candidates),
        "additionalProperties": False,
    }


class LLMVerifier:
    """Keep or reject value matches of one table with a single LLM request."""

    def __init__(self, registry, provider, sample_policy: str = "names-only",
                 send_pii_values: bool = False, max_requests: int = 100) -> None:
        self.registry = registry
        self.provider = provider
        self.sample_policy = sample_policy
        self.send_pii_values = send_pii_values
        self.max_requests = max_requests
        self.request_count = 0
        self._cache: Dict[str, Dict[str, List[str]]] = {}

    def _describe(self, datatype_id: str) -> str:
        record = self.registry.record(datatype_id) if self.registry is not None and self.registry.available else None
        if not record:
            return datatype_id
        doc = " ".join(str(record.get("doc") or "").split())[:DOC_LIMIT]
        name = record.get("name") or datatype_id
        return f"{name}: {doc}" if doc else str(name)

    def _is_pii(self, datatype_id: str) -> bool:
        if self.registry is None or not self.registry.available:
            return False
        return bool(self.registry.is_pii(datatype_id))

    def disclosure(self, candidate_ids: Iterable[str]) -> str:
        """What the prompt may contain: ``names-only``, ``masked`` or ``raw``."""
        if self.sample_policy == "names-only":
            return "names-only"
        if self.sample_policy == "redacted":
            return "masked"
        if any(self._is_pii(c) for c in candidate_ids) and not getattr(self.provider, "is_local", False) \
                and not self.send_pii_values:
            return "masked"
        return "raw"

    def build_prompt(self, columns: List[str], table: List[Dict[str, Any]], disclosure: str) -> str:
        items = []
        for column in table:
            entry: Dict[str, Any] = {"column": column["field"], "type": column.get("ftype"),
                                     "distinct": column.get("distinct")}
            if disclosure != "names-only":
                values = [str(v)[:64] for v in column.get("values") or [] if v not in (None, "")]
                values = list(dict.fromkeys(values))[:PROMPT_SAMPLES]
                entry["samples"] = [mask_value(v) for v in values] if disclosure == "masked" else values
            entry["candidates"] = {c: self._describe(c) for c in column["candidates"]}
            items.append(json.dumps(entry, ensure_ascii=False))
        return PROMPT.format(columns=", ".join(columns), disclosure=DISCLOSURE_TEXT[disclosure],
                             items="\n".join(items))

    def _cache_key(self, prompt: str) -> str:
        version = getattr(getattr(self.registry, "provenance", None), "version", None) or "unknown"
        model = getattr(self.provider, "model", None) or ""
        return hashlib.sha256(f"{version}\n{model}\n{prompt}".encode("utf-8")).hexdigest()

    def verify(self, columns: List[str], table: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Return ``{"keep": {field: [ids]}, "disclosure": ..., "cached": bool}``.

        ``table`` items have ``field``, ``ftype``, ``distinct``, ``values`` and ``candidates``.
        A column missing from the answer keeps all its candidates.
        """
        from metacrafter.exceptions import LLMBudgetExceeded

        candidates = {column["field"]: list(column["candidates"]) for column in table}
        all_ids = {c for ids in candidates.values() for c in ids}
        disclosure = self.disclosure(all_ids)
        prompt = self.build_prompt(columns, table, disclosure)
        key = self._cache_key(prompt)
        if key in self._cache:
            return {"keep": self._cache[key], "disclosure": disclosure, "cached": True}
        if self.request_count >= self.max_requests:
            raise LLMBudgetExceeded(self.max_requests)
        self.request_count += 1
        answer = self.provider.complete_json(prompt, schema=verification_schema(candidates))
        keep: Dict[str, List[str]] = {}
        for field, ids in candidates.items():
            given = answer.get(field)
            if not isinstance(given, list):
                keep[field] = ids
                continue
            rejected = [str(item) for item in given if str(item) not in ids]
            if rejected:
                logger.warning("LLM verifier kept ids that were not candidates of '%s': %s; ignored", field, rejected)
            keep[field] = [i for i in ids if i in {str(item) for item in given}]
        self._cache[key] = keep
        return {"keep": keep, "disclosure": disclosure, "cached": False}
