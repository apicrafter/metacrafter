"""Value identification, search and crosswalks over registry snapshot records.

Ported from ``metacrafter_registry.client`` (Apache-2.0) so metacrafter needs only the
published ``/api/v1/`` data, not the registry package. The scoring matches the registry's
``identify``: 60 x weighted regexp hit rate x regexp specificity + 25 x check-digit pass
rate + 25 x field-name similarity, +5 for a matching country, -10 for a different one,
-5 for drafts, capped at 100.
"""
from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterable, List, Optional

from metacrafter.engine.checksum_algorithms import ALGORITHMS

_TOKEN = re.compile(r"[a-z0-9]+")
_SEPARATORS = re.compile(r"[\s-]")
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NO_MATCH, _RAW, _COMPACT = 0, 1, 2
_COMPACT_WEIGHT = 0.5
_QDDATE_VARIANT = re.compile(r"(?::time_(?:\d+|left))?(?::t_right)?$")


def tokens(text: str) -> set:
    return set(_TOKEN.findall(_CAMEL.sub(" ", str(text)).lower()))


def qddate_key(value: str) -> str:
    """Base qddate key: ``dt:date:date_2:time_1`` and ``date_2`` give ``dt:date:date_2``."""
    key = _QDDATE_VARIANT.sub("", str(value).strip(), count=1)
    return key if key.startswith("dt:") else "dt:date:" + key


def match_kind(pattern: "re.Pattern[str]", value: str) -> int:
    if pattern.fullmatch(value) is not None:
        return _RAW
    compact = _SEPARATORS.sub("", value)
    if compact != value and pattern.fullmatch(compact) is not None:
        return _COMPACT
    return _NO_MATCH


def _ids(values: Any) -> List[str]:
    out = []
    for value in values or []:
        if isinstance(value, dict):
            value = value.get("id")
        if value:
            out.append(str(value))
    return out


class RecordIndex:
    """Lazy indexes over registry records: compiled regexps, broadness, search."""

    def __init__(self, records: Dict[str, Dict[str, Any]]) -> None:
        self.records = records
        self._compiled: Optional[Dict[str, "re.Pattern[str]"]] = None
        self._broadness: Optional[Dict[str, float]] = None

    @property
    def compiled(self) -> Dict[str, "re.Pattern[str]"]:
        if self._compiled is None:
            compiled = {}
            for rid, record in self.records.items():
                if record.get("regexp"):
                    try:
                        compiled[rid] = re.compile(record["regexp"])
                    except re.error:
                        continue
            self._compiled = compiled
        return self._compiled

    def broadness(self, ids: Optional[Iterable[str]] = None) -> Dict[str, float]:
        """Share of other records' positive examples that each regexp fully matches."""
        if self._broadness is None:
            self._broadness = {}
        wanted = list(ids) if ids is not None else list(self.compiled)
        missing = [rid for rid in wanted if rid in self.compiled and rid not in self._broadness]
        if missing:
            corpus = [
                (record["id"], str(example["value"]))
                for record in self.records.values()
                for example in record.get("examples") or []
                if isinstance(example, dict) and example.get("valid", True) and example.get("value") is not None
            ]
            for rid in missing:
                pattern = self.compiled[rid]
                others = [value for owner, value in corpus if owner != rid]
                matched = sum(1 for value in others if pattern.fullmatch(value))
                self._broadness[rid] = round(matched / len(others), 3) if others else 0.0
        return {rid: self._broadness[rid] for rid in wanted if rid in self._broadness}

    @staticmethod
    def name_similarity(record: Dict[str, Any], field_name: str) -> float:
        field = field_name.strip().lower()
        if not field:
            return 0.0
        aliases = record.get("aliases") or []
        if field == record["id"] or field in aliases:
            return 1.0
        field_tokens = tokens(field_name)
        if not field_tokens:
            return 0.0
        best = 0.0
        for candidate_id in (record["id"], *aliases):
            compact = candidate_id.replace("_", "")
            if any(len(tok) >= 4 and tok in compact for tok in field_tokens):
                best = 0.6
        candidates = [record["id"], *aliases, record.get("name", "")]
        candidates += [t.get("name", "") for t in (record.get("translations") or {}).values() if isinstance(t, dict)]
        for candidate in candidates:
            cand_tokens = tokens(candidate)
            if not cand_tokens:
                continue
            best = max(best, len(field_tokens & cand_tokens) / len(field_tokens | cand_tokens))
        return best

    def identify(
        self,
        values: Iterable[Any],
        *,
        field_name: Optional[str] = None,
        country: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Rank candidate datatypes for sample values of one column (registry scoring)."""
        samples = [str(v).strip() for v in values if v is not None and str(v).strip()]
        country = country.upper() if country else None
        kinds = {rid: [match_kind(p, v) for v in samples] for rid, p in self.compiled.items()} if samples else {}
        hit_ids = [rid for rid, ks in kinds.items() if any(ks)]
        broadness = self.broadness(hit_ids)
        claimed = [math.inf] * len(samples)
        for rid in hit_ids:
            for i, kind in enumerate(kinds[rid]):
                if kind == _RAW:
                    claimed[i] = min(claimed[i], broadness.get(rid, 0.0))
        results = []
        for rid, record in self.records.items():
            if status and record.get("status") != status:
                continue
            hit_rate = None
            weighted = 0.0
            algorithm = record.get("checksum")
            checks = [ALGORITHMS[algorithm](v) for v in samples] if samples and algorithm in ALGORITHMS else None
            if rid in kinds and any(kinds[rid]):
                own = broadness.get(rid, 0.0)
                hits, weight = 0, 0.0
                for i, kind in enumerate(kinds[rid]):
                    if kind == _RAW:
                        hits += 1
                        weight += 1.0
                    elif kind == _COMPACT and claimed[i] > own:
                        hits += 1
                        weight += 1.0 if checks and checks[i] else _COMPACT_WEIGHT
                hit_rate = hits / len(samples)
                weighted = weight / len(samples)
            checksum = None
            if checks is not None:
                checksum = {"algorithm": algorithm, "pass_rate": sum(checks) / len(samples)}
            similarity = self.name_similarity(record, field_name) if field_name else 0.0
            if not (hit_rate or (checksum and checksum["pass_rate"]) or similarity >= 0.5):
                continue
            specificity = 1.0 - broadness.get(rid, 0.0)
            score = 60 * weighted * specificity + 25 * similarity
            if checksum:
                score += 25 * checksum["pass_rate"]
            countries = [c.upper() for c in _ids(record.get("country"))]
            if country and countries:
                score += 5 if country in countries else -10
            if record.get("status") == "draft":
                score -= 5
            results.append({
                "id": rid,
                "name": record.get("name"),
                "status": record.get("status"),
                "is_pii": record.get("is_pii"),
                "score": max(0, min(100, round(score))),
                "regexp_hit_rate": None if hit_rate is None else round(hit_rate, 3),
                "regexp_specificity": None if hit_rate is None else round(specificity, 3),
                "checksum": None if checksum is None else {**checksum, "pass_rate": round(checksum["pass_rate"], 3)},
                "field_name_similarity": round(similarity, 3),
            })
        results.sort(key=lambda r: (-r["score"], r["status"] != "stable", r["id"]))
        return results[:limit]

    def search(self, query: str = "", *, limit: int = 20) -> List[Dict[str, Any]]:
        """Rank records by query tokens over id, aliases, names, categories and description."""
        query_tokens = _TOKEN.findall(query.lower()) if query else []
        hits = []
        for record in self.records.values():
            score = self._text_score(record, query_tokens) if query_tokens else 1
            if score:
                hits.append((score, record))
        hits.sort(key=lambda hit: (-hit[0], hit[1]["id"]))
        return [record for _, record in hits[:limit]]

    @staticmethod
    def _text_score(record: Dict[str, Any], query_tokens: List[str]) -> int:
        rid = record["id"]
        aliases_list = record.get("aliases") or []
        aliases = " ".join(aliases_list)
        name = str(record.get("name", "")).lower()
        names = " ".join(t.get("name", "") for t in (record.get("translations") or {}).values() if isinstance(t, dict)).lower()
        cats = " ".join(_ids(record.get("categories")) + _ids(record.get("country"))).lower()
        doc = str(record.get("doc", "")).lower()
        total = 0
        for t in query_tokens:
            if rid == t or t in aliases_list:
                s = 100
            elif rid.startswith(t):
                s = 60
            elif t in rid or t in aliases:
                s = 40
            elif t in name:
                s = 35
            elif t in names:
                s = 25
            elif t in cats:
                s = 10
            elif t in doc:
                s = 8
            else:
                return 0
            total += s
        return total

    def qddate_pattern(self, date_format: str) -> Optional[str]:
        wanted = qddate_key(date_format)
        for record in self.records.values():
            values = (record.get("mappings") or {}).get("qddate")
            values = values if isinstance(values, list) else [values]
            if any(v and qddate_key(v) == wanted for v in values):
                return record["id"]
        return None
