"""Contract checks between metacrafter, metacrafter-rules and metacrafter-registry.

* :func:`registry_example_tests` runs data rules and registry-derived detectors on the
  registry's positive and negative examples of their datatype.
* :func:`check_thresholds` turns those results into a CI gate using a thresholds file.
* :func:`compare_benchmark` compares an evaluation result with a stored baseline.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

THRESHOLDS_VERSION = 1


def _examples(record: Dict[str, Any]):
    positives, negatives = [], []
    for example in record.get("examples") or []:
        if not isinstance(example, dict) or example.get("value") is None:
            continue
        (positives if example.get("valid", True) else negatives).append(str(example["value"]))
    return positives, negatives


def _accepts(processor, rule: Dict[str, Any], value: str) -> bool:
    if len(value) < rule.get("minlen", 0) or len(value) > rule.get("maxlen", 10**9):
        return False
    return processor._value_check(rule, value)[1]


def registry_example_tests(processor, registry, rule_ids: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """Run every data rule on the registry examples of the datatype its key resolves to."""
    wanted = set(rule_ids) if rule_ids else None
    rows: List[Dict[str, Any]] = []
    for rule in processor.data_rules:
        if wanted and rule["id"] not in wanted:
            continue
        canonical = registry.resolve(rule.get("key", "")).canonical_id
        record = registry.record(canonical) if canonical else None
        if not record:
            continue
        positives, negatives = _examples(record)
        if not positives and not negatives:
            continue
        missed = [v for v in positives if not _accepts(processor, rule, v)]
        accepted = [v for v in negatives if _accepts(processor, rule, v)]
        rows.append({
            "ruleid": rule["id"],
            "datatype": canonical,
            "match": rule.get("match"),
            "checksum": bool(record.get("checksum")) or "vfunc" in rule,
            "imprecise": bool(rule.get("imprecise")),
            "positives_tested": len(positives),
            "positives_matched": len(positives) - len(missed),
            "negatives_tested": len(negatives),
            "negatives_accepted": len(accepted),
            "recall": round((len(positives) - len(missed)) / len(positives), 4) if positives else None,
            "missed": missed,
            "accepted_negatives": accepted,
        })
    tested_pos = sum(r["positives_tested"] for r in rows)
    matched_pos = sum(r["positives_matched"] for r in rows)
    return {
        "rules": rows,
        "summary": {
            "rules_tested": len(rows),
            "positives_tested": tested_pos,
            "positives_matched": matched_pos,
            "recall": round(matched_pos / tested_pos, 4) if tested_pos else None,
            "negatives_tested": sum(r["negatives_tested"] for r in rows),
            "negatives_accepted": sum(r["negatives_accepted"] for r in rows),
            "registry": registry.provenance.asdict(),
        },
    }


def thresholds_from_results(results: Dict[str, Any]) -> Dict[str, Any]:
    """Baseline thresholds: current recall per rule and current accepted negatives as exceptions."""
    min_recall = {}
    exceptions = {}
    for row in results["rules"]:
        if row["recall"] is not None:
            min_recall[row["ruleid"]] = row["recall"]
        if row["negatives_accepted"]:
            exceptions[row["ruleid"]] = "accepts registry negative examples; fix in metacrafter-rules"
    return {
        "version": THRESHOLDS_VERSION,
        "default_min_recall": 0.0,
        "min_recall": dict(sorted(min_recall.items())),
        "negative_exceptions": dict(sorted(exceptions.items())),
    }


def check_thresholds(results: Dict[str, Any], thresholds: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return gate failures: recall below the stored minimum, or accepted negatives without an exception.

    Rules with a checksum must accept no negative example unless listed in
    ``negative_exceptions``.
    """
    failures = []
    default = float(thresholds.get("default_min_recall", 0.0))
    minimums = thresholds.get("min_recall") or {}
    exceptions = thresholds.get("negative_exceptions") or {}
    for row in results["rules"]:
        rid = row["ruleid"]
        minimum = float(minimums.get(rid, default))
        if row["recall"] is not None and row["recall"] + 1e-9 < minimum:
            failures.append({"ruleid": rid, "datatype": row["datatype"], "problem": "recall below threshold",
                             "recall": row["recall"], "minimum": minimum, "missed": row["missed"]})
        if row["negatives_accepted"] and rid not in exceptions:
            failures.append({"ruleid": rid, "datatype": row["datatype"], "problem": "accepts negative examples",
                             "checksum": row["checksum"], "accepted": row["accepted_negatives"]})
    return failures


def compare_benchmark(result: Dict[str, Any], baseline: Dict[str, Any], tolerance: float = 0.02) -> List[Dict[str, Any]]:
    """Datatypes whose precision or recall dropped more than ``tolerance`` below the baseline."""
    regressions = []
    base = baseline.get("datatypes") or {}
    current = result.get("datatypes") or {}
    for datatype, old in sorted(base.items()):
        new = current.get(datatype)
        for metric in ("precision", "recall"):
            before = old.get(metric)
            if before is None:
                continue
            after = (new or {}).get(metric)
            if after is None or after + tolerance < before:
                regressions.append({"datatype": datatype, "metric": metric, "baseline": before, "current": after})
    return regressions


def benchmark_baseline(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "datatypes": {
            datatype: {"precision": score.get("precision"), "recall": score.get("recall")}
            for datatype, score in sorted((result.get("datatypes") or {}).items())
        }
    }
