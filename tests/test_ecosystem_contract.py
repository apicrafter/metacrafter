"""Ecosystem contract: benchmark baseline, thresholds logic and allow-list files."""
import json
from pathlib import Path

import yaml

from metacrafter.ecosystem import (
    benchmark_baseline,
    check_thresholds,
    compare_benchmark,
    registry_example_tests,
    thresholds_from_results,
)

ROOT = Path(__file__).parent


def test_benchmark_has_no_regressions():
    from metacrafter.core import CrafterCmd
    from metacrafter.product_tools import evaluate_fixture

    fixture = yaml.safe_load((ROOT / "benchmark" / "fixture.yaml").read_text())
    baseline = json.loads((ROOT / "benchmark" / "baseline.json").read_text())
    result = evaluate_fixture(CrafterCmd(quiet=True), fixture)
    assert compare_benchmark(result, baseline) == []
    assert result["datatypes"]["email"]["recall"] == 1.0
    # The mixed-country IBAN column shares only the generic IBAN check with de_iban.
    assert "de_iban" not in result["datatypes"]


def test_compare_benchmark_detects_drop():
    baseline = {"datatypes": {"email": {"precision": 1.0, "recall": 1.0}}}
    current = {"datatypes": {"email": {"precision": 1.0, "recall": 0.5}}}
    assert compare_benchmark(current, baseline) == [
        {"datatype": "email", "metric": "recall", "baseline": 1.0, "current": 0.5}
    ]
    assert benchmark_baseline(current) == {"datatypes": {"email": {"precision": 1.0, "recall": 0.5}}}


def test_thresholds_gate():
    results = {"rules": [
        {"ruleid": "a", "datatype": "x", "recall": 0.5, "missed": ["1"], "negatives_accepted": 0, "accepted_negatives": [], "checksum": False},
        {"ruleid": "b", "datatype": "y", "recall": 1.0, "missed": [], "negatives_accepted": 1, "accepted_negatives": ["bad"], "checksum": True},
    ]}
    baseline = thresholds_from_results(results)
    assert check_thresholds(results, baseline) == []
    stricter = dict(baseline, min_recall={"a": 0.8}, negative_exceptions={})
    problems = {f["problem"] for f in check_thresholds(results, stricter)}
    assert problems == {"recall below threshold", "accepts negative examples"}


def test_registry_example_tests_on_fixture(tmp_path):
    from metacrafter.classify.processor import RulesProcessor
    from metacrafter.registry.adapter import RegistryAdapter

    (tmp_path / "r.yaml").write_text(
        "name: t\ndescription: t\ncontext: common\nlang: common\nrules:\n"
        "  ibanbyregistry: {key: iban, match: registry, type: data, rule: iban}\n",
        encoding="utf-8",
    )
    processor = RulesProcessor()
    processor.import_rules_path(str(tmp_path))
    registry = RegistryAdapter.from_directory(ROOT / "fixtures" / "registry_snapshot")
    processor.bind_registry(registry, detectors="off")
    result = registry_example_tests(processor, registry)
    row = result["rules"][0]
    assert row["datatype"] == "iban" and row["negatives_tested"] >= 1


def test_ecosystem_files_are_well_formed():
    allow = yaml.safe_load((ROOT / "ecosystem" / "known_unresolved_rule_keys.yaml").read_text())
    assert isinstance(allow, dict) and allow
    thresholds = yaml.safe_load((ROOT / "ecosystem" / "rule_thresholds.yaml").read_text())
    assert thresholds["version"] == 1 and thresholds["min_recall"]
    lock = yaml.safe_load((ROOT.parent / "ecosystem.lock").read_text())
    assert {"metacrafter-rules", "metacrafter-registry"} <= set(lock)
