"""Detection engine v2: rule format, match types, validators, scoring, registry detectors."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from metacrafter.classify.processor import RulesProcessor
from metacrafter.registry.adapter import RegistryAdapter, clear_adapter_cache

SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"

HEADER = """
format_version: 2
name: t
description: t
context: common
lang: common
rules:
"""


def write_rules(path, body):
    path.write_text(HEADER + body, encoding="utf-8")
    return path


@pytest.fixture
def registry():
    return RegistryAdapter.from_directory(SNAPSHOT)


@pytest.fixture(autouse=True)
def _clear():
    clear_adapter_cache()
    yield
    clear_adapter_cache()


def load(tmp_path, body, registry=None, detectors="off"):
    processor = RulesProcessor()
    processor.import_rules(str(write_rules(tmp_path / "r.yaml", body)))
    if registry is not None:
        processor.bind_registry(registry, detectors=detectors)
    return processor


def scan(processor, rows, **kwargs):
    kwargs.setdefault("confidence", 5)
    result = processor.match_dict(rows, **kwargs)
    return {r.field: r.matches for r in result.results}


def test_schema_error_rule_is_skipped(tmp_path):
    processor = load(tmp_path, """
  bad:
    key: email
    match: text
    type: field
    rule: email
    is_pii: "yes"
""")
    assert not processor.field_rules
    assert any(d["ruleid"] == "bad" and d["error"].startswith("Schema:") for d in processor.diagnostics)


def test_rules_validate_command(tmp_path):
    from metacrafter.cli import commands  # noqa: F401
    from metacrafter.core import app

    write_rules(tmp_path / "r.yaml", """
  bad:
    key: email
    match: wrong
    type: field
    rule: email
""")
    result = CliRunner().invoke(app, ["rules", "validate", str(tmp_path)])
    assert result.exit_code == 1
    assert "bad" in result.output


def test_regex_data_rule(tmp_path):
    processor = load(tmp_path, """
  docnum:
    key: doc_number
    match: regex
    type: data
    rule: '[A-Z]{2}\\d{6}'
""")
    matches = scan(processor, [{"x": "AB123456"}, {"x": "CD654321"}], parse_dates=False)["x"]
    assert matches[0].dataclass == "doc_number"
    assert matches[0].confidence == 100


def test_deprecated_rule_skipped_unless_included(tmp_path):
    body = """
  old:
    key: email
    match: text
    type: field
    rule: mail
    deprecated: true
"""
    processor = load(tmp_path, body)
    assert scan(processor, [{"mail": "a"}], parse_dates=False)["mail"] == []
    assert scan(processor, [{"mail": "a"}], parse_dates=False, include_deprecated=True)["mail"]


def test_validator_applies_to_text_rule(tmp_path, monkeypatch):
    import types, sys

    module = types.ModuleType("mc_test_validators")
    module.only_red = lambda value: value == "red"
    monkeypatch.setitem(sys.modules, "mc_test_validators", module)
    processor = load(tmp_path, """
  colors:
    key: color
    match: text
    type: data
    rule: red,green
    validator: mc_test_validators.only_red
""")
    matches = scan(processor, [{"c": "red"}, {"c": "green"}], parse_dates=False)["c"]
    assert matches[0].confidence == 50
    assert matches[0].evidence["checksum_rate"] == 0.5


def test_field_only_match_is_capped_and_legacy_restores_100(tmp_path):
    processor = load(tmp_path, """
  emailname:
    key: email
    match: text
    type: field
    rule: contact_email
    priority: 1
""")
    rows = [{"contact_email": "x"}]
    assert scan(processor, rows, parse_dates=False)["contact_email"][0].confidence == 60
    assert scan(processor, rows, parse_dates=False, legacy_scoring=True)["contact_email"][0].confidence == 100


def test_priority_one_sorts_first(tmp_path):
    processor = load(tmp_path, """
  weak:
    key: a
    match: text
    type: field
    rule: f
    priority: 3
  strong:
    key: b
    match: text
    type: field
    rule: f
    priority: 1
""")
    assert [r["id"] for r in processor.field_rules] == ["strong", "weak"]


def test_registry_rule_key_mismatch_is_disabled(tmp_path, registry):
    processor = load(tmp_path, """
  wrong:
    key: email
    match: registry
    type: data
    rule: iban
""", registry=registry)
    assert not processor.data_rules
    assert any(d["error"] == "Rule key does not match the registry record" for d in processor.diagnostics)


def test_registry_rule_uses_regexp_and_checksum(tmp_path, registry):
    processor = load(tmp_path, """
  ibanbyregistry:
    key: iban
    match: registry
    type: data
    rule: iban
""", registry=registry)
    good = scan(processor, [{"a": "GB82WEST12345698765432"}, {"a": "DE89370400440532013000"}], parse_dates=False)["a"]
    assert good[0].dataclass == "iban" and good[0].confidence == 100
    assert good[0].evidence["checksum_rate"] == 1.0
    bad = scan(processor, [{"a": "GB82WEST12345698765433"}], parse_dates=False)["a"]
    assert bad == []


def test_checksum_validator(tmp_path):
    processor = load(tmp_path, """
  card:
    key: card
    match: regex
    type: data
    rule: '\\d{16}'
    validator: checksum:luhn
""")
    matches = scan(processor, [{"n": "4111111111111111"}, {"n": "4111111111111112"}], parse_dates=False)["n"]
    assert matches[0].confidence == 50


def test_unknown_checksum_disables_rule(tmp_path):
    processor = load(tmp_path, """
  card:
    key: card
    match: regex
    type: data
    rule: '\\d{16}'
    validator: checksum:no_such_algorithm
""")
    assert not processor.data_rules
    assert any(d["error"] == "Required validator unavailable" for d in processor.diagnostics)


def test_registry_detector_finds_uncovered_stable_type(tmp_path, registry):
    processor = load(tmp_path, """
  placeholder:
    key: email
    match: text
    type: field
    rule: email
""", registry=registry, detectors="stable")
    ids = {r["id"] for r in processor.data_rules}
    assert "registry:inn" in ids
    assert "registry:date_dmy" not in ids  # draft pattern, not a stable datatype
    matches = scan(processor, [{"x": "7707083893"}, {"x": "7728168971"}], parse_dates=False)["x"]
    inn = [m for m in matches if m.dataclass == "inn"]
    assert inn and inn[0].ruletype == "registry"


def test_registry_detectors_off(tmp_path, registry):
    processor = load(tmp_path, """
  placeholder:
    key: email
    match: text
    type: field
    rule: email
""", registry=registry, detectors="off")
    assert not [r for r in processor.data_rules if r["id"].startswith("registry:")]


def test_qddate_crosswalk(registry):
    assert registry.qddate_pattern("dt:date:date_2") == "date_dmy"
    assert registry.qddate_pattern("dt:date:date_9") == "date"


def test_scan_report_states_scoring_model_and_detectors():
    from metacrafter.core import CrafterCmd

    report = CrafterCmd(quiet=True, registry_detectors="off").scan_data([{"a": "x"}], parse_dates=False)
    assert report["scoring_model"] == "v1"
    assert report["registry_detectors"] == {"mode": "off", "count": 0}


def _scan_with_stats(processor, rows):
    from metacrafter.classify.stats import Analyzer
    from metacrafter.reports import statistics_dict

    stats = statistics_dict(Analyzer().analyze(fromfile=None, itemlist=rows, options={}))
    result = processor.match_dict(rows, datastats=stats, confidence=5, parse_dates=False)
    return {r.field: r.matches for r in result.results}, stats


THIRTEEN_DIGITS = """
  thirteen:
    key: thirteen
    match: regex
    type: data
    rule: '\\d{13}'
"""


def test_whole_number_float_column_is_matched_as_integers(tmp_path):
    # Identifiers read as floats (.xls, pandas with NaN) still match their integer rules.
    processor = load(tmp_path, THIRTEEN_DIGITS)
    rows = [{"x": 1027700043502.0}, {"x": 1027700132195.0}, {"x": float("nan")}]
    matches, stats = _scan_with_stats(processor, rows)
    assert stats["x"]["ftype"] == "float"
    assert [m.dataclass for m in matches["x"]] == ["thirteen"]


def test_float_text_column_of_whole_numbers_is_matched(tmp_path):
    processor = load(tmp_path, THIRTEEN_DIGITS)
    rows = [{"x": "1027700043502.0"}, {"x": "1027700132195.0"}]
    matches, stats = _scan_with_stats(processor, rows)
    assert stats["x"]["ftype"] == "float"
    assert [m.dataclass for m in matches["x"]] == ["thirteen"]


def test_fractional_float_column_is_not_matched(tmp_path):
    processor = load(tmp_path, THIRTEEN_DIGITS)
    rows = [{"x": 1027700043502.5}, {"x": 1027700132195.0}]
    matches, _ = _scan_with_stats(processor, rows)
    assert matches["x"] == []


@pytest.mark.parametrize("values, expected", [
    ([1.0, 2.0, None, float("nan"), "", 3], ["1", "2", "", "", "", "3"]),
    (["1027700043502.0", " 5.0 ", "N/A"], ["1027700043502", "5", ""]),
    ([1.5, 2.0], None),
    ([2.0 ** 53], None),
    (["abc", 1.0], None),
    ([True, 1.0], None),
    ([None, float("nan")], None),
])
def test_whole_number_texts(values, expected):
    from metacrafter.classify.utils import whole_number_texts

    assert whole_number_texts(values, empty_values=["N/A"]) == expected
