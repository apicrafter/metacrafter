"""Precise scan defaults: confidence 50, checksum-validated registry detectors only."""
from pathlib import Path

import pytest

from metacrafter.classify.processor import RulesProcessor
from metacrafter.config import MetacrafterConfig
from metacrafter.registry.adapter import RegistryAdapter, clear_adapter_cache
from metacrafter.scan_service import DEFAULT_REGISTRY_DETECTORS, MIN_CONFIDENCE_FOR_MATCH

SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"

RULES = """
format_version: 2
name: t
description: t
context: common
lang: common
rules:
  createdname:
    key: date
    match: text
    type: field
    rule: created
"""


@pytest.fixture(autouse=True)
def _clear():
    clear_adapter_cache()
    yield
    clear_adapter_cache()


def _processor(tmp_path, detectors):
    path = tmp_path / "r.yaml"
    path.write_text(RULES, encoding="utf-8")
    processor = RulesProcessor()
    processor.import_rules(str(path))
    processor.bind_registry(RegistryAdapter.from_directory(SNAPSHOT), detectors=detectors)
    return processor


def _derived(processor):
    return {r["registry_id"] for r in processor.data_rules if r.get("derived")}


def test_defaults():
    assert MIN_CONFIDENCE_FOR_MATCH == 50.0
    assert DEFAULT_REGISTRY_DETECTORS == "checksum"
    assert MetacrafterConfig().registry_detectors == "checksum"
    assert MetacrafterConfig(registry_detectors="stable").registry_detectors == "stable"


def test_checksum_mode_keeps_only_checksum_validated_detectors(tmp_path):
    checksum = _derived(_processor(tmp_path, "checksum"))
    stable = _derived(_processor(tmp_path, "stable"))
    assert {"inn", "iban"} <= checksum
    # Stable records without a checksum (generic or format-only regexps) need --registry-detectors stable.
    assert not checksum & {"email", "uuid", "date", "datetime", "md5hash"}
    assert {"date", "uuid"} <= stable
    assert checksum < stable


def test_checksum_detector_reports_validated_values(tmp_path):
    processor = _processor(tmp_path, "checksum")
    rows = [{"x": "7707083893"}, {"x": "7728168971"}]
    result = processor.match_dict(rows, confidence=MIN_CONFIDENCE_FOR_MATCH, parse_dates=False)
    matches = {r.field: r.matches for r in result.results}["x"]
    assert [m.ruleid for m in matches] == ["registry:inn"]


def test_date_patterns_run_on_a_field_with_only_a_name_match(tmp_path):
    import qddate

    processor = _processor(tmp_path, "off")
    dateparser = qddate.DateParser(patterns=qddate.patterns.PATTERNS_EN)
    rows = [{"created": "2025-05-15"}, {"created": "1993-12-30"}, {"created": "2024-01-02"}]
    result = processor.match_dict(rows, confidence=MIN_CONFIDENCE_FOR_MATCH, dateparser=dateparser)
    matches = {r.field: r.matches for r in result.results}["created"]
    assert {(m.ruleid, m.ruletype) for m in matches} == {("createdname", "field"), ("qddate", "data")}


def test_date_patterns_skip_a_field_with_a_value_match(tmp_path):
    import qddate

    processor = _processor(tmp_path, "stable")
    dateparser = qddate.DateParser(patterns=qddate.patterns.PATTERNS_EN)
    rows = [{"created": "2025-05-15"}, {"created": "1993-12-30"}, {"created": "2024-01-02"}]
    result = processor.match_dict(rows, confidence=MIN_CONFIDENCE_FOR_MATCH, dateparser=dateparser)
    ruleids = {m.ruleid for m in {r.field: r.matches for r in result.results}["created"]}
    assert "registry:date" in ruleids and "qddate" not in ruleids


def test_default_confidence_drops_partial_value_matches():
    from metacrafter.core import CrafterCmd

    service = CrafterCmd(quiet=True)
    rows = [{"note": "alice@example.com"}] + [{"note": text} for text in ("call back", "paid", "n/a today")]

    def datatypes(**options):
        report = service.scan_data(rows, parse_dates=False, **options)
        field = next(f for f in report["data"] if f["field"] == "note")
        return {m["datatype"] for m in field["matches"]}

    assert report_mode(service) == "checksum"
    assert "email" not in datatypes()
    assert "email" in datatypes(confidence=5)


def report_mode(service):
    return service.scan_data([{"a": "x"}], parse_dates=False)["registry_detectors"]["mode"]
