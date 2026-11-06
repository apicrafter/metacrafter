"""Registry-derived detectors from pattern records (issue #39)."""

import pytest

from metacrafter.classify.processor import RulesProcessor
from metacrafter.engine.background import background_broadness
from metacrafter.registry.adapter import RegistryAdapter, SnapshotRegistry

RECORDS = [
    {"id": "language", "status": "stable", "kind": "datatype", "name": "Language"},
    {"id": "iso639_2", "status": "stable", "kind": "pattern", "semantic_type": "language",
     "name": "ISO 639-2 code", "regexp": "^(?:[a-z]{3}|[A-Z]{3})$"},
    {"id": "iso639_3", "status": "draft", "kind": "pattern", "semantic_type": "language",
     "name": "ISO 639-3 code", "regexp": "^[a-z]{3}$"},
    {"id": "geometry", "status": "stable", "kind": "datatype", "name": "Geometry"},
    {"id": "wkt_point", "status": "stable", "kind": "pattern", "semantic_type": "geometry",
     "name": "WKT point", "regexp": "^POINT ?\\(-?\\d+(\\.\\d+)? -?\\d+(\\.\\d+)?\\)$"},
]


def processor_for(tmp_path, detectors="stable", rules=""):
    registry = SnapshotRegistry(datatypes=RECORDS, aliases={}, manifest={"version": "t"}, source="t")
    path = tmp_path / "r.yaml"
    path.write_text("format_version: 2\nname: t\ndescription: t\ncontext: common\nlang: common\nrules:\n"
                    "  placeholder:\n    key: email\n    match: text\n    type: field\n    rule: email\n" + rules,
                    encoding="utf-8")
    processor = RulesProcessor()
    processor.import_rules(str(path))
    processor.bind_registry(RegistryAdapter._wrap(registry, None, "t"), detectors=detectors)
    return processor


def matches(processor, rows, **kwargs):
    result = processor.match_dict(rows, confidence=5, parse_dates=False, **kwargs)
    return {r.field: r.matches for r in result.results}


def test_pattern_detector_reports_semantic_type_and_pattern(tmp_path):
    processor = processor_for(tmp_path)
    rule = next(r for r in processor.data_rules if r["id"] == "registry:iso639_2")
    assert rule["key"] == "language" and rule["pattern_id"] == "iso639_2"
    found = matches(processor, [{"language": "eng"}, {"language": "rus"}])["language"]
    match = next(m for m in found if m.ruleid == "registry:iso639_2")
    assert (match.dataclass, match.pattern, match.evidence["name_score"]) == ("language", "iso639_2", 1.0)


def test_generic_pattern_needs_the_datatype_in_the_field_name(tmp_path):
    processor = processor_for(tmp_path)
    found = matches(processor, [{"workspace": "geo"}, {"workspace": "dev"}], explain=True)
    assert found["workspace"] == []


def test_distinctive_pattern_needs_no_field_name(tmp_path):
    processor = processor_for(tmp_path)
    found = matches(processor, [{"shape": "POINT (30 10)"}, {"shape": "POINT(1.5 -2)"}])["shape"]
    assert [(m.dataclass, m.pattern) for m in found] == [("geometry", "wkt_point")]


def test_draft_patterns_only_in_all_mode(tmp_path):
    assert "registry:iso639_3" not in {r["id"] for r in processor_for(tmp_path).data_rules}
    rule = next(r for r in processor_for(tmp_path, "all").data_rules if r["id"] == "registry:iso639_3")
    assert rule["imprecise"] is True


def test_pattern_covered_by_a_rule_is_skipped(tmp_path):
    rule = "  isocode:\n    key: iso639_2\n    match: regex\n    type: data\n    rule: '[a-z]{3}'\n"
    ids = {r["id"] for r in processor_for(tmp_path, rules=rule).data_rules}
    assert "registry:iso639_2" not in ids and "isocode" in ids


@pytest.mark.parametrize("regexp,cls", [
    ("^(?:[a-z]{2}|[A-Z]{2})$", "code2"),
    ("^(?:[A-Z]{2,3}|[A-Z][0-9]|[0-9][A-Z])\\d{1,4}[A-Z]?$", "code_num"),
])
def test_short_code_classes(regexp, cls):
    share, name = background_broadness(regexp)
    assert share >= 0.3 and name == cls
