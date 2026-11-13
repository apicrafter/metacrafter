"""Field-name matches scored by how well the values fit (#111); geopoint on geometry."""

import pytest

from metacrafter.classify.processor import RulesProcessor, fit_texts, split_field_path
from metacrafter.engine.scoring import field_score
from metacrafter.registry.adapter import RegistryAdapter, SnapshotRegistry

HEADER = "format_version: 2\nname: t\ndescription: t\ncontext: common\nlang: common\nrules:\n"

YEAR_RULES = """
  yearname:
    key: year
    match: text
    type: field
    rule: year,tahun
    priority: 1
  anyfourdigits:
    key: year
    match: ppr
    type: data
    rule: Word(nums, exact=4)
    imprecise: true
"""

MODERN_YEAR = """
  modernyear:
    key: year
    match: regex
    type: data
    rule: (19|20)\\d{2}
"""


def processor_with(tmp_path, body, adapter=None):
    path = tmp_path / "rules.yaml"
    path.write_text(HEADER + body, encoding="utf-8")
    processor = RulesProcessor()
    processor.import_rules(str(path))
    if adapter is not None:
        processor.bind_registry(adapter, detectors="off")
    return processor


def matches(processor, rows, explain=False):
    result = processor.match_dict(rows, confidence=5, parse_dates=False, explain=explain)
    return {r.field: r for r in result.results}


def field_match(result, key):
    return next(m for m in result.matches if m.ruletype == "field" and m.dataclass == key)


def test_values_that_fail_even_a_broad_check_lower_the_name_match(tmp_path):
    # Word positions of an inverted abstract index under the key "year" (metacrafter-rules #69).
    processor = processor_with(tmp_path, YEAR_RULES)
    result = matches(processor, [{"year": v} for v in (58, 226, 0, 12)])["year"]
    match = field_match(result, "year")
    fit = match.evidence["value_fit"]
    assert fit["misfit"] == 1.0 and fit["support"] == 0.0
    # At the default threshold of 50 still; --confidence 60 drops it.
    assert match.confidence == 50.0


def test_a_broad_check_that_passes_proves_little(tmp_path):
    processor = processor_with(tmp_path, YEAR_RULES)
    match = field_match(matches(processor, [{"tahun": v} for v in (2001, 2014, 2023)])["tahun"], "year")
    assert match.evidence["value_fit"]["support"] == 0.0
    assert match.confidence == 60.0


def test_a_specific_check_that_passes_raises_the_name_match(tmp_path):
    processor = processor_with(tmp_path, YEAR_RULES + MODERN_YEAR)
    rows = [{"tahun": v} for v in (2001.0, 2014.0, 2023.0)]  # whole floats are years as text
    match = field_match(matches(processor, rows)["tahun"], "year")
    fit = match.evidence["value_fit"]
    assert fit["check"] == "modernyear" and fit["rate"] == 1.0 and fit["broadness"] < 0.1
    assert match.confidence > 85


def test_dictionary_misses_do_not_count_against_a_name(tmp_path):
    processor = processor_with(tmp_path, """
  surname:
    key: person_surname
    match: text
    type: field
    rule: surname
    priority: 1
  knownsurnames:
    key: person_surname
    match: text
    type: data
    rule: smith,jones,brown
""")
    rows = [{"surname": v} for v in ("Smith", "Okonkwo", "Abernathy", "Nakamura")]
    match = field_match(matches(processor, rows)["surname"], "person_surname")
    fit = match.evidence["value_fit"]
    assert fit["misfit"] == 0.0 and fit["rate"] == 0.25 and "shape_rate" not in fit
    assert 60.0 < match.confidence < 70.0


def test_name_match_without_checks_keeps_its_score(tmp_path):
    processor = processor_with(tmp_path, """
  emailname:
    key: email
    match: text
    type: field
    rule: contact_email
    priority: 1
""")
    match = field_match(matches(processor, [{"contact_email": "x"}])["contact_email"], "email")
    assert "value_fit" not in match.evidence and match.confidence == 60.0
    # Without values there is nothing to fit either.
    match = field_match(matches(processor, [{"contact_email": None}])["contact_email"], "email")
    assert match.confidence == 60.0


@pytest.fixture
def language_registry():
    records = [{"id": "languagetag", "status": "stable", "kind": "datatype", "name": "Language tag",
                "regexp": "^[a-z]{2,3}$"}]
    registry = SnapshotRegistry(datatypes=records, aliases={}, manifest={"version": "test"}, source="test")
    return RegistryAdapter._wrap(registry, None, "test")


LANGUAGE_FIELD = """
  langname:
    key: languagetag
    match: text
    type: field
    rule: languagecode
    priority: 1
"""


def test_registry_regexp_checks_the_values(tmp_path, language_registry):
    processor = processor_with(tmp_path, LANGUAGE_FIELD, language_registry)
    codes = field_match(matches(processor, [{"languageCode": v} for v in ("fi", "sv", "en")])["languageCode"],
                        "languagetag")
    numbers = field_match(matches(processor, [{"languageCode": v} for v in ("1", "2", "3")])["languageCode"],
                          "languagetag")
    assert codes.evidence["value_fit"]["check"] == "registry:languagetag"
    # The regexp passes about half of the two-letter background codes (the lower-case ones):
    # some support for codes, half a misfit for numbers.
    broadness = codes.evidence["value_fit"]["broadness"]
    assert 0.4 < broadness < 0.6
    assert codes.confidence == pytest.approx(60 + 30 * (1 - broadness), abs=0.01)
    assert numbers.evidence["value_fit"]["misfit"] == broadness
    assert numbers.confidence == pytest.approx(60 - 10 * broadness, abs=0.01)


def test_legacy_scoring_ignores_the_values(tmp_path):
    processor = processor_with(tmp_path, YEAR_RULES)
    result = processor.match_dict([{"year": 58}], confidence=5, parse_dates=False, legacy_scoring=True)
    assert field_match(result.results[0], "year").confidence == 100


def test_field_score_formula():
    assert field_score(1) == 60.0 and field_score(3) == 36.0
    assert field_score(1, fit={"support": 1.0, "misfit": 0.0}) == 90.0
    assert field_score(3, fit={"support": 0.0, "misfit": 1.0}) == 26.0
    assert field_score(1, legacy=True, fit={"misfit": 1.0}) == 100.0


def test_fit_helpers():
    assert fit_texts([2023.0, "2023", " x ", None, "", True, float("nan"), 1.5], [None, ""]) == {
        "2023": 2, "x": 1, "1.5": 1}
    assert split_field_path("geometry.coordinates") == ("geometry", "coordinates")
    assert split_field_path("a\\.b.c") == ("a\\.b", "c")
    assert split_field_path("a\\\\.b") == ("a\\\\", "b")
    assert split_field_path("coordinates") == ("", "coordinates")


# ---------------------------------------------------------------- geopoint on geometry (rules #69)
GEOPOINT_FIELD = """
  coordinatesname:
    key: geopoint
    match: text
    type: field
    rule: coordinates
    priority: 1
"""


def feature(kind, coordinates):
    return {"type": "Feature", "properties": {"name": "x"}, "geometry": {"type": kind, "coordinates": coordinates}}


def geopoint_labels(tmp_path, rows):
    # Coordinates are a float column: the float fields skip value rules, not this check.
    stats = {"geometry.coordinates": {"ftype": "float", "minlen": 1, "maxlen": 20}}
    result = processor_with(tmp_path, GEOPOINT_FIELD).match_dict(
        rows, datastats=stats, confidence=5, parse_dates=False, explain=True)
    result = {r.field: r for r in result.results}["geometry.coordinates"]
    return [m.dataclass for m in result.matches], [r["reason"] for r in result.rejected]


def test_geopoint_kept_on_longitude_latitude_points(tmp_path):
    rows = [feature("Point", [24.1 + i / 100, 56.9, 12.0]) for i in range(5)]
    assert geopoint_labels(tmp_path, rows)[0] == ["geopoint"]


def test_geopoint_dropped_on_polygons_and_lines(tmp_path):
    ring = [[[4.3, 50.8], [4.4, 50.8], [4.4, 50.9], [4.3, 50.8]]]
    rows = [feature("Polygon", ring), feature("MultiPolygon", [ring]), feature("Point", [4.3, 50.8])]
    labels, reasons = geopoint_labels(tmp_path, rows)
    assert labels == [] and "Coordinates of lines or polygons, not of points" in reasons
    labels, _ = geopoint_labels(tmp_path, [feature("LineString", [[27.1, 38.4], [27.2, 38.5]])])
    assert labels == []
    # Mostly points, with a few building outlines: still points.
    rows = [feature("Point", [44.8 + i / 100, 41.7]) for i in range(9)] + [feature("Polygon", ring)]
    assert geopoint_labels(tmp_path, rows)[0] == ["geopoint"]


def test_geopoint_dropped_on_projected_points(tmp_path):
    rows = [feature("Point", [570181.18 + i, 6592327.82]) for i in range(5)]
    labels, reasons = geopoint_labels(tmp_path, rows)
    assert labels == [] and "Projected coordinates, outside the longitude / latitude range" in reasons
