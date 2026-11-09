"""Values that veto field-name labels and name-backed rules over detectors
(metacrafter-rules #60, #61, #63).

- A field rule with a ``validator`` keeps its match only when at least half of the non-empty
  sampled values pass the validator.
- A column of JSON booleans gets ``boolean`` only, whatever its name says.
- A registry detector without field-name evidence is dropped when a value rule whose field
  rule names the field matched it.
"""
import pytest

from metacrafter.classify.processor import RulesProcessor

HEADER = """
format_version: 2
name: t
description: t
context: common
lang: common
rules:
"""

RULES = r"""
  orgnamebyname:
    key: orgname
    match: text
    type: field
    rule: organization,host_organization
    validator: tests.test_rules_testdata_60_61.has_letters
    priority: 1
  categorybyname:
    key: category
    match: text
    type: field
    rule: status,type
    priority: 1
  addresspointbyname:
    key: cz_ruian_address_point
    match: text
    type: field
    rule: adresnibod
    priority: 1
  addresspointvalue:
    key: cz_ruian_address_point
    match: regex
    type: data
    rule: '\d{1,9}'
    fieldrule: adresnibod
    fieldrulematch: text
    priority: 1
"""


def has_letters(value):
    return any(ch.isalpha() for ch in value) and not value.startswith("http")


@pytest.fixture
def processor(tmp_path):
    path = tmp_path / "r.yaml"
    path.write_text(HEADER + RULES, encoding="utf-8")
    proc = RulesProcessor()
    proc.import_rules(str(path))
    return proc


def labels(processor, rows, **kwargs):
    result = processor.match_dict(rows, confidence=50, **kwargs)
    return {r.field: sorted({m.dataclass for m in r.matches}) for r in result.results}


def test_field_rule_validator_drops_names_of_codes(processor):
    rows = [{"organization": str(1 + i % 5)} for i in range(20)]
    assert labels(processor, rows)["organization"] == []


def test_field_rule_validator_drops_url_values(processor):
    rows = [{"host_organization": f"https://openalex.org/P43103{i:05d}"} for i in range(20)]
    assert labels(processor, rows)["host_organization"] == []


def test_field_rule_validator_keeps_names(processor):
    rows = [{"organization": f"Company {i}"} for i in range(20)]
    rows[0]["organization"] = "1"  # one code among names
    assert labels(processor, rows)["organization"] == ["orgname"]


def test_field_rule_validator_explains_the_rejection(processor):
    rows = [{"organization": "1"}, {"organization": "2"}]
    result = processor.match_dict(rows, confidence=50, explain=True)
    rejected = result.results[0].rejected
    assert any(r["ruleid"] == "orgnamebyname" and r["reason"] == "Values do not fit the field-name rule"
               for r in rejected)


def test_rules_without_validator_keep_name_matches(processor):
    rows = [{"type": str(i)} for i in range(5)]
    assert labels(processor, rows)["type"] == ["category"]


def test_boolean_column_is_not_a_category(processor):
    rows = [{"status": i % 7 != 0} for i in range(20)]
    stats = {"status": {"ftype": "bool", "minlen": 4, "maxlen": 5}}
    assert labels(processor, rows, datastats=stats)["status"] == ["boolean"]


def test_named_value_rule_drops_an_unnamed_detector(processor):
    detector = {
        "id": "registry:cz_ico", "key": "cz_ico", "type": "data", "match": "regex", "derived": True,
        "registry_country": ["CZ"], "minlen": 8, "maxlen": 8, "priority": 1, "context": ["common"],
        "lang": "common", "imprecise": False, "deprecated": False, "rule": r"\d{8}",
        "compiled": __import__("re").compile(r"\d{8}"), "vfunc": lambda value: True, "country_code": [],
    }
    processor.register_rule(detector)
    rows = [{"AdresniBod": str(17242606 + i), "kod": str(17242606 + i)} for i in range(20)]
    found = labels(processor, rows)
    assert found["AdresniBod"] == ["cz_ruian_address_point"]
    # Without a name for another type the detector reports the values as before.
    assert found["kod"] == ["cz_ico"]


def test_unfit_universal_rule_does_not_cover_a_country_rule(tmp_path):
    universal = tmp_path / "u.yaml"
    universal.write_text(HEADER + r"""
  provincebyname:
    key: province
    match: text
    type: field
    rule: provincia
    validator: tests.test_rules_testdata_60_61.is_long_name
    priority: 1
""", encoding="utf-8")
    country = tmp_path / "it.yaml"
    country.write_text(HEADER.replace("lang: common", "lang: it\ncountry_code: it") + r"""
  itprovincebyname:
    key: it_province_code
    match: text
    type: field
    rule: provincia
    priority: 1
""", encoding="utf-8")
    proc = RulesProcessor()
    proc.import_rules(str(universal))
    proc.import_rules(str(country))
    # Province codes (MI, BG) do not fit the universal rule; the Italian rule keeps the field.
    assert labels(proc, [{"provincia": code} for code in ("MI", "BG", "BS", "CO")])["provincia"] == ["it_province_code"]
    assert labels(proc, [{"provincia": name} for name in ("Milano", "Bergamo")])["provincia"] == ["province"]


def is_long_name(value):
    return len(value) > 2 and value.isalpha()


def test_country_rule_of_an_unfit_type_is_dropped_too(tmp_path):
    universal = tmp_path / "u.yaml"
    universal.write_text(HEADER + r"""
  citybyname:
    key: city
    match: text
    type: field
    rule: city
    validator: tests.test_rules_testdata_60_61.is_long_name
    priority: 1
""", encoding="utf-8")
    country = tmp_path / "am.yaml"
    country.write_text(HEADER.replace("lang: common", "lang: hy\ncountry_code: am") + r"""
  amcitybyname:
    key: city
    match: text
    type: field
    rule: city
    priority: 1
""", encoding="utf-8")
    proc = RulesProcessor()
    proc.import_rules(str(universal))
    proc.import_rules(str(country))
    # Numeric city ids (HayPost) are no city names for the Armenian rule either.
    assert labels(proc, [{"city": str(n)} for n in (1, 335, 470, 652)])["city"] == []
    assert labels(proc, [{"city": name} for name in ("Yerevan", "Gyumri")])["city"] == ["city"]
