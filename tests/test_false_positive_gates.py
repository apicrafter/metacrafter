"""False-positive gates: generic registry patterns, country-specific types, name variants."""

import pytest

from metacrafter.classify.processor import RulesProcessor
from metacrafter.engine.background import background_broadness, is_generic
from metacrafter.engine.registry_detectors import detector_name_evidence
from metacrafter.registry.adapter import RegistryAdapter, SnapshotRegistry

UUID_RE = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
RECORDS = [
    # A real registry regexp that accepts any 1-4 digit number.
    {"id": "flightnumber", "status": "stable", "kind": "datatype", "name": "Flight number",
     "regexp": "([A-Z]{2,3}|[A-Z][0-9]|[0-9][A-Z])\\d{1,4}[A-Z]?|\\d{1,4}"},
    {"id": "am_postal_code", "status": "stable", "kind": "datatype", "name": "Armenian postal code",
     "regexp": "\\d{4}", "country": ["AM"]},
    {"id": "tld", "status": "stable", "kind": "datatype", "name": "Top-level domain",
     "regexp": "^\\.?(?:[A-Za-z]{2,63}|[Xx][Nn]--[A-Za-z0-9-]{1,59})$"},
    {"id": "de_tax_code", "status": "stable", "kind": "datatype", "name": "German tax code",
     "regexp": "DE\\d{9}", "country": ["DE"]},
]
ALIASES = {"domain_zone": "tld"}

PLACEHOLDER = """
  placeholder:
    key: email
    match: text
    type: field
    rule: email
"""


def ruleset(body, country=None):
    head = "format_version: 2\nname: t\ndescription: t\ncontext: common\nlang: common\n"
    if country:
        head += f"country_code: {country}\n"
    return head + "rules:\n" + body


@pytest.fixture
def adapter():
    registry = SnapshotRegistry(datatypes=RECORDS, aliases=ALIASES, manifest={"version": "test"}, source="test")
    return RegistryAdapter._wrap(registry, None, "test")


def processor_with(tmp_path, files, adapter=None, countries=None):
    processor = RulesProcessor(countries=countries)
    for index, (body, country) in enumerate(files):
        path = tmp_path / f"r{index}.yaml"
        path.write_text(ruleset(body, country), encoding="utf-8")
        processor.import_rules(str(path))
    if adapter is not None:
        processor.bind_registry(adapter, detectors="stable")
    return processor


def scan(processor, rows, explain=False):
    result = processor.match_dict(rows, confidence=5, parse_dates=False, explain=explain)
    return {r.field: r for r in result.results}


def classes(column):
    return {m.dataclass for m in column.matches}


# --- background broadness ---------------------------------------------------

@pytest.mark.parametrize("regexp", ["\\d{1,4}", "[1234]", RECORDS[2]["regexp"], "[A-Z]{2}[A-Z0-9]{0,3}"])
def test_generic_patterns(regexp):
    assert is_generic(regexp)


@pytest.mark.parametrize("regexp", [UUID_RE, "[A-Z]{2}\\d{2}[A-Z0-9]{11,30}", "DE\\d{9}", "[a-z0-9.]+@[a-z0-9.]+"])
def test_distinctive_patterns(regexp):
    assert not is_generic(regexp)


def test_background_broadness_reports_class():
    assert background_broadness("\\d{4}") == (1.0, "int4")
    assert background_broadness("(") == (0.0, None)


@pytest.mark.parametrize("regexp,cls", [
    # Scopus author id: any 10-12 digit number (phones, tax ids, account numbers).
    ("^[1-9]\\d{9,11}$", "int10"),
    # UIC station code: most 7-8 digit numbers.
    ("^(?:10|2\\d|3[0-3]|4[01249]|5\\d|6[0125678]|7\\d|8[0-8]|9\\d)\\d{5,6}$", "int7"),
    ("^\\d{20}$", "int20"),
])
def test_long_digit_patterns_are_generic(regexp, cls):
    share, name = background_broadness(regexp)
    assert share >= 0.3 and name == cls


def test_long_digit_detector_needs_field_name_evidence(tmp_path):
    record = {"id": "author_number", "status": "stable", "kind": "datatype", "name": "Author number",
              "regexp": "^[1-9]\\d{9,11}$"}
    registry = SnapshotRegistry(datatypes=[record], aliases={}, manifest={"version": "test"}, source="test")
    processor = processor_with(tmp_path, [(PLACEHOLDER, None)], RegistryAdapter._wrap(registry, None, "test"))
    detector = next(r for r in processor.data_rules if r["id"] == "registry:author_number")
    assert detector["needs_name_evidence"]
    rows = [{"phone": str(79107866363 + n), "author_number": str(7004212771 + n)} for n in range(20)]
    columns = scan(processor, rows)
    assert "author_number" not in classes(columns["phone"])
    assert "author_number" in classes(columns["author_number"])


# --- length prefilter -------------------------------------------------------

LONGCODE = """
  longcode:
    key: longcode
    match: regex
    type: data
    rule: '\\d{13}'
    minlen: 13
    maxlen: 13
"""


def test_missing_nested_value_keeps_long_rules(tmp_path):
    """A record without the nested object (a null in the column) keeps rules longer than 4."""
    processor = processor_with(tmp_path, [(LONGCODE, None)])
    rows = [{"org": {"code": "1027700132195"}}, {"org": {"code": "1037739010891"}}, {"org": {"code": ""}}]
    assert "longcode" in classes(scan(processor, rows)["org.code"])
    assert "longcode" in classes(scan(processor, rows + [{"other": 1}])["org.code"])


# --- generic registry detectors ---------------------------------------------

def test_generic_detector_needs_field_name_evidence(tmp_path, adapter):
    processor = processor_with(tmp_path, [(PLACEHOLDER, None)], adapter)
    detector = next(r for r in processor.data_rules if r["id"] == "registry:flightnumber")
    assert detector["needs_name_evidence"] and detector["background_class"] == "int1"
    rows = [{"layers_count": str(n), "flight_number": str(1000 + n)} for n in range(1, 30)]
    columns = scan(processor, rows, explain=True)
    assert "flightnumber" not in classes(columns["layers_count"])
    assert {"ruleid": "registry:flightnumber", "reason": "Generic pattern without field-name evidence"}.items() <= next(
        r for r in columns["layers_count"].rejected if r["ruleid"] == "registry:flightnumber"
    ).items()
    flight = [m for m in columns["flight_number"].matches if m.dataclass == "flightnumber"]
    assert flight and flight[0].evidence["name_score"] == 1.0


def test_generic_word_detector_ignores_plain_words(tmp_path, adapter):
    processor = processor_with(tmp_path, [(PLACEHOLDER, None)], adapter)
    rows = [{"workspace": "geonode", "domain_zone": "com"}, {"workspace": "local", "domain_zone": "org"}]
    columns = scan(processor, rows)
    assert "tld" not in classes(columns["workspace"])
    assert "tld" in classes(columns["domain_zone"])  # registry alias names the type


def test_name_evidence_skips_stop_tokens_and_country_codes(tmp_path, adapter):
    processor = processor_with(tmp_path, [(PLACEHOLDER, None)], adapter)
    rule = next(r for r in processor.data_rules if r["id"] == "registry:am_postal_code")
    assert rule["name_keys"] == [("postal",)]
    assert detector_name_evidence(rule, "postal_code")
    assert detector_name_evidence(rule, "postalCode")
    assert detector_name_evidence(rule, "postalindex")
    assert not detector_name_evidence(rule, "code")
    assert not detector_name_evidence(rule, "am_id")


def test_year_column_is_not_a_postal_code(tmp_path, adapter):
    processor = processor_with(tmp_path, [(PLACEHOLDER, None)], adapter, countries=["am"])
    columns = scan(processor, [{"edition": "2020"}, {"edition": "2021"}])
    assert "am_postal_code" not in classes(columns["edition"])
    columns = scan(processor, [{"postal_code": "0001"}, {"postal_code": "3101"}])
    assert "am_postal_code" in classes(columns["postal_code"])


# --- country-specific registry detectors ------------------------------------

def test_country_detector_without_checksum_needs_country(tmp_path, adapter):
    rows = [{"x": "DE123456789"}, {"x": "DE987654321"}]
    unknown = processor_with(tmp_path, [(PLACEHOLDER, None)], adapter)
    assert "de_tax_code" not in classes(scan(unknown, rows)["x"])
    other = processor_with(tmp_path, [], adapter, countries=["fr"])
    assert "de_tax_code" not in classes(scan(other, rows)["x"])
    same = processor_with(tmp_path, [], adapter, countries=["de"])
    assert "de_tax_code" in classes(scan(same, rows)["x"])


def test_country_detector_kept_with_field_name_evidence(tmp_path, adapter):
    processor = processor_with(tmp_path, [(PLACEHOLDER, None)], adapter)
    rows = [{"de_tax_code": "DE123456789"}, {"de_tax_code": "DE987654321"}]
    assert "de_tax_code" in classes(scan(processor, rows)["de_tax_code"])


# --- curated rules ----------------------------------------------------------

CITY = """
  cityfield:
    key: city
    match: text
    type: field
    rule: city
    priority: 1
"""
EGCITY = """
  egcityfield:
    key: egcity
    match: text
    type: field
    rule: city
    priority: 3
"""
AUNAME = """
  aupersonname:
    key: person_name
    match: text
    type: field
    rule: full_name
    priority: 1
"""
RUNAME = """
  rufullname:
    key: rusfullname
    match: text
    type: field
    rule: full_name
    priority: 3
"""


def test_country_name_variant_dropped_when_universal_rule_matches(tmp_path):
    processor = processor_with(tmp_path, [(CITY, "xx"), (EGCITY, "eg")])
    columns = scan(processor, [{"city": ""}], explain=True)
    assert classes(columns["city"]) == {"city"}
    assert any(r["ruleid"] == "egcityfield" for r in columns["city"].rejected)


def test_weaker_country_name_variant_dropped(tmp_path):
    processor = processor_with(tmp_path, [(AUNAME, "au"), (RUNAME, "ru")])
    assert classes(scan(processor, [{"full_name": "x"}])["full_name"]) == {"person_name"}


def test_country_name_variant_kept_alone(tmp_path):
    processor = processor_with(tmp_path, [(EGCITY, "eg")])
    assert classes(scan(processor, [{"city": "Cairo"}])["city"]) == {"egcity"}


UUIDRULE = f"""
  uuidbyvalue:
    key: uuid
    match: regex
    type: data
    rule: '{UUID_RE}'
"""
FIAS = f"""
  rufiasvalue:
    key: rufias
    match: regex
    type: data
    rule: '{UUID_RE}'
  rufiasfield:
    key: rufias
    match: text
    type: field
    rule: aoguid
"""
GUIDS = ["d87bcaa2-3c7e-11eb-9e5f-0242ac130008", "e2c3a00c-3d55-11eb-9088-0242ac130008"]


def test_country_pattern_repeating_universal_match_is_dropped(tmp_path):
    processor = processor_with(tmp_path, [(UUIDRULE, "xx"), (FIAS, "ru")])
    columns = scan(processor, [{"uuid": g, "aoguid": g} for g in GUIDS], explain=True)
    assert classes(columns["uuid"]) == {"uuid"}
    assert any(r["reason"] == "Country-specific pattern repeats a universal match" for r in columns["uuid"].rejected)
    assert {"uuid", "rufias"} <= classes(columns["aoguid"])  # the field name supports rufias


def test_country_pattern_kept_when_scan_country_matches(tmp_path):
    processor = processor_with(tmp_path, [(FIAS, "ru")], countries=["ru"])
    assert "rufias" in classes(scan(processor, [{"id": g} for g in GUIDS])["id"])


IBANRULE = """
  ibanvalue:
    key: iban
    match: regex
    type: data
    rule: '[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}'
    validator: checksum:iban_mod97
"""
DEIBAN = """
  deibanvalue:
    key: de_iban
    match: regex
    type: data
    rule: 'DE[0-9]{20}'
    validator: checksum:iban_mod97
"""
MIXED_IBANS = ["GB82WEST12345698765432", "DE89370400440532013000", "FR1420041010050500013M02606"]


def test_country_pattern_with_shared_validator_dropped_on_mixed_values(tmp_path):
    processor = processor_with(tmp_path, [(IBANRULE, "xx"), (DEIBAN, "de")])
    columns = scan(processor, [{"account": v} for v in MIXED_IBANS], explain=True)
    assert classes(columns["account"]) == {"iban"}
    assert any(
        r["ruleid"] == "deibanvalue" and "same validator" in r["reason"] for r in columns["account"].rejected
    )


def test_country_pattern_with_shared_validator_kept_when_it_covers_all_values(tmp_path):
    processor = processor_with(tmp_path, [(IBANRULE, "xx"), (DEIBAN, "de")])
    rows = [{"account": "DE89370400440532013000"}, {"account": "DE75512108001245126199"}]
    assert classes(scan(processor, rows)["account"]) == {"iban", "de_iban"}


NUMBERRULE = """
  numbervalue:
    key: number
    match: regex
    type: data
    rule: '[0-9]+'
"""
CARDLIKE = """
  cardlikevalue:
    key: de_cardlike
    match: regex
    type: data
    rule: '[0-9]{16}'
    validator: checksum:luhn
"""


def test_country_pattern_with_own_validator_kept_below_universal_match(tmp_path):
    processor = processor_with(tmp_path, [(NUMBERRULE, "xx"), (CARDLIKE, "de")])
    rows = [{"n": "4111111111111111"}, {"n": "5500000000000004"}, {"n": "12345"}]
    assert classes(scan(processor, rows)["n"]) == {"number", "de_cardlike"}


# apicrafter/metacrafter#58: OKPD2 codes are valid IPv4 strings.
IPV4RULE = """
  ipv4value:
    key: ipv4
    match: regex
    type: data
    rule: '[0-9]{1,3}(\\.[0-9]{1,3}){3}'
"""
OKPDNAME = """
  okpdknown:
    key: okpd
    match: text
    type: field
    rule: okpd,okpd2
    priority: 1
"""
OKPDVALUE = """
  okpdvalue:
    key: okpd
    match: regex
    type: data
    rule: '[0-9]{2}(\\.[0-9]{1,2}){0,2}(\\.[0-9]{1,3})?'
    priority: 3
"""
OKPD_CODES = ["21.20.10.226", "21.20.10.194", "21.20.21.120", "21.20.23.190"]


def test_rank_puts_named_match_first_on_a_tie():
    from metacrafter.classify.processor import ColumnMatchResult, RuleResult

    result = ColumnMatchResult("okpd2")
    result.add(RuleResult(ruleid="ipv4", confidence=100.0, dataclass="ipv4", ruletype="data", evidence={"name_score": 0.0}))
    result.add(RuleResult(ruleid="date", confidence=60.0, dataclass="date", ruletype="field", evidence={"name_score": 1.0}))
    result.add(RuleResult(ruleid="okpd", confidence=100.0, dataclass="okpd", ruletype="data", evidence={"name_score": 1.0}))
    result.rank()
    assert [m.dataclass for m in result.matches] == ["okpd", "ipv4", "date"]


def test_scan_reports_named_datatype_as_primary(tmp_path):
    from metacrafter.core import CrafterCmd

    rules = tmp_path / "rules"
    rules.mkdir()
    (rules / "ip.yaml").write_text(ruleset(IPV4RULE), encoding="utf-8")
    (rules / "okpd.yaml").write_text(ruleset(OKPDVALUE + OKPDNAME.replace("okpdknown", "okpdname")), encoding="utf-8")
    cmd = CrafterCmd(rulepath=[str(rules)], registry_detectors="off")
    report = cmd.scan_data([{"okpd2": v} for v in OKPD_CODES], confidence=5.0)
    field = next(f for f in report["data"] if f["field"] == "okpd2")
    assert field["matches"][0]["dataclass"] == "okpd"
    assert field["datatype_url"].endswith("/okpd")
    assert {"okpd", "ipv4"} <= {m["dataclass"] for m in field["matches"]}
