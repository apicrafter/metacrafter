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
    # The default pack's OKPD rule runs only on fields named for it and matches every code, so
    # the IPv4 look-alike without field-name evidence is dropped (metacrafter-rules #66).
    assert {m["dataclass"] for m in field["matches"]} == {"okpd"}


# apicrafter/metacrafter#62: a value-only PII match outranked by a named non-PII datatype.
def value_match(dataclass, confidence, name_score=0.0, is_pii=False, ruletype="data"):
    return {
        "ruleid": dataclass, "dataclass": dataclass, "datatype": dataclass, "ruletype": ruletype,
        "confidence": confidence, "is_pii": is_pii, "pii_source": "registry" if is_pii else None,
        "evidence": {"name_score": name_score},
    }


def test_named_datatype_demotes_value_only_pii_match():
    from metacrafter.engine.pii import demote_outranked_pii

    okpd = value_match("okpd", 100.0, name_score=1.0)
    ipv4 = value_match("ipv4", 99.0, is_pii=True)
    ipaddr = value_match("ipaddr", 98.7, is_pii=True)
    demote_outranked_pii([okpd, ipv4, ipaddr])
    for match in (ipv4, ipaddr):
        assert match["is_pii"] is False
        assert match["pii_source"] is None
        assert match["pii_outranked_by"] == "okpd"
    assert "pii_outranked_by" not in okpd


@pytest.mark.parametrize(
    "matches",
    [
        # Field-name-only match: a field login with email values stays PII.
        [value_match("login", 60.0, name_score=1.0, ruletype="field"), value_match("email", 100.0, is_pii=True)],
        # The named datatype scores lower than the value match.
        [value_match("ipv4", 100.0, is_pii=True), value_match("okpd", 90.0, name_score=1.0)],
        # The named datatype is PII itself.
        [value_match("username", 100.0, name_score=1.0, is_pii=True), value_match("email", 100.0, is_pii=True)],
        # The field name also names the PII datatype.
        [value_match("okpd", 100.0, name_score=1.0), value_match("ipv4", 99.0, is_pii=True),
         value_match("ipv4", 60.0, name_score=1.0, is_pii=True, ruletype="field")],
        # LLM matches are not value matches.
        [value_match("okpd", 100.0, name_score=1.0), value_match("ipv4", 99.0, is_pii=True, ruletype="llm")],
    ],
)
def test_pii_match_kept(matches):
    from metacrafter.engine.pii import demote_outranked_pii

    demote_outranked_pii(matches)
    assert any(m["is_pii"] for m in matches)
    assert not any("pii_outranked_by" in m for m in matches)


def test_related_datatypes_keep_pii():
    from metacrafter.engine.pii import demote_outranked_pii

    parents = {"corporate_email": ["email"]}
    email = value_match("email", 100.0, is_pii=True)
    demote_outranked_pii([value_match("corporate_email", 100.0, name_score=1.0), email],
                         ancestors=lambda d: parents.get(d, []))
    assert email["is_pii"] is True


def test_registry_ancestors():
    records = [
        {"id": "ipaddr", "status": "stable", "kind": "datatype"},
        {"id": "ipv4", "status": "stable", "kind": "datatype", "parent": "ipaddr"},
        {"id": "loop", "status": "stable", "kind": "datatype", "parent": {"id": "loop"}},
    ]
    registry = SnapshotRegistry(datatypes=records, aliases={"ip4": "ipv4"}, manifest={}, source="test")
    adapter = RegistryAdapter._wrap(registry, None, "test")
    assert adapter.ancestors("ip4") == ["ipaddr"]
    assert adapter.ancestors("ipaddr") == []
    assert adapter.ancestors("loop") == []
    assert adapter.ancestors("unknown") == []


def okpd_scan(tmp_path, **kwargs):
    from metacrafter.core import CrafterCmd

    rules = tmp_path / "rules"
    rules.mkdir()
    (rules / "ip.yaml").write_text(ruleset(IPV4RULE + "    is_pii: true\n"), encoding="utf-8")
    (rules / "okpd.yaml").write_text(ruleset(OKPDVALUE + OKPDNAME.replace("okpdknown", "okpdname")), encoding="utf-8")
    cmd = CrafterCmd(rulepath=[str(rules)], registry_detectors="off", **kwargs)
    rows = [{"okpd2": v, "ip": v} for v in OKPD_CODES]
    report = cmd.scan_data(rows, confidence=5.0)
    return {f["field"]: f for f in report["data"]}


def test_scan_does_not_report_named_non_pii_field_as_pii(tmp_path):
    fields = okpd_scan(tmp_path)
    okpd2 = fields["okpd2"]
    assert okpd2["matches"][0]["dataclass"] == "okpd"
    # The guarded OKPD rule of the default pack matched every code: the IPv4 look-alike is dropped
    # rather than demoted (metacrafter-rules #66); test_named_datatype_demotes_value_only_pii_match
    # covers the demotion.
    assert all(m["dataclass"] != "ipv4" for m in okpd2["matches"])
    assert okpd2["pii"] == {"is_pii": False, "datatypes": []}
    # Without field-name evidence for okpd the same values stay PII.
    assert fields["ip"]["pii"]["is_pii"] is True
    assert all("pii_outranked_by" not in m for m in fields["ip"]["matches"])


def test_pii_only_drops_outranked_matches(tmp_path):
    fields = okpd_scan(tmp_path, pii_only=True)
    assert fields["okpd2"]["matches"] == []
    assert {m["dataclass"] for m in fields["ip"]["matches"]} == {"ipv4"}


GENERIC_ID = """
  idbyfield:
    key: id
    match: regex
    type: field
    rule: (?i)(?:.*[ _.-])?(?:id|nummer)(?:[ _.-].*)?
    priority: 1
"""
KG_NUMBER = """
  atkgnumberbyname:
    key: at_kg_number
    match: regex
    type: field
    rule: (?i)kg[ _-]?nummer
    priority: 1
"""
REFINED = [
    {"id": "id", "status": "draft", "kind": "datatype", "name": "Unique identifier"},
    {"id": "at_kg_number", "status": "draft", "kind": "datatype", "name": "Austrian cadastral municipality number",
     "parent": "id", "country": ["AT"], "regexp": "^\\d{5}$"},
    {"id": "city", "status": "stable", "kind": "datatype", "name": "City"},
    {"id": "egcity", "status": "draft", "kind": "datatype", "name": "Egyptian city", "country": ["EG"]},
    {"id": "fr_commune", "status": "draft", "kind": "datatype", "name": "French commune", "parent": "city",
     "country": ["FR"]},
]


@pytest.fixture
def refined_adapter():
    registry = SnapshotRegistry(datatypes=REFINED, aliases={}, manifest={"version": "test"}, source="test")
    return RegistryAdapter._wrap(registry, None, "test")


def test_country_type_kept_beside_its_broader_universal_type(tmp_path, refined_adapter):
    # The generic id rule names KG_NUMMER too; at_kg_number refines id, so it is not covered.
    processor = processor_with(tmp_path, [(GENERIC_ID, "xx"), (KG_NUMBER, "at")], refined_adapter)
    columns = scan(processor, [{"KG_NUMMER": "01204"}, {"KG_NUMMER": "73518"}])
    assert "at_kg_number" in classes(columns["KG_NUMMER"])


FRCOMMUNE = """
  frcommunefield:
    key: fr_commune
    match: text
    type: field
    rule: city
    priority: 1
"""


def test_country_name_variant_still_dropped_with_registry(tmp_path, refined_adapter):
    processor = processor_with(tmp_path, [(CITY, "xx"), (EGCITY, "eg")], refined_adapter)
    assert classes(scan(processor, [{"city": "Cairo"}])["city"]) == {"city"}


def test_country_variant_of_a_concrete_parent_still_dropped(tmp_path, refined_adapter):
    # fr_commune refines city, but city is a type of its own, not a fallback: a Belgian or
    # Swiss commune name is a city too.
    processor = processor_with(tmp_path, [(CITY, "xx"), (FRCOMMUNE, "fr")], refined_adapter)
    assert classes(scan(processor, [{"city": "Namur"}])["city"]) == {"city"}
