"""Field guards, map keys, generic names, yes/no columns and geometries (metacrafter-rules #69, #70).

- ``fieldregex``: a value rule may ask for a field name by regular expression (Unix milliseconds
  in date-named fields), and a field rule with ``needs_values: true`` needs values.
- The keys of a nested object that is a map (the words of an OpenAlex inverted index) are data:
  field-name rules and field guards do not look at them.
- A field-name match of a generic text type gives way to a value rule of another type, and to
  values that are short lower-case codes.
- A yes/no match is dropped when the values hold a third answer, in the sample or in the
  records read after it.
- The coordinates of GeoJSON lines and polygons are labelled ``geojson``.
"""
from metacrafter.classify.processor import RulesProcessor
from metacrafter.classify.utils import sparse_map_parents

HEADER = """
format_version: 2
name: t
description: t
context: common
lang: common
rules:
"""


def processor_with(tmp_path, rules):
    path = tmp_path / "r.yaml"
    path.write_text(HEADER + rules, encoding="utf-8")
    proc = RulesProcessor()
    proc.import_rules(str(path))
    return proc


def labels(processor, rows, **kwargs):
    kwargs.setdefault("confidence", 20)
    kwargs.setdefault("parse_dates", False)
    result = processor.match_dict(rows, **kwargs)
    return {r.field: sorted({m.dataclass for m in r.matches}) for r in result.results}


# ---------------------------------------------------------------- fieldregex (rules #70)
UNIXTIME_MS = r"""
  unixtimemsbydate:
    key: unixtime_ms
    match: regex
    type: data
    rule: '-?\d{11,13}'
    fieldregex: 'date|datum|data|fecha|_at$|time'
    priority: 1
"""


def test_field_regex_guard_runs_the_rule_on_matching_names_only(tmp_path):
    proc = processor_with(tmp_path, UNIXTIME_MS)
    rows = [{"TicketReportedOnDate": 1689019752000 + i, "Data założenia": 936050400000 + i,
             "updated_at": 1689019752000 + i, "code": 1689019752000 + i} for i in range(10)]
    found = labels(proc, rows)
    assert found["TicketReportedOnDate"] == ["unixtime_ms"]
    assert found["Data założenia"] == ["unixtime_ms"]
    assert found["updated_at"] == ["unixtime_ms"]
    assert found["code"] == []  # a 13-digit code without a date-like name


def test_field_regex_or_field_rule(tmp_path):
    # A rule pack keeps a keyword list for metacrafter versions that ignore fieldregex.
    proc = processor_with(tmp_path, UNIXTIME_MS + "    fieldrule: ts\n    fieldrulematch: text\n")
    rows = [{"ts": 1689019752000 + i, "created": 1689019752000 + i, "x": 1689019752000 + i} for i in range(5)]
    found = labels(proc, rows)
    assert found["ts"] == ["unixtime_ms"] and found["x"] == []
    assert found["created"] == []
    rows = [{"createdTime": 1689019752000 + i} for i in range(5)]
    assert labels(proc, rows)["createdTime"] == ["unixtime_ms"]


def test_field_regex_on_a_field_rule(tmp_path):
    proc = processor_with(tmp_path, """
  monthbyname:
    key: month
    match: text
    type: field
    rule: m
    fieldregex: '^m$'
""")
    assert labels(proc, [{"M": "1"}])["M"] == ["month"]


def test_invalid_field_regex_skips_the_rule(tmp_path):
    proc = processor_with(tmp_path, UNIXTIME_MS.replace("'date|datum|data|fecha|_at$|time'", "'date('"))
    assert not proc.data_rules
    assert proc.diagnostics[0]["error"] == "Required field guard unavailable"


def test_field_regex_is_in_the_rule_schema():
    from metacrafter.rules.validation import validate_rule_file

    document = {"name": "t", "description": "t", "context": "common", "lang": "common",
                "rules": {"r": {"key": "k", "type": "data", "match": "regex", "rule": "x", "fieldregex": ""}}}
    assert validate_rule_file(document)[1]["r"]


def test_field_rule_with_needs_values(tmp_path):
    proc = processor_with(tmp_path, """
  createdms:
    key: unixtime_ms
    match: text
    type: field
    rule: created_at
    validator: metacrafter.classify.utils.strip_language_suffix
    needs_values: true
  createdname:
    key: date
    match: text
    type: field
    rule: created_at
    validator: metacrafter.classify.utils.strip_language_suffix
""")
    empty = [{"created_at": None}, {"created_at": ""}]
    result = proc.match_dict(empty, confidence=20, parse_dates=False, explain=True).results[0]
    # Without needs_values a rule with a validator keeps its match on an empty field.
    assert [m.dataclass for m in result.matches] == ["date"]
    assert result.rejected[0]["reason"] == "The field-name rule needs values and the field has none"
    # Values after the sample count when the sample has none.
    result = proc.match_dict([{"created_at": None}], confidence=20, parse_dates=False,
                             lookahead=[{"created_at": "1689019752000"}]).results[0]
    assert sorted(m.dataclass for m in result.matches) == ["date", "unixtime_ms"]


def test_needs_values_is_ignored_unless_true(tmp_path):
    proc = processor_with(tmp_path, """
  createdms:
    key: unixtime_ms
    match: text
    type: field
    rule: created_at
    needs_values: false
""")
    assert labels(proc, [{"created_at": None}])["created_at"] == ["unixtime_ms"]
    from metacrafter.rules.validation import validate_rule_file

    document = {"name": "t", "description": "t", "context": "common", "lang": "common",
                "rules": {"r": {"key": "k", "type": "field", "match": "text", "rule": "x", "needs_values": "yes"}}}
    assert validate_rule_file(document)[1]["r"]


# ---------------------------------------------------------------- map keys are data (rules #69)
NAME_RULES = """
  yearbyname:
    key: year
    match: text
    type: field
    rule: year,data
    priority: 1
  idbyname:
    key: id
    match: text
    type: field
    rule: id
    priority: 1
"""


def inverted_index(work, words=40):
    return {"id": f"W{work}", "abstract_inverted_index": {f"w{work}_{i}": [i] for i in range(words)} | {"year": [3]}}


def test_keys_of_an_inverted_index_are_not_field_names(tmp_path):
    rows = [inverted_index(i) for i in range(6)]
    assert sparse_map_parents(rows) == {"abstract_inverted_index"}
    found = labels(processor_with(tmp_path, NAME_RULES), rows)
    assert found["abstract_inverted_index.year"] == []
    assert found["id"] == ["id"]


def test_nested_objects_of_one_schema_keep_their_names():
    rows = [{"a": {f"k{j}": j for j in range(150)}} for _ in range(5)]  # wide, but the same keys
    assert sparse_map_parents(rows) == set()
    rows = [{"tags": {f"tag{i}_{j}": "yes" for j in range(30)}} for i in range(10)]  # text values
    assert sparse_map_parents(rows) == set()
    rows = [{"m": {f"k{i}_{j}": j for j in range(30)}} for i in range(2)]  # too few keys
    assert sparse_map_parents(rows) == set()


def test_map_keys_with_dots_are_escaped():
    rows = [{"x": {f"a.{i}.{j}": [j] for j in range(30)}} for i in range(5)]
    assert sparse_map_parents(rows) == {"x"}
    rows = [{"x.y": {"m": {f"k{i}_{j}": [j] for j in range(30)}}} for i in range(5)]
    assert sparse_map_parents(rows) == {"x\\.y.m"}


# ---------------------------------------------------------------- generic names (rules #69)
GENERIC = r"""
  namebyname:
    key: name
    match: text
    type: field
    rule: name
    priority: 1
  speciesvalue:
    key: species
    match: regex
    type: data
    rule: '[A-Z][a-z]+ [a-z]+(?: \(.+\))?'
"""


def test_generic_name_gives_way_to_a_specific_value_type(tmp_path):
    proc = processor_with(tmp_path, GENERIC)
    rows = [{"usage": {"name": "Paratrechina longicornis (Latreille, 1802)", "rank": "SPECIES"}},
            {"usage": {"name": "Apis mellifera"}}]
    assert labels(proc, rows)["usage.name"] == ["species"]
    rows = [{"name": "Hồ Chí Minh"}, {"name": "Bình Dương"}]
    assert labels(proc, rows)["name"] == ["name"]


def test_generic_name_of_short_codes(tmp_path):
    proc = processor_with(tmp_path, GENERIC)
    rows = [{"_links": {"curies": [{"name": "wp", "href": "https://api.w.org/{rel}"}]}} for _ in range(5)]
    assert labels(proc, rows)["_links.curies.name"] == []
    rows = [{"name": "Vietnam"} for _ in range(5)]
    assert labels(proc, rows)["name"] == ["name"]


# ---------------------------------------------------------------- yes/no with a third answer (rules #69)
YESNO = """
  yesno:
    key: boolean
    match: text
    type: data
    rule: да,нет,yes,no
"""


def test_boolean_with_a_third_answer_is_a_category(tmp_path):
    proc = processor_with(tmp_path, YESNO)
    rows = [{"b": v} for v in ["Да", "Нет", "Нет", "Частично", "Да", "Частично"]]
    assert labels(proc, rows)["b"] == []
    rows = [{"b": v} for v in ["yes"] * 8 + ["hospital", "retail", "kiosk"]]
    assert labels(proc, rows)["b"] == []


def test_boolean_with_two_answers_or_noise_stays(tmp_path):
    proc = processor_with(tmp_path, YESNO)
    # Placeholders are no answer.
    rows = [{"b": v} for v in ["Да", "Нет", "-", "N/A", "?", "Да", "?"]]
    assert labels(proc, rows)["b"] == ["boolean"]
    # Two answers, one of which the rule does not know (Spanish si, Bulgarian не).
    rows = [{"b": v} for v in ["NO"] * 8 + ["SI"] * 2]
    assert labels(proc, rows)["b"] == ["boolean"]
    # A third value given once is noise (a footnote), not an answer.
    rows = [{"b": v} for v in ["yes", "no"] * 4 + ["no [1]"]]
    assert labels(proc, rows)["b"] == ["boolean"]


def test_boolean_third_answer_after_the_sample(tmp_path):
    proc = processor_with(tmp_path, YESNO)
    rows = [{"b": v} for v in ["Да", "Нет"] * 5]
    assert labels(proc, rows, lookahead=[{"b": "Нет"}, {"b": "Да"}])["b"] == ["boolean"]
    assert labels(proc, rows, lookahead=[{"b": "Частично"}, {"b": "Частично"}])["b"] == []


# ---------------------------------------------------------------- coordinates of shapes (rules #69)
def feature(kind, coordinates):
    return {"type": "Feature", "properties": {"id": 1}, "geometry": {"type": kind, "coordinates": coordinates}}


def geometry_labels(proc, rows):
    stats = {"geometry.coordinates": {"ftype": "float", "minlen": 1, "maxlen": 20}}
    result = proc.match_dict(rows, datastats=stats, confidence=5, parse_dates=False)
    return {r.field: [(m.dataclass, m.confidence) for m in r.matches] for r in result.results}["geometry.coordinates"]


def test_coordinates_of_lines_and_polygons_are_geojson(tmp_path):
    proc = processor_with(tmp_path, GENERIC)
    ring = [[[570181.18, 6592327.82], [570190.0, 6592327.82], [570181.18, 6592330.0], [570181.18, 6592327.82]]]
    rows = [feature("Polygon", ring), feature("MultiPolygon", [ring]), feature("LineString", ring[0]),
            feature("Point", [24.1, 56.9])]
    assert geometry_labels(proc, rows) == [("geojson", 75.0)]
    rows = [feature("Point", [24.1 + i / 100, 56.9]) for i in range(5)]
    assert geometry_labels(proc, rows) == []
