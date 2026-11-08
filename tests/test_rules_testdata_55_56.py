"""Value evidence over name evidence (metacrafter-rules #55 and #56).

- qddate patterns are dates: a field is a datetime only when its values carry a time.
- A field-name ``date`` is dropped when the values are datetimes, a field-name ``name`` when
  every value is a number, a ``boolean`` value match when the field has more than three values.
- A checksum detector of a format with letters does not report digit strings without
  field-name or country evidence.
"""
import re

import qddate
import qddate.patterns
import pytest

from metacrafter.classify.processor import RulesProcessor, date_pattern_match

HEADER = """
format_version: 2
name: t
description: t
context: common
lang: common
rules:
"""

RULES = r"""
  datebyname:
    key: date
    match: text
    type: field
    rule: date,created_date
    priority: 1
  isodatetimevalue:
    key: datetime
    match: regex
    type: data
    rule: '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})?'
    minlen: 19
    maxlen: 25
  namebyname:
    key: name
    match: text
    type: field
    rule: name
    priority: 1
  yesno:
    key: boolean
    match: text
    type: data
    rule: да,нет,yes,no
"""


@pytest.fixture
def processor(tmp_path):
    path = tmp_path / "r.yaml"
    path.write_text(HEADER + RULES, encoding="utf-8")
    proc = RulesProcessor()
    proc.import_rules(str(path))
    return proc


@pytest.fixture(scope="module")
def dateparser():
    return qddate.DateParser(patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU)


def labels(processor, rows, dateparser=None, field=None):
    result = processor.match_dict(rows, confidence=50, dateparser=dateparser, parse_dates=dateparser is not None)
    found = {r.field: sorted({m.dataclass for m in r.matches}) for r in result.results}
    return found[field] if field else found


@pytest.mark.parametrize("values", [
    ["01.01.1974", "01.08.2016", "15.11.2024"],      # de_nrw Schulbetriebsdatum
    ["2020/8/5", "2020/6/16", "2020/8/21"],          # jp_tochigi 入札日
    ["26/07/2005", "11/10/2006", "07/04/2025"],      # br_fuel DATAPUBLICACAO
    ["2022.02.02", "2022.02.03", "2022.02.04"],      # hu_kekva date
])
def test_date_only_values_are_dates(processor, dateparser, values):
    assert labels(processor, [{"day": v} for v in values], dateparser, "day") == ["date"]


def test_values_with_a_time_are_datetimes(processor, dateparser):
    rows = [{"t": "2016-01-01 00:00:00"}, {"t": "2016-01-02 12:30:00"}]
    assert labels(processor, rows, dateparser, "t") == ["datetime"]


def test_date_pattern_match_reports_the_time(dateparser):
    assert date_pattern_match(dateparser, "2016-01-01 10:00")[1] is True
    assert date_pattern_match(dateparser, "2016-01-01")[1] is False
    assert date_pattern_match(dateparser, "30.09.2016 (бывш. 25.01.2008)")[1] is False


def test_date_name_with_datetime_values(processor, dateparser):
    rows = [{"date": "2021-11-12T08:08:16+00:00"}, {"date": "2023-12-03T11:59:06+00:00"}]
    assert labels(processor, rows, dateparser, "date") == ["datetime"]
    rows = [{"date": "2021-11-12"}, {"date": "2023-12-03"}]
    assert "date" in labels(processor, rows, dateparser, "date")


def test_name_with_numbers_only(processor):
    assert labels(processor, [{"name": "17"}, {"name": "18"}], field="name") == []
    assert labels(processor, [{"name": "Centro"}, {"name": "18"}], field="name") == ["name"]


def test_boolean_needs_a_yes_no_column(processor):
    rows = [{"b": v} for v in ["да"] * 6 + ["не", "кран", "бутон", "не"]]
    assert labels(processor, rows, field="b") == []
    rows = [{"b": v} for v in ["да", "нет", "да", "", "нет"]]
    assert labels(processor, rows, field="b") == ["boolean"]


def detector(regexp):
    return {"derived": True, "registry_country": ["DE"], "compiled": re.compile(regexp)}


def test_checksum_detector_of_a_format_with_letters_needs_evidence():
    proc = RulesProcessor()
    # German driving licence (letters allowed) and Italian AIC (base32): digits are not evidence.
    assert proc._digits_only_for_letters(detector(r"^[A-Z0-9]\d{2}[A-Z0-9]{6}[0-9X][A-Z0-9]$"), 0.0)
    assert proc._digits_only_for_letters(detector(r"^(?:0\d{8}|[0-9B-DF-HJ-NP-Z]{6})$"), 0.0)
    # An all-digit format (Russian INN) keeps reporting by checksum; a field name is evidence.
    assert not proc._digits_only_for_letters(detector(r"^(?:\d{10}|\d{12})$"), 0.0)
    assert not proc._digits_only_for_letters(detector(r"^[A-Z0-9]{11}$"), 1.0)
    proc.preset_countries = ["DE"]
    assert not proc._digits_only_for_letters(detector(r"^[A-Z0-9]{11}$"), 0.0)
