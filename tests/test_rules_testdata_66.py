"""A value rule with a field guard beats an unnamed value rule of the same values
(metacrafter-rules #66).

NTEE codes of the IRS EO BMF (``NTEE_CD``: B71Z, J40, D200) also have the shape of ICD-10 codes.
The NTEE rule runs only on fields named for it (its ``fieldrule`` guard) and matches all values;
the ICD-10 rule, which has no field-name evidence, matched fewer and is dropped. A value rule
backed by a field-name rule keeps the unnamed matches beside it
(tests/test_false_positive_gates.py: ``uuid`` with ``rufias``, ``ipv4`` with ``okpd``).
"""
import pytest

from metacrafter.classify.processor import RulesProcessor

RULES = r"""
format_version: 2
name: t
description: t
context: common
lang: common
rules:
  nteevalue:
    key: us_ntee_code
    match: regex
    type: data
    rule: '[A-Z]\d{2}[A-Z0-9]?'
    minlen: 3
    maxlen: 4
    fieldrule: ntee_cd
    fieldrulematch: text
    priority: 1
  icd10value:
    key: icd10code
    match: regex
    type: data
    rule: '[A-Z]\d{2}(?:\.\d{1,2})?'
    minlen: 3
    maxlen: 7
    priority: 2
"""

NTEE = ["B71Z", "J40", "N50", "X20", "D200", "P33Z", "X21", "S41", "E220", "L21"]


@pytest.fixture
def processor(tmp_path):
    path = tmp_path / "r.yaml"
    path.write_text(RULES, encoding="utf-8")
    proc = RulesProcessor()
    proc.import_rules(str(path))
    return proc


def labels(processor, rows, **kwargs):
    result = processor.match_dict(rows, confidence=20, **kwargs)
    return {r.field: sorted({m.dataclass for m in r.matches}) for r in result.results}


def test_named_value_rule_drops_an_unnamed_rule_of_fewer_values(processor):
    rows = [{"NTEE_CD": code} for code in NTEE * 3]
    assert labels(processor, rows)["NTEE_CD"] == ["us_ntee_code"]


def test_the_rejection_is_explained(processor):
    rows = [{"NTEE_CD": code} for code in NTEE * 3]
    result = processor.match_dict(rows, confidence=20, explain=True)
    rejected = result.results[0].rejected
    assert any(r["ruleid"] == "icd10value" and "at least as many values" in r["reason"] for r in rejected)


def test_unnamed_rule_of_more_values_is_kept(processor):
    # ICD-10 codes with a decimal part are not NTEE codes: the ICD-10 rule matched more values.
    rows = [{"NTEE_CD": code} for code in ["J40", "N50", "E11.9", "I10.1", "K21.0", "M54.5"] * 3]
    assert labels(processor, rows)["NTEE_CD"] == ["icd10code", "us_ntee_code"]


def test_without_a_field_name_the_unnamed_rule_reports(processor):
    rows = [{"code": code} for code in NTEE * 3]
    assert labels(processor, rows)["code"] == ["icd10code"]


FIELD_BACKED = r"""
  nteename:
    key: us_ntee_code
    match: text
    type: field
    rule: ntee
    priority: 1
  nteeplain:
    key: us_ntee_code
    match: regex
    type: data
    rule: '[A-Z]\d{2}[A-Z0-9]?'
    minlen: 3
    maxlen: 4
    priority: 1
"""


def test_a_field_rule_backed_value_rule_keeps_the_unnamed_match(tmp_path):
    path = tmp_path / "r.yaml"
    text = RULES.split("  nteevalue:")[0] + FIELD_BACKED.lstrip("\n") + "  icd10value:" + RULES.split("  icd10value:")[1]
    path.write_text(text, encoding="utf-8")
    proc = RulesProcessor()
    proc.import_rules(str(path))
    rows = [{"ntee": code} for code in NTEE * 3]
    assert labels(proc, rows)["ntee"] == ["icd10code", "us_ntee_code"]
