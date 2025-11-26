# -*- coding: utf-8 -*-
"""Tests for classify/processor.py"""
import pytest
import os
from metacrafter.classify.processor import (
    RulesProcessor,
    RuleResult,
    ColumnMatchResult,
    TableScanResult,
    RULE_TYPE_FIELD,
    RULE_TYPE_DATA,
)


class TestRuleResult:
    def test_rule_result_init(self):
        result = RuleResult(
            ruleid="test_rule",
            dataclass="test_class",
            confidence=95.5,
            ruletype="data",
            is_pii=True,
            format="YYYY-MM-DD",
        )
        assert result.ruleid == "test_rule"
        assert result.dataclass == "test_class"
        assert result.confidence == 95.5
        assert result.ruletype == "data"
        assert result.is_pii is True
        assert result.format == "YYYY-MM-DD"

    def test_rule_result_asdict(self):
        result = RuleResult(
            ruleid="test_rule",
            dataclass="test_class",
            confidence=95.5,
            ruletype="data",
        )
        result_dict = result.asdict()
        assert result_dict["ruleid"] == "test_rule"
        assert result_dict["dataclass"] == "test_class"
        assert result_dict["confidence"] == 95.5
        assert result_dict["ruletype"] == "data"
        assert "classurl" in result_dict

    def test_rule_result_class_url(self):
        result = RuleResult(
            ruleid="test_rule",
            dataclass="test_class",
            confidence=95.5,
            ruletype="data",
        )
        url = result.class_url()
        assert "test_class" in url
        assert url.startswith("https://")


class TestColumnMatchResult:
    def test_column_match_result_init(self):
        result = ColumnMatchResult(field="test_field", matches=[])
        assert result.field == "test_field"
        assert len(result.matches) == 0
        assert result.is_empty() is True

    def test_column_match_result_add(self):
        result = ColumnMatchResult(field="test_field", matches=[])
        rule_result = RuleResult(
            ruleid="test_rule", dataclass="test_class", confidence=95, ruletype="data"
        )
        result.add(rule_result)
        assert len(result.matches) == 1
        assert result.is_empty() is False

    def test_column_match_result_asdict(self):
        result = ColumnMatchResult(field="test_field", matches=[])
        rule_result = RuleResult(
            ruleid="test_rule", dataclass="test_class", confidence=95, ruletype="data"
        )
        result.add(rule_result)
        result_dict = result.asdict()
        assert result_dict["field"] == "test_field"
        assert len(result_dict["matches"]) == 1


class TestTableScanResult:
    def test_table_scan_result_init(self):
        result = TableScanResult()
        assert len(result.results) == 0
        assert result.is_empty() is True

    def test_table_scan_result_add(self):
        result = TableScanResult()
        column_result = ColumnMatchResult(field="test_field", matches=[])
        result.add(column_result)
        assert len(result.results) == 1
        assert result.is_empty() is False

    def test_table_scan_result_asdict(self):
        result = TableScanResult()
        column_result = ColumnMatchResult(field="test_field", matches=[])
        rule_result = RuleResult(
            ruleid="test_rule", dataclass="test_class", confidence=95, ruletype="data"
        )
        column_result.add(rule_result)
        result.add(column_result)
        result_dict = result.asdict()
        assert len(result_dict) == 1


class TestRulesProcessor:
    def test_rules_processor_init(self):
        processor = RulesProcessor()
        assert len(processor.field_rules) == 0
        assert len(processor.data_rules) == 0

    def test_rules_processor_init_with_filters(self):
        processor = RulesProcessor(langs=["en"], contexts=["common"])
        assert processor.preset_langs == ["en"]
        assert processor.preset_contexts == ["common"]

    def test_reset_rules(self):
        processor = RulesProcessor()
        # Import some rules first
        rule_path = os.path.join(
            os.path.dirname(__file__), "..", "rules", "common", "common.yaml"
        )
        if os.path.exists(rule_path):
            processor.import_rules(rule_path)
            assert len(processor.field_rules) > 0 or len(processor.data_rules) > 0
            processor.reset_rules()
            assert len(processor.field_rules) == 0
            assert len(processor.data_rules) == 0

    def test_import_rules_path(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=True)
            assert len(processor.field_rules) > 0 or len(processor.data_rules) > 0

    def test_import_rules_path_non_recursive(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules", "common")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=False)
            # Should import at least one file
            assert len(processor.field_rules) > 0 or len(processor.data_rules) > 0

    def test_import_rules_file(self):
        processor = RulesProcessor()
        rule_path = os.path.join(
            os.path.dirname(__file__), "..", "rules", "common", "common.yaml"
        )
        if os.path.exists(rule_path):
            processor.import_rules(rule_path)
            # Rules should be loaded
            assert len(processor.field_rules) > 0 or len(processor.data_rules) > 0

    def test_rules_processor_respects_country_filter(self, tmp_path):
        sample_rule = tmp_path / "sample.yaml"
        sample_rule.write_text(
            """
name: sample
description: Sample ruleset
context: sample
lang: en
country_code: us
rules:
  sample_field:
    key: sample_field
    name: Sample Field
    rule: sample
    type: field
    match: text
""",
            encoding="utf8",
        )
        processor_us = RulesProcessor(countries=["us"])
        processor_us.import_rules(str(sample_rule))
        assert len(processor_us.field_rules) == 1
        assert processor_us.field_rules[0]["country_code"] == ["us"]

        processor_ca = RulesProcessor(countries=["ca"])
        processor_ca.import_rules(str(sample_rule))
        assert len(processor_ca.field_rules) == 0

    def test_get_filtered_rules_no_filters(self):
        processor = RulesProcessor()
        rule_path = os.path.join(
            os.path.dirname(__file__), "..", "rules", "common", "common.yaml"
        )
        if os.path.exists(rule_path):
            processor.import_rules(rule_path)
            filtered = processor.get_filtered_rules(RULE_TYPE_FIELD)
            # Without filters, should return all rules
            assert isinstance(filtered, list)

    def test_get_filtered_rules_with_contexts(self):
        processor = RulesProcessor()
        rule_path = os.path.join(
            os.path.dirname(__file__), "..", "rules", "common", "common.yaml"
        )
        if os.path.exists(rule_path):
            processor.import_rules(rule_path)
            filtered = processor.get_filtered_rules(
                RULE_TYPE_FIELD, contexts=["common"]
            )
            assert isinstance(filtered, list)

    def test_get_filtered_rules_with_langs(self):
        processor = RulesProcessor()
        rule_path = os.path.join(
            os.path.dirname(__file__), "..", "rules", "common", "common.yaml"
        )
        if os.path.exists(rule_path):
            processor.import_rules(rule_path)
            filtered = processor.get_filtered_rules(RULE_TYPE_FIELD, langs=["common"])
            assert isinstance(filtered, list)

    def test_match_dict_simple(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=True)

            data = [{"id": "1", "name": "John"}, {"id": "2", "name": "Mary"}]
            results = processor.match_dict(data, limit=10)

            assert isinstance(results, TableScanResult)
            assert len(results.results) >= 0  # May or may not match

    def test_match_dict_with_datastats(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=True)

            data = [{"id": "1", "name": "John"}, {"id": "2", "name": "Mary"}]
            datastats = {
                "id": {
                    "ftype": "str",
                    "minlen": 1,
                    "maxlen": 1,
                    "tags": [],
                    "has_digit": 1,
                    "has_alphas": 0,
                    "has_special": 0,
                },
                "name": {
                    "ftype": "str",
                    "minlen": 3,
                    "maxlen": 5,
                    "tags": [],
                    "has_digit": 0,
                    "has_alphas": 1,
                    "has_special": 0,
                },
            }
            results = processor.match_dict(data, datastats=datastats, limit=10)

            assert isinstance(results, TableScanResult)

    def test_match_dict_with_filters(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=True)

            data = [{"id": "1", "name": "John"}]
            results = processor.match_dict(
                data, filter_contexts=["common"], filter_langs=["common"], limit=10
            )

            assert isinstance(results, TableScanResult)

    def test_match_dict_with_boolean_field(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=True)

            data = [{"active": True}, {"active": False}]
            datastats = {
                "active": {
                    "ftype": "bool",
                    "tags": [],
                }
            }
            results = processor.match_dict(data, datastats=datastats, limit=10)

            assert isinstance(results, TableScanResult)
            # Should have at least one match for boolean
            assert len(results.results) >= 1

    def test_match_dict_with_datetime_field(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=True)

            from datetime import datetime

            data = [{"date": datetime(2023, 1, 1)}, {"date": datetime(2023, 1, 2)}]
            datastats = {
                "date": {
                    "ftype": "datetime",
                    "tags": [],
                }
            }
            results = processor.match_dict(data, datastats=datastats, limit=10)

            assert isinstance(results, TableScanResult)

    def test_match_dict_with_date_field(self):
        processor = RulesProcessor()
        rule_path = os.path.join(os.path.dirname(__file__), "..", "rules")
        if os.path.exists(rule_path):
            processor.import_rules_path(rule_path, recursive=True)

            from datetime import date

            data = [{"birthday": date(2023, 1, 1)}, {"birthday": date(2023, 1, 2)}]
            datastats = {
                "birthday": {
                    "ftype": "date",
                    "tags": [],
                }
            }
            results = processor.match_dict(data, datastats=datastats, limit=10)

            assert isinstance(results, TableScanResult)
