# -*- coding: utf-8 -*-
"""Tests for core.py"""
import pytest
import os
import tempfile
import json
from metacrafter.core import CrafterCmd


class TestCrafterCmd:
    def test_init_without_remote(self):
        cmd = CrafterCmd(remote=None, debug=False)
        assert cmd.remote is None
        assert cmd.processor is not None

    def test_init_with_remote(self):
        cmd = CrafterCmd(remote="http://localhost:10399", debug=False)
        assert cmd.remote == "http://localhost:10399"
        assert cmd.processor is None  # Processor not initialized when remote is set

    def test_init_with_debug(self):
        cmd = CrafterCmd(remote=None, debug=True)
        assert cmd.remote is None
        assert cmd.processor is not None

    def test_prepare_default_config(self):
        cmd = CrafterCmd(remote=None, debug=False)
        # prepare() is called in __init__ when remote is None
        assert cmd.processor is not None
        # Should have loaded some rules
        assert len(cmd.processor.field_rules) > 0 or len(cmd.processor.data_rules) > 0

    def test_prepare_with_config_file(self, tmp_path, monkeypatch):
        import yaml
        from metacrafter.config import DEFAULT_RULEPATH
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".metacrafter").write_text(yaml.safe_dump({"auto_rules": False, "rulepath": DEFAULT_RULEPATH}))
        cmd = CrafterCmd()
        assert cmd.processor is not None
        assert len(cmd.processor.field_rules) + len(cmd.processor.data_rules) >= 130

    def test_scan_data_simple(self):
        cmd = CrafterCmd(remote=None, debug=False)
        items = [{"id": "1", "name": "John"}, {"id": "2", "name": "Mary"}]
        report = cmd.scan_data(items, limit=10)
        assert report is not None
        assert "results" in report
        assert "data" in report
        assert isinstance(report["results"], list)
        assert isinstance(report["data"], list)

    def test_scan_data_with_filters(self):
        cmd = CrafterCmd(remote=None, debug=False)
        items = [{"id": "1", "name": "John"}]
        report = cmd.scan_data(
            items, limit=10, contexts=["common"], langs=["common"]
        )
        assert report is not None
        assert "results" in report
        assert "data" in report

    def test_scan_data_empty_list(self):
        cmd = CrafterCmd(remote=None, debug=False)
        items = []
        report = cmd.scan_data(items, limit=10)
        assert report is not None
        assert "results" in report
        assert "data" in report

    def test_scan_file_csv(self):
        cmd = CrafterCmd(remote=None, debug=False)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv"
        )
        if os.path.exists(fixture_path):
            # This will print output, but we can test it doesn't crash
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            # scan_file returns None when output is written to stdout
            # or returns empty list on error, so we just verify it doesn't raise
            assert isinstance(result, dict) and result["report_version"] == 3

    def test_scan_file_jsonl(self):
        cmd = CrafterCmd(remote=None, debug=False)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_flat.jsonl"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(fixture_path, limit=10, dformat="short")
            # scan_file returns None when output is written to stdout
            # or returns empty list on error
            assert isinstance(result, dict) and result["report_version"] == 3

    def test_scan_file_with_delimiter(self):
        cmd = CrafterCmd(remote=None, debug=False)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_semicolon.csv"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, delimiter=";", limit=10, dformat="short"
            )
            assert isinstance(result, dict) and result["report_version"] == 3

    def test_scan_file_with_encoding(self):
        cmd = CrafterCmd(remote=None, debug=False)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_cp1251_comma.csv"
        )
        if os.path.exists(fixture_path):
            result = cmd.scan_file(
                fixture_path, encoding="cp1251", limit=10, dformat="short"
            )
            assert isinstance(result, dict) and result["report_version"] == 3

    def test_scan_file_invalid_file(self):
        cmd = CrafterCmd(remote=None, debug=False)
        result = cmd.scan_file("/nonexistent/file.csv", limit=10)
        # Failed sources retain the explicit empty-list error return.
        assert result == []

    def test_scan_file_output_to_file(self):
        cmd = CrafterCmd(remote=None, debug=False)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv"
        )
        if os.path.exists(fixture_path):
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
                output_path = f.name

            try:
                cmd.scan_file(
                    fixture_path, limit=10, dformat="short", output=output_path
                )
                # Check if output file was created
                if os.path.exists(output_path):
                    with open(output_path, "r") as f:
                        content = f.read()
                        assert len(content) > 0
            finally:
                if os.path.exists(output_path):
                    os.remove(output_path)

    def test_rules_list(self):
        cmd = CrafterCmd(remote=None, debug=False)
        # This prints to stdout, but we can test it doesn't crash
        cmd.rules_list()

    def test_rules_dumpstats(self):
        cmd = CrafterCmd(remote=None, debug=False)
        # This prints to stdout, but we can test it doesn't crash
        cmd.rules_dumpstats()

    def test_craftercmd_country_code_filter(self, tmp_path):
        rule_dir = tmp_path / "rules"
        rule_dir.mkdir()
        us_rule = rule_dir / "us.yaml"
        ca_rule = rule_dir / "ca.yaml"
        us_rule.write_text(
            """
name: us-rules
description: US test rules
context: sample
lang: en
country_code: us
rules:
  us_field:
    key: us_field
    name: US Field
    rule: usfield
    type: field
    match: text
""",
            encoding="utf8",
        )
        ca_rule.write_text(
            """
name: ca-rules
description: CA test rules
context: sample
lang: en
country_code: ca
rules:
  ca_field:
    key: ca_field
    name: CA Field
    rule: cafield
    type: field
    match: text
""",
            encoding="utf8",
        )
        cmd = CrafterCmd(
            remote=None,
            debug=False,
            rulepath=[str(rule_dir)],
            country_codes=["us"],
        )
        assert cmd.processor is not None
        assert all("us" in r["country_code"] for r in cmd.processor.field_rules)
        assert any(rule["key"] == "us_field" for rule in cmd.processor.field_rules)

    def test_write_results_no_output(self):
        cmd = CrafterCmd(remote=None, debug=False)
        report = {
            "results": [
                ["field1", "str", "tag1,tag2", "match1", "url1"],
                ["field2", "int", "", "", ""],
            ],
            "data": [
                {"field": "field1", "matches": []},
                {"field": "field2", "matches": []},
            ],
        }
        # This prints to stdout, test doesn't crash
        cmd._write_results(report, "test.csv", "short", None)

    def test_write_results_with_output(self):
        cmd = CrafterCmd(remote=None, debug=False)
        report = {
            "results": [["field1", "str", "tag1", "match1", "url1"]],
            "data": [{"field": "field1", "matches": []}],
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            output_path = f.name

        try:
            cmd._write_results(report, "test.csv", "short", output_path)
            if os.path.exists(output_path):
                with open(output_path, "r") as f:
                    content = f.read()
                    assert len(content) > 0
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

    def test_write_results_csv_output(self):
        cmd = CrafterCmd(remote=None, debug=False)
        report = {
            "results": [["field1", "str", "tag1", "match1", "url1"]],
            "data": [{"field": "field1", "matches": []}],
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            output_path = f.name

        try:
            cmd._write_results(report, "test.csv", "short", output_path)
            if os.path.exists(output_path):
                with open(output_path, "r") as f:
                    content = f.read()
                    assert len(content) > 0
                    assert "key" in content or "field1" in content
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

    def test_write_results_full_format(self):
        cmd = CrafterCmd(remote=None, debug=False)
        report = {
            "results": [
                ["field1", "str", "tag1", "match1", "url1"],
                ["field2", "int", "", "", ""],
            ],
            "data": [
                {"field": "field1", "matches": []},
                {"field": "field2", "matches": []},
            ],
        }

        cmd._write_results(report, "test.csv", "full", None)

    def test_write_db_results(self):
        cmd = CrafterCmd(remote=None, debug=False)
        db_results = {
            "table1": {
                "results": [["field1", "str", "tag1", "match1", "url1"]],
                "data": [{"field": "field1", "matches": []}],
            }
        }

        cmd._write_db_results(db_results, "short", None)

    def test_write_db_results_with_output(self):
        cmd = CrafterCmd(remote=None, debug=False)
        db_results = {
            "table1": {
                "results": [["field1", "str", "tag1", "match1", "url1"]],
                "data": [{"field": "field1", "matches": []}],
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            output_path = f.name

        try:
            cmd._write_db_results(db_results, "short", output_path)
            if os.path.exists(output_path):
                with open(output_path, "r") as f:
                    content = f.read()
                    assert len(content) > 0
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

