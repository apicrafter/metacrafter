# -*- coding: utf-8 -*-
"""Tests for classify/stats.py"""
import pytest
import os
import tempfile
import json
from datetime import datetime, date
from metacrafter.classify.stats import (
    Analyzer,
    guess_datatype,
    guess_int_size,
    get_file_type,
    get_option,
)


class TestGuessIntSize:
    def test_uint8(self):
        assert guess_int_size(100) == "uint8"
        assert guess_int_size(255) == "uint8"
        assert guess_int_size(0) == "uint8"

    def test_uint16(self):
        assert guess_int_size(256) == "uint16"
        assert guess_int_size(65535) == "uint16"
        assert guess_int_size(5000) == "uint16"

    def test_uint32(self):
        assert guess_int_size(65536) == "uint32"
        assert guess_int_size(100000) == "uint32"


class TestGuessDatatype:
    def test_none_value(self):
        result = guess_datatype(None)
        assert result["base"] == "empty"

    def test_bool_value(self):
        result = guess_datatype(True)
        assert result["base"] == "bool"
        result = guess_datatype(False)
        assert result["base"] == "bool"

    def test_int_value(self):
        result = guess_datatype(42)
        assert result["base"] == "int"

    def test_float_value(self):
        result = guess_datatype(3.14)
        assert result["base"] == "float"

    def test_datetime_value(self):
        result = guess_datatype(datetime(2023, 1, 1, 12, 0, 0))
        assert result["base"] == "datetime"

    def test_date_value(self):
        result = guess_datatype(date(2023, 1, 1))
        assert result["base"] == "date"

    def test_string_digit(self):
        result = guess_datatype("123")
        assert result["base"] == "int"
        assert "subtype" in result

    def test_string_digit_leading_zero(self):
        result = guess_datatype("0123")
        assert result["base"] == "numstr"

    def test_string_float(self):
        result = guess_datatype("3.14")
        assert result["base"] == "float"

    def test_string_empty(self):
        result = guess_datatype("")
        assert result["base"] == "empty"

    def test_string_regular(self):
        result = guess_datatype("hello")
        assert result["base"] == "str"

    def test_non_string_type(self):
        result = guess_datatype([1, 2, 3])
        assert result["base"] == "typed"


class TestGetFileType:
    def test_csv_file(self):
        assert get_file_type("test.csv") == "csv"
        assert get_file_type("TEST.CSV") == "csv"

    def test_json_file(self):
        assert get_file_type("test.json") == "json"

    def test_jsonl_file(self):
        assert get_file_type("test.jsonl") == "jsonl"

    def test_unsupported_file(self):
        assert get_file_type("test.txt") is None

    def test_no_extension(self):
        assert get_file_type("test") is None


class TestGetOption:
    def test_option_exists(self):
        options = {"key": "value"}
        assert get_option(options, "key") == "value"

    def test_option_missing_has_default(self):
        options = {}
        assert get_option(options, "encoding") == "utf8"

    def test_option_missing_no_default(self):
        options = {}
        assert get_option(options, "nonexistent") is None


class TestAnalyzer:
    def test_analyzer_init_with_dates(self):
        analyzer = Analyzer(nodates=False)
        assert analyzer.qd is not None

    def test_analyzer_init_without_dates(self):
        analyzer = Analyzer(nodates=True)
        assert analyzer.qd is None

    def test_analyze_simple_dict_list(self):
        analyzer = Analyzer(nodates=True)
        items = [{"id": "1", "name": "John"}, {"id": "2", "name": "Mary"}]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        assert len(result) >= 2  # At least id and name fields

    def test_analyze_empty_list(self):
        analyzer = Analyzer()
        result = analyzer.analyze(itemlist=[])
        assert result is None

    def test_analyze_none_input(self):
        analyzer = Analyzer()
        result = analyzer.analyze(itemlist=None, fromfile=None)
        assert result is None

    def test_analyze_nested_dict(self):
        analyzer = Analyzer(nodates=True)
        items = [{"user": {"id": "1", "name": "John"}}]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        # Should have flattened nested keys
        assert any("user.id" in str(row[0]) or "user.name" in str(row[0]) for row in result)

    def test_analyze_with_datatypes(self):
        analyzer = Analyzer(nodates=True)
        items = [
            {"id": 1, "name": "John", "active": True, "score": 95.5},
            {"id": 2, "name": "Mary", "active": False, "score": 87.2},
        ]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        # Check that different types are detected
        for row in result:
            assert row[1] in ["str", "int", "bool", "float"]  # ftype

    def test_analyze_stats_fields(self):
        analyzer = Analyzer(nodates=True)
        items = [{"id": "1", "name": "John"}, {"id": "2", "name": "John"}]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        for row in result:
            # Check required fields are present
            assert len(row) >= 10  # Should have multiple fields
            key = row[0]
            ftype = row[1]
            n_uniq = row[4]
            share_uniq = row[5]
            assert isinstance(key, str)
            assert isinstance(ftype, str)
            assert isinstance(n_uniq, int)
            assert isinstance(share_uniq, float)

    def test_analyze_ignores_id(self):
        analyzer = Analyzer(nodates=True)
        items = [{"_id": "123", "name": "John"}]
        result = analyzer.analyze(itemlist=items)
        # _id should be ignored
        if result:
            assert not any("_id" in str(row[0]) for row in result)

    def test_analyze_jsonl_file(self):
        analyzer = Analyzer(nodates=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows_flat.jsonl"
        )
        if os.path.exists(fixture_path):
            result = analyzer.analyze(
                fromfile=fixture_path, options={"format_in": "jsonl"}
            )
            assert result is not None
            assert len(result) >= 2  # At least id and name

    def test_analyze_csv_file(self):
        analyzer = Analyzer(nodates=True)
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "2cols6rows.csv"
        )
        if os.path.exists(fixture_path):
            result = analyzer.analyze(
                fromfile=fixture_path, options={"format_in": "csv", "delimiter": ","}
            )
            assert result is not None

    def test_analyze_with_unique_values(self):
        analyzer = Analyzer(nodates=True)
        items = [{"id": str(i), "name": f"Name{i}"} for i in range(10)]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        # All values are unique
        for row in result:
            if row[0] == "id" or row[0] == "name":
                assert row[5] == 100.0  # share_uniq should be 100%

    def test_analyze_with_repeated_values(self):
        analyzer = Analyzer(nodates=True)
        items = [{"status": "active"} for _ in range(10)]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        # All values are the same
        for row in result:
            if row[0] == "status":
                assert row[5] < 100.0  # share_uniq should be less than 100%
                assert "dict" in row[9]  # tags should include "dict"

    def test_analyze_string_length_stats(self):
        analyzer = Analyzer(nodates=True)
        items = [
            {"name": "A"},
            {"name": "AB"},
            {"name": "ABC"},
            {"name": "ABCD"},
            {"name": "ABCDE"},
        ]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        for row in result:
            if row[0] == "name":
                assert row[6] == 1  # minlen
                assert row[7] == 5  # maxlen
                assert 1 <= row[8] <= 5  # avglen

    def test_analyze_string_character_stats(self):
        analyzer = Analyzer(nodates=True)
        items = [
            {"field": "abc123"},
            {"field": "def456"},
            {"field": "ghi789"},
        ]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        for row in result:
            if row[0] == "field":
                assert row[10] > 0  # has_digit
                assert row[11] > 0  # has_alphas
                assert row[12] == 0  # has_special (none in these examples)

    def test_analyze_with_options(self):
        analyzer = Analyzer(nodates=True)
        items = [{"id": "1", "name": "John"}]
        options = {"delimiter": ",", "encoding": "utf8", "limit": 1000}
        result = analyzer.analyze(itemlist=items, options=options)
        assert result is not None

    def test_analyze_empty_values(self):
        analyzer = Analyzer(nodates=True)
        items = [{"id": "1", "name": ""}, {"id": "2", "name": None}]
        result = analyzer.analyze(itemlist=items)
        assert result is not None
        # Should handle empty values gracefully

    def test_analyze_dict_keys(self):
        analyzer = Analyzer(nodates=True)
        items = [{"status": "active"} for _ in range(10)]
        result = analyzer.analyze(itemlist=items, options={"dictshare": 10})
        assert result is not None
        # Should identify dict keys when share_uniq < dictshare
        for row in result:
            if row[0] == "status":
                assert row[2] is True  # is_dictkey should be True

