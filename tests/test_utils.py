# -*- coding: utf-8 -*-
"""Tests for classify/utils.py"""
import pytest
import os
from metacrafter.classify.utils import (
    dict_generator,
    headers,
    get_dict_value,
    dict_to_columns,
    string_to_charrange,
    string_array_to_charrange,
    detect_encoding,
    detect_delimiter,
    etree_to_dict,
)


class TestDictGenerator:
    def test_simple_dict(self):
        data = {"key1": "value1", "key2": "value2"}
        result = list(dict_generator(data))
        assert len(result) == 2
        assert ["key1", "value1"] in result
        assert ["key2", "value2"] in result

    def test_nested_dict(self):
        data = {"key1": {"nested": "value"}, "key2": "value2"}
        result = list(dict_generator(data))
        assert len(result) == 2
        assert ["key1", "nested", "value"] in result
        assert ["key2", "value2"] in result

    def test_dict_with_list(self):
        data = {"key1": [{"item": "value"}]}
        result = list(dict_generator(data))
        assert len(result) == 1
        assert ["key1", "item", "value"] in result

    def test_dict_ignores_id(self):
        data = {"_id": "123", "key1": "value1"}
        result = list(dict_generator(data))
        assert ["_id", "123"] not in result
        assert ["key1", "value1"] in result

    def test_empty_dict(self):
        data = {}
        result = list(dict_generator(data))
        assert len(result) == 0


class TestHeaders:
    def test_simple_dict_list(self):
        data = [{"id": "1", "name": "John"}, {"id": "2", "name": "Mary"}]
        result = headers(data)
        assert "id" in result
        assert "name" in result

    def test_nested_dict(self):
        data = [{"user": {"id": "1", "name": "John"}}]
        result = headers(data)
        assert "user.id" in result
        assert "user.name" in result

    def test_empty_list(self):
        data = []
        result = headers(data)
        assert len(result) == 0

    def test_limit(self):
        data = [{"id": str(i), "name": f"Name{i}"} for i in range(2000)]
        result = headers(data, limit=10)
        assert len(result) == 2  # id and name


class TestGetDictValue:
    def test_simple_key(self):
        data = {"key1": "value1", "key2": "value2"}
        result = get_dict_value(data, ["key1"])
        assert result == ["value1"]

    def test_nested_key(self):
        data = {"user": {"id": "1", "name": "John"}}
        result = get_dict_value(data, ["user", "id"])
        assert result == ["1"]

    def test_list_of_dicts(self):
        data = [{"id": "1"}, {"id": "2"}]
        result = get_dict_value(data, ["id"])
        assert len(result) == 2
        assert "1" in result
        assert "2" in result

    def test_missing_key(self):
        data = {"key1": "value1"}
        result = get_dict_value(data, ["missing"])
        assert result == []

    def test_none_object(self):
        result = get_dict_value(None, ["key"])
        assert result == []


class TestDictToColumns:
    def test_simple_dict_list(self):
        data = [{"id": "1", "name": "John"}, {"id": "2", "name": "Mary"}]
        result = dict_to_columns(data)
        assert "id" in result
        assert "name" in result
        assert result["id"] == ["1", "2"]
        assert result["name"] == ["John", "Mary"]

    def test_nested_dict(self):
        data = [{"user": {"id": "1"}}]
        result = dict_to_columns(data)
        assert "user.id" in result
        assert result["user.id"] == ["1"]

    def test_empty_list(self):
        data = []
        result = dict_to_columns(data)
        assert len(result) == 0


class TestStringToCharrange:
    def test_simple_string(self):
        result = string_to_charrange("hello")
        assert result["h"] == 1
        assert result["e"] == 1
        assert result["l"] == 2
        assert result["o"] == 1

    def test_empty_string(self):
        result = string_to_charrange("")
        assert len(result) == 0

    def test_repeated_chars(self):
        result = string_to_charrange("aaa")
        assert result["a"] == 3


class TestStringArrayToCharrange:
    def test_array_of_strings(self):
        data = ["hello", "world"]
        result = string_array_to_charrange(data)
        assert result["h"] == 1
        assert result["w"] == 1
        assert result["l"] == 3  # 2 from hello, 1 from world
        assert result["o"] == 2  # 1 from hello, 1 from world

    def test_empty_array(self):
        result = string_array_to_charrange([])
        assert len(result) == 0


class TestDetectEncoding:
    def test_detect_encoding_csv(self):
        # Use test fixture
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_comma.csv"
        )
        if os.path.exists(fixture_path):
            result = detect_encoding(fixture_path)
            assert result is not None
            assert "encoding" in result

    def test_detect_encoding_limit(self):
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_comma.csv"
        )
        if os.path.exists(fixture_path):
            result = detect_encoding(fixture_path, limit=100)
            assert result is not None


class TestDetectDelimiter:
    def test_comma_delimiter(self):
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_comma.csv"
        )
        if os.path.exists(fixture_path):
            result = detect_delimiter(fixture_path)
            assert result == ","

    def test_semicolon_delimiter(self):
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_semicolon.csv"
        )
        if os.path.exists(fixture_path):
            result = detect_delimiter(fixture_path, encoding="utf8")
            assert result == ";"

    def test_tab_delimiter(self):
        fixture_path = os.path.join(
            os.path.dirname(__file__), "fixtures", "ru_utf8_tab.csv"
        )
        if os.path.exists(fixture_path):
            result = detect_delimiter(fixture_path, encoding="utf8")
            assert result == "\t"


class TestEtreeToDict:
    def test_etree_simple(self):
        # Mock etree element
        class MockElement:
            def __init__(self, tag, text=None, attrib=None):
                self.tag = tag
                self.text = text
                self.attrib = attrib or {}
                self.children = []

            def __iter__(self):
                return iter(self.children)

        root = MockElement("root", text="test")
        result = etree_to_dict(root)
        assert "root" in result
        assert result["root"] == "test"

    def test_etree_with_children(self):
        class MockElement:
            def __init__(self, tag, text=None, attrib=None):
                self.tag = tag
                self.text = text
                self.attrib = attrib or {}
                self.children = []

            def __iter__(self):
                return iter(self.children)

        root = MockElement("root")
        child = MockElement("child", text="value")
        root.children = [child]
        result = etree_to_dict(root)
        assert "root" in result
        assert "child" in result["root"]
        assert result["root"]["child"] == "value"

    def test_etree_with_attrib(self):
        class MockElement:
            def __init__(self, tag, text=None, attrib=None):
                self.tag = tag
                self.text = text
                self.attrib = attrib or {}
                self.children = []

            def __iter__(self):
                return iter(self.children)

        root = MockElement("root", attrib={"attr1": "value1"})
        result = etree_to_dict(root)
        assert "root" in result
        assert "@attr1" in result["root"]
        assert result["root"]["@attr1"] == "value1"

