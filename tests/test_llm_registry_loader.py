# -*- coding: utf-8 -*-
"""Tests for LLM registry loader."""
import json
import tempfile
import sys
from pathlib import Path
import pytest

# Import directly from module to avoid dependency on openai/chromadb
import metacrafter.classify.llm.registry_loader as registry_loader_module
load_registry = registry_loader_module.load_registry
filter_datatypes = registry_loader_module.filter_datatypes
get_datatype_text = registry_loader_module.get_datatype_text


def test_load_registry(tmp_path):
    """Test loading registry from JSONL file."""
    test_data = [
        {"id": "email", "name": "Email", "doc": "Email address", "langs": [{"id": "en", "name": "English"}]},
        {"id": "phone", "name": "Phone", "doc": "Phone number", "langs": [{"id": "en", "name": "English"}]},
    ]
    
    jsonl_file = tmp_path / "test.jsonl"
    with open(jsonl_file, 'w') as f:
        for item in test_data:
            f.write(json.dumps(item) + '\n')
    
    result = load_registry(jsonl_file)
    assert len(result) == 2
    assert result[0]["id"] == "email"
    assert result[1]["id"] == "phone"


def test_load_registry_file_not_found():
    """Test loading registry raises error when file not found."""
    with pytest.raises(FileNotFoundError):
        load_registry("/nonexistent/file.jsonl")


def test_load_registry_invalid_json(tmp_path):
    """Test loading registry handles invalid JSON gracefully."""
    jsonl_file = tmp_path / "test.jsonl"
    with open(jsonl_file, 'w') as f:
        f.write('{"id": "valid"}\n')
        f.write('invalid json line\n')
        f.write('{"id": "also_valid"}\n')
    
    # Should load valid lines and skip invalid ones
    result = load_registry(jsonl_file)
    assert len(result) == 2


def test_filter_datatypes_by_lang():
    """Test filtering datatypes by language."""
    datatypes = [
        {"id": "email", "langs": [{"id": "en", "name": "English"}]},
        {"id": "phone", "langs": [{"id": "ru", "name": "Russian"}]},
        {"id": "name", "langs": [{"id": "en", "name": "English"}]},
    ]
    
    filtered = filter_datatypes(datatypes, langs="en")
    assert len(filtered) == 2
    assert all(dt["id"] in ["email", "name"] for dt in filtered)


def test_filter_datatypes_by_country():
    """Test filtering datatypes by country."""
    datatypes = [
        {"id": "us_ssn", "country": [{"id": "US", "name": "United States"}]},
        {"id": "ru_inn", "country": [{"id": "RU", "name": "Russia"}]},
        {"id": "email", "country": []},
    ]
    
    filtered = filter_datatypes(datatypes, country="us")
    assert len(filtered) == 1
    assert filtered[0]["id"] == "us_ssn"


def test_filter_datatypes_by_categories():
    """Test filtering datatypes by categories."""
    datatypes = [
        {"id": "email", "categories": [{"id": "pii", "name": "PII"}]},
        {"id": "phone", "categories": [{"id": "pii", "name": "PII"}]},
        {"id": "uuid", "categories": [{"id": "common", "name": "Common"}]},
    ]
    
    filtered = filter_datatypes(datatypes, categories="pii")
    assert len(filtered) == 2
    assert all(dt["id"] in ["email", "phone"] for dt in filtered)


def test_filter_datatypes_multiple_filters():
    """Test filtering with multiple filters."""
    datatypes = [
        {"id": "us_email", "langs": ["en"], "country": ["us"], "categories": ["pii"]},
        {"id": "ru_email", "langs": ["ru"], "country": ["ru"], "categories": ["pii"]},
        {"id": "us_phone", "langs": ["en"], "country": ["us"], "categories": ["pii"]},
    ]
    
    filtered = filter_datatypes(datatypes, langs="en", country="us", categories="pii")
    assert len(filtered) == 2
    assert all(dt["id"] in ["us_email", "us_phone"] for dt in filtered)


def test_get_datatype_text():
    """Test converting datatype to text."""
    datatype = {
        "id": "email",
        "name": "Email Address",
        "doc": "Electronic mail address",
        "categories": [{"id": "pii", "name": "PII"}],
        "langs": [{"id": "en", "name": "English"}],
        "examples": [{"value": "test@example.com", "description": "Example"}],
        "regexp": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    }
    
    text = get_datatype_text(datatype)
    assert "ID: email" in text
    assert "Name: Email Address" in text
    assert "Description: Electronic mail address" in text
    assert "test@example.com" in text
    assert "pii" in text or "PII" in text


def test_get_datatype_text_minimal():
    """Test converting minimal datatype to text."""
    datatype = {
        "id": "test",
        "name": "Test"
    }
    
    text = get_datatype_text(datatype)
    assert "ID: test" in text
    assert "Name: Test" in text

