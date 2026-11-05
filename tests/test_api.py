"""Tests for API endpoints."""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock

from flask import Flask
from metacrafter.server.api import MetacrafterApp, scan_data
from metacrafter.classify.processor import RulesProcessor
import qddate


@pytest.fixture
def app():
    """Create Flask test app."""
    app_factory = MetacrafterApp()
    app = app_factory.app
    app.config['TESTING'] = True
    return app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


@pytest.fixture
def mock_processor():
    """Create mock rules processor."""
    processor = Mock(spec=RulesProcessor)
    processor.match_dict.return_value = Mock(
        results=[],
        spec=['results']
    )
    return processor


@pytest.fixture
def mock_date_parser():
    """Create mock date parser."""
    return Mock(spec=qddate.DateParser)


def test_scan_data_endpoint_success(client):
    response = client.post("/api/v1/scan_data", json=[{"name": "John", "email": "john@example.com"}], query_string={"limit": 100})
    assert response.status_code == 200
    report = response.get_json()
    assert report["report_version"] == 3
    assert {field["field"] for field in report["data"]} == {"name", "email"}


def test_scan_data_comma_separated_langs_contexts(client):
    response = client.post("/api/v1/scan_data", json=[{"name": "John", "email": "a@b.test"}], query_string={"langs": "en,ru", "contexts": "pii,finance", "confidence": "100", "fields": "email"})
    assert response.status_code == 200
    assert [f["field"] for f in response.get_json()["data"]] == ["email"]


def test_scan_data_invalid_json(client):
    """Test API with invalid JSON."""
    response = client.post(
        "/api/v1/scan_data",
        data="invalid json",
        content_type="application/json"
    )
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data
    assert "Invalid JSON" in data.get("error", "")


def test_scan_data_missing_data(client):
    """Test API with missing data."""
    response = client.post(
        "/api/v1/scan_data",
        data="",
        content_type="application/json"
    )
    assert response.status_code == 400  # Could be either depending on implementation


def test_metacrafter_app_initialization():
    """Test MetacrafterApp initialization."""
    app = MetacrafterApp()
    assert app.app is not None
    assert app.rules_processor is None
    assert app.date_parser is None


def test_metacrafter_app_with_dependencies():
    """Test MetacrafterApp with provided dependencies."""
    processor = Mock(spec=RulesProcessor)
    parser = Mock(spec=qddate.DateParser)
    
    app = MetacrafterApp(rules_processor=processor, date_parser=parser)
    assert app.rules_processor is processor
    assert app.date_parser is parser


def test_metacrafter_app_initialize_rules():
    """Test MetacrafterApp.initialize_rules()."""
    app = MetacrafterApp()
    app.initialize_rules()
    assert app.rules_processor is not None
    assert app.date_parser is not None

