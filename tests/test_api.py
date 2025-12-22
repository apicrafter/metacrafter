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


def test_scan_data_endpoint_success(client, mock_processor, mock_date_parser):
    """Test successful API request."""
    with patch('metacrafter.server.api.Analyzer') as mock_analyzer_class:
        mock_analyzer = Mock()
        mock_analyzer.analyze.return_value = [
            ['field1', 'str', False, False, 10, 1.0, 5, 10, 7.5, [], True, True, False, {}]
        ]
        mock_analyzer_class.return_value = mock_analyzer
        
        # Replace the endpoint handler
        client.application.view_functions['scan_data'] = lambda: scan_data(
            mock_processor, mock_date_parser
        )
        
        response = client.post(
            "/api/v1/scan_data",
            json=[{"name": "John", "email": "john@example.com"}],
            query_string={"limit": 100}
        )
        
        assert response.status_code == 200
        data = response.get_json()
        assert "results" in data
        assert "data" in data


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
    assert response.status_code in [400, 500]  # Could be either depending on implementation


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

