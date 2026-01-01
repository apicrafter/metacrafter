# -*- coding: utf-8 -*-
"""Tests for Apache Atlas integration."""
import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock

from metacrafter.integrations.atlas import AtlasExporter, REQUESTS_AVAILABLE


@pytest.fixture
def sample_scan_report():
    """Sample Metacrafter scan report."""
    return {
        "results": [
            ["email", "str", "pii", "email 99.50", "https://registry.apicrafter.io/datatype/email"],
            ["phone", "str", "pii", "phone 95.20", "https://registry.apicrafter.io/datatype/phone"],
        ],
        "data": [
            {
                "field": "email",
                "matches": [
                    {
                        "ruleid": "email",
                        "dataclass": "email",
                        "confidence": 99.5,
                        "ruletype": "data",
                        "format": None,
                        "classurl": "https://registry.apicrafter.io/datatype/email",
                    }
                ],
                "tags": ["pii"],
                "ftype": "str",
                "datatype_url": "https://registry.apicrafter.io/datatype/email",
                "stats": {},
            },
            {
                "field": "phone",
                "matches": [
                    {
                        "ruleid": "phone",
                        "dataclass": "phone",
                        "confidence": 95.2,
                        "ruletype": "data",
                        "format": None,
                        "classurl": "https://registry.apicrafter.io/datatype/phone",
                    }
                ],
                "tags": ["pii"],
                "ftype": "str",
                "datatype_url": "https://registry.apicrafter.io/datatype/phone",
                "stats": {},
            },
        ],
        "stats": {},
    }


@pytest.mark.skipif(not REQUESTS_AVAILABLE, reason="requests library not installed")
class TestAtlasExporter:
    """Tests for AtlasExporter class."""
    
    def test_init(self):
        """Test AtlasExporter initialization."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
            timeout=60.0,
        )
        assert exporter.atlas_url == "http://localhost:21000"
        assert exporter.username == "admin"
        assert exporter.password == "admin"
        assert exporter.timeout == 60.0
        assert "Authorization" in exporter.auth_headers
    
    def test_init_from_env(self, monkeypatch):
        """Test initialization from environment variables."""
        monkeypatch.setenv("ATLAS_USERNAME", "env-user")
        monkeypatch.setenv("ATLAS_PASSWORD", "env-pass")
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        assert exporter.username == "env-user"
        assert exporter.password == "env-pass"
    
    def test_resolve_column_entity(self):
        """Test column entity resolution."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
        )
        
        # Mock the requests.get call
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entity": {
                "guid": "test-guid-123"
            }
        }
        
        with patch('requests.get', return_value=mock_response):
            guid = exporter._resolve_column_entity(
                "postgres.public.users",
                "email",
                "rdbms_column"
            )
            assert guid == "test-guid-123"
    
    def test_resolve_column_entity_not_found(self):
        """Test column entity resolution when entity not found."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        
        # Mock the requests.get call for 404
        mock_response = MagicMock()
        mock_response.status_code = 404
        
        with patch('requests.get', return_value=mock_response):
            guid = exporter._resolve_column_entity(
                "postgres.public.users",
                "nonexistent",
                "rdbms_column"
            )
            assert guid is None
    
    def test_extract_pii_classifications(self):
        """Test PII classification extraction."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        
        # Test with pii tag
        field_info = {"tags": ["pii"], "matches": []}
        classifications = exporter._extract_pii_classifications(field_info)
        assert "PII" in classifications
        
        # Test with email datatype
        field_info = {
            "tags": [],
            "matches": [{"dataclass": "email"}],
        }
        classifications = exporter._extract_pii_classifications(field_info)
        assert "PII" in classifications
    
    def test_extract_datatype_classifications(self):
        """Test datatype classification extraction."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        
        matches = [
            {"dataclass": "email"},
            {"dataclass": "phone"},
        ]
        classifications = exporter._extract_datatype_classifications(matches)
        assert "Email" in classifications
        assert "Phone" in classifications
    
    def test_build_attributes(self):
        """Test attribute building."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        
        field_info = {
            "ftype": "str",
            "datatype_url": "https://registry.apicrafter.io/datatype/email",
        }
        best_match = {
            "dataclass": "email",
            "confidence": 99.5,
            "ruleid": "email",
        }
        
        attributes = exporter._build_attributes(field_info, best_match)
        assert "metacrafter_confidence" in attributes
        assert "metacrafter_datatype" in attributes
        assert "metacrafter_datatype_url" in attributes
        assert "metacrafter_rule_id" in attributes
        assert attributes["metacrafter_confidence"] == "99.5"
    
    @patch('requests.get')
    @patch('requests.post')
    @patch('requests.put')
    def test_export_scan_results(self, mock_put, mock_post, mock_get, sample_scan_report):
        """Test exporting scan results."""
        # Mock entity resolution
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "entity": {
                "guid": "test-guid-123"
            }
        }
        mock_get.return_value = mock_get_response
        
        # Mock classification check (empty list)
        mock_class_response = MagicMock()
        mock_class_response.status_code = 200
        mock_class_response.json.return_value = {"list": []}
        
        # Mock entity get for attributes
        mock_entity_response = MagicMock()
        mock_entity_response.status_code = 200
        mock_entity_response.json.return_value = {
            "entity": {
                "guid": "test-guid-123",
                "attributes": {}
            }
        }
        
        # Setup mock_get to return different responses
        def get_side_effect(*args, **kwargs):
            url = args[0] if args else kwargs.get('url', '')
            if 'classifications' in url:
                return mock_class_response
            elif 'guid' in url and 'classifications' not in url:
                return mock_entity_response
            else:
                return mock_get_response
        
        mock_get.side_effect = get_side_effect
        
        # Mock successful POST and PUT
        mock_post_response = MagicMock()
        mock_post_response.status_code = 201
        mock_post.return_value = mock_post_response
        
        mock_put_response = MagicMock()
        mock_put_response.status_code = 200
        mock_put.return_value = mock_put_response
        
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
        )
        
        stats = exporter.export_scan_results(
            table_qualified_name="postgres.public.users",
            scan_report=sample_scan_report,
        )
        
        assert stats["fields_processed"] == 2
        assert stats["classifications_added"] > 0
        assert stats["attributes_added"] > 0
        assert len(stats["errors"]) == 0
    
    @patch('requests.get')
    def test_export_with_min_confidence(self, mock_get, sample_scan_report):
        """Test exporting with minimum confidence filter."""
        # Mock entity resolution
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "entity": {
                "guid": "test-guid-123"
            }
        }
        mock_get.return_value = mock_get_response
        
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        
        stats = exporter.export_scan_results(
            table_qualified_name="postgres.public.users",
            scan_report=sample_scan_report,
            min_confidence=98.0,  # Higher threshold
        )
        
        # Should filter out phone (95.2 < 98.0)
        assert stats["fields_processed"] >= 1  # At least email should pass
    
    @patch('requests.get')
    @patch('requests.post')
    def test_add_classification(self, mock_post, mock_get):
        """Test adding classification to entity."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        
        # Mock getting existing classifications (empty)
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {"list": []}
        mock_get.return_value = mock_get_response
        
        # Mock successful POST
        mock_post_response = MagicMock()
        mock_post_response.status_code = 201
        mock_post.return_value = mock_post_response
        
        exporter._add_classification("test-guid-123", "PII")
        
        # Verify POST was called
        assert mock_post.called
        call_args = mock_post.call_args
        assert "test-guid-123" in call_args[0][0]
    
    @patch('requests.get')
    @patch('requests.put')
    def test_add_attributes(self, mock_put, mock_get):
        """Test adding attributes to entity."""
        exporter = AtlasExporter(
            atlas_url="http://localhost:21000",
        )
        
        # Mock getting entity
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "entity": {
                "guid": "test-guid-123",
                "attributes": {}
            }
        }
        mock_get.return_value = mock_get_response
        
        # Mock successful PUT
        mock_put_response = MagicMock()
        mock_put_response.status_code = 200
        mock_put.return_value = mock_put_response
        
        attributes = {
            "metacrafter_confidence": "99.5",
            "metacrafter_datatype": "email"
        }
        exporter._add_attributes("test-guid-123", attributes)
        
        # Verify PUT was called
        assert mock_put.called
        call_args = mock_put.call_args
        assert "test-guid-123" in call_args[0][0]


class TestAtlasExporterWithoutRequests:
    """Tests for AtlasExporter when requests library is not available."""
    
    def test_import_error_without_requests(self):
        """Test that ImportError is raised when requests is not available."""
        if REQUESTS_AVAILABLE:
            pytest.skip("requests library is available")
        
        with pytest.raises(ImportError):
            AtlasExporter(
                atlas_url="http://localhost:21000",
            )

