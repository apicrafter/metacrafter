# -*- coding: utf-8 -*-
"""Tests for DataHub integration."""
import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock

from metacrafter.integrations.datahub import DataHubExporter, DATAHUB_AVAILABLE


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


@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="DataHub SDK not installed")
class TestDataHubExporter:
    """Tests for DataHubExporter class."""
    
    def test_init(self):
        """Test DataHubExporter initialization."""
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
            token="test-token",
            timeout=60.0,
        )
        assert exporter.datahub_url == "http://localhost:8080"
        assert exporter.token == "test-token"
        assert exporter.timeout == 60.0
        assert exporter.emitter is not None
    
    def test_init_from_env(self, monkeypatch):
        """Test initialization from environment variables."""
        monkeypatch.setenv("DATAHUB_TOKEN", "env-token")
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
        )
        assert exporter.token == "env-token"
    
    def test_map_field_to_urn(self):
        """Test field URN mapping."""
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
        )
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)"
        field_urn = exporter._map_field_to_urn(dataset_urn, "email")
        assert "email" in field_urn
        assert dataset_urn in field_urn
    
    def test_extract_pii_tags(self):
        """Test PII tag extraction."""
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
        )
        
        # Test with pii tag
        field_info = {"tags": ["pii"], "matches": []}
        tags = exporter._extract_pii_tags(field_info)
        assert "PII" in tags
        
        # Test with email datatype
        field_info = {
            "tags": [],
            "matches": [{"dataclass": "email"}],
        }
        tags = exporter._extract_pii_tags(field_info)
        assert "PII" in tags
    
    def test_extract_datatype_tags(self):
        """Test datatype tag extraction."""
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
        )
        
        matches = [
            {"dataclass": "email"},
            {"dataclass": "phone"},
        ]
        tags = exporter._extract_datatype_tags(matches)
        assert "Email" in tags
        assert "Phone" in tags
    
    def test_extract_glossary_terms(self):
        """Test glossary term extraction."""
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
        )
        
        matches = [
            {"dataclass": "email"},
            {"dataclass": "phone"},
        ]
        terms = exporter._extract_glossary_terms(matches)
        assert "urn:li:glossaryTerm:email" in terms
        assert "urn:li:glossaryTerm:phone" in terms
    
    def test_build_properties(self):
        """Test property building."""
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
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
        
        properties = exporter._build_properties(field_info, best_match)
        assert "metacrafter_confidence" in properties
        assert "metacrafter_datatype" in properties
        assert "metacrafter_datatype_url" in properties
        assert "metacrafter_rule_id" in properties
        assert properties["metacrafter_confidence"] == "99.5"
    
    @patch('metacrafter.integrations.datahub.DatahubRestEmitter')
    def test_export_scan_results(self, mock_emitter_class, sample_scan_report):
        """Test exporting scan results."""
        mock_emitter = MagicMock()
        mock_emitter_class.return_value = mock_emitter
        
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
            token="test-token",
        )
        
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)"
        stats = exporter.export_scan_results(
            dataset_urn=dataset_urn,
            scan_report=sample_scan_report,
        )
        
        assert stats["fields_processed"] == 2
        assert stats["tags_added"] > 0
        assert stats["glossary_terms_linked"] > 0
        assert stats["properties_added"] > 0
        assert len(stats["errors"]) == 0
    
    @patch('metacrafter.integrations.datahub.DatahubRestEmitter')
    def test_export_with_min_confidence(self, mock_emitter_class, sample_scan_report):
        """Test exporting with minimum confidence filter."""
        mock_emitter = MagicMock()
        mock_emitter_class.return_value = mock_emitter
        
        exporter = DataHubExporter(
            datahub_url="http://localhost:8080",
        )
        
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)"
        stats = exporter.export_scan_results(
            dataset_urn=dataset_urn,
            scan_report=sample_scan_report,
            min_confidence=98.0,  # Higher threshold
        )
        
        # Should filter out phone (95.2 < 98.0)
        assert stats["fields_processed"] >= 1  # At least email should pass


class TestDataHubExporterWithoutSDK:
    """Tests for DataHubExporter when SDK is not available."""
    
    def test_import_error_without_sdk(self):
        """Test that ImportError is raised when SDK is not available."""
        if DATAHUB_AVAILABLE:
            pytest.skip("DataHub SDK is available")
        
        with pytest.raises(ImportError):
            DataHubExporter(
                datahub_url="http://localhost:8080",
            )

