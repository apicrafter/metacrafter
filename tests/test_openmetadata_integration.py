# -*- coding: utf-8 -*-
"""Tests for OpenMetadata integration."""
import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock

from metacrafter.integrations.openmetadata import OpenMetadataExporter, OPENMETADATA_AVAILABLE


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


@pytest.mark.skipif(not OPENMETADATA_AVAILABLE, reason="OpenMetadata SDK not installed")
class TestOpenMetadataExporter:
    """Tests for OpenMetadataExporter class."""
    
    def test_init(self):
        """Test OpenMetadataExporter initialization."""
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
            token="test-token",
            timeout=60.0,
        )
        assert exporter.openmetadata_url == "http://localhost:8585/api"
        assert exporter.token == "test-token"
        assert exporter.timeout == 60.0
        assert exporter.metadata is not None
    
    def test_init_from_env(self, monkeypatch):
        """Test initialization from environment variables."""
        monkeypatch.setenv("OPENMETADATA_TOKEN", "env-token")
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
        )
        assert exporter.token == "env-token"
    
    def test_map_field_to_fqn(self):
        """Test field FQN mapping."""
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
        )
        table_fqn = "postgres.default.public.users"
        column_fqn = exporter._map_field_to_fqn(table_fqn, "email")
        assert column_fqn == "postgres.default.public.users.email"
        assert "email" in column_fqn
        assert table_fqn in column_fqn
    
    def test_extract_pii_tags(self):
        """Test PII tag extraction."""
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
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
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
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
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
        )
        
        matches = [
            {"dataclass": "email"},
            {"dataclass": "phone"},
        ]
        terms = exporter._extract_glossary_terms(matches)
        assert "GlossaryTerm.email" in terms
        assert "GlossaryTerm.phone" in terms
    
    def test_build_properties(self):
        """Test property building."""
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
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
    
    @patch('metacrafter.integrations.openmetadata.OpenMetadata')
    def test_export_scan_results(self, mock_metadata_class, sample_scan_report):
        """Test exporting scan results."""
        # Create mock table entity with columns
        from unittest.mock import Mock
        
        mock_column_email = Mock()
        mock_column_email.name.__root__ = "email"
        mock_column_email.tags = []
        mock_column_email.glossaryTerms = []
        mock_column_email.customProperties = {}
        
        mock_column_phone = Mock()
        mock_column_phone.name.__root__ = "phone"
        mock_column_phone.tags = []
        mock_column_phone.glossaryTerms = []
        mock_column_phone.customProperties = {}
        
        mock_table = Mock()
        mock_table.columns = [mock_column_email, mock_column_phone]
        
        mock_metadata = Mock()
        mock_metadata.get_by_name.return_value = mock_table
        mock_metadata.patch_column = Mock()
        mock_metadata_class.return_value = mock_metadata
        
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
            token="test-token",
        )
        exporter.metadata = mock_metadata
        
        table_fqn = "postgres.default.public.users"
        stats = exporter.export_scan_results(
            table_fqn=table_fqn,
            scan_report=sample_scan_report,
        )
        
        assert stats["fields_processed"] == 2
        assert stats["tags_added"] > 0
        assert stats["glossary_terms_linked"] > 0
        assert stats["properties_added"] > 0
        assert len(stats["errors"]) == 0
    
    @patch('metacrafter.integrations.openmetadata.OpenMetadata')
    def test_export_with_min_confidence(self, mock_metadata_class, sample_scan_report):
        """Test exporting with minimum confidence filter."""
        # Create mock table entity with columns
        mock_column_email = Mock()
        mock_column_email.name.__root__ = "email"
        mock_column_email.tags = []
        mock_column_email.glossaryTerms = []
        mock_column_email.customProperties = {}
        
        mock_column_phone = Mock()
        mock_column_phone.name.__root__ = "phone"
        mock_column_phone.tags = []
        mock_column_phone.glossaryTerms = []
        mock_column_phone.customProperties = {}
        
        mock_table = Mock()
        mock_table.columns = [mock_column_email, mock_column_phone]
        
        mock_metadata = Mock()
        mock_metadata.get_by_name.return_value = mock_table
        mock_metadata.patch_column = Mock()
        mock_metadata_class.return_value = mock_metadata
        
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
        )
        exporter.metadata = mock_metadata
        
        table_fqn = "postgres.default.public.users"
        stats = exporter.export_scan_results(
            table_fqn=table_fqn,
            scan_report=sample_scan_report,
            min_confidence=98.0,  # Higher threshold
        )
        
        # Should filter out phone (95.2 < 98.0)
        assert stats["fields_processed"] >= 1  # At least email should pass
    
    @patch('metacrafter.integrations.openmetadata.OpenMetadata')
    def test_export_table_not_found(self, mock_metadata_class, sample_scan_report):
        """Test export when table is not found."""
        mock_metadata = Mock()
        mock_metadata.get_by_name.return_value = None
        mock_metadata_class.return_value = mock_metadata
        
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
        )
        exporter.metadata = mock_metadata
        
        table_fqn = "postgres.default.public.nonexistent"
        stats = exporter.export_scan_results(
            table_fqn=table_fqn,
            scan_report=sample_scan_report,
        )
        
        assert stats["fields_processed"] == 0
        assert len(stats["errors"]) > 0
    
    @patch('metacrafter.integrations.openmetadata.OpenMetadata')
    def test_export_column_not_found(self, mock_metadata_class, sample_scan_report):
        """Test export when column is not found in table."""
        mock_column = Mock()
        mock_column.name.__root__ = "other_column"
        
        mock_table = Mock()
        mock_table.columns = [mock_column]
        
        mock_metadata = Mock()
        mock_metadata.get_by_name.return_value = mock_table
        mock_metadata.patch_column = Mock()
        mock_metadata_class.return_value = mock_metadata
        
        exporter = OpenMetadataExporter(
            openmetadata_url="http://localhost:8585/api",
        )
        exporter.metadata = mock_metadata
        
        table_fqn = "postgres.default.public.users"
        stats = exporter.export_scan_results(
            table_fqn=table_fqn,
            scan_report=sample_scan_report,
        )
        
        # Should process but skip columns that don't exist
        assert stats["fields_processed"] == 0


class TestOpenMetadataExporterWithoutSDK:
    """Tests for OpenMetadataExporter when SDK is not available."""
    
    def test_import_error_without_sdk(self):
        """Test that ImportError is raised when SDK is not available."""
        if OPENMETADATA_AVAILABLE:
            pytest.skip("OpenMetadata SDK is available")
        
        with pytest.raises(ImportError):
            OpenMetadataExporter(
                openmetadata_url="http://localhost:8585/api",
            )

