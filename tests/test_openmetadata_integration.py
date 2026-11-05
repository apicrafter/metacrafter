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


@pytest.mark.skipif(not OPENMETADATA_AVAILABLE, reason="OpenMetadata REST client not installed")
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
        
        # PII comes from the match (registry-enriched), not from a list of datatypes
        field_info = {
            "tags": [],
            "matches": [{"dataclass": "rusnils", "datatype": "rusnils", "is_pii": True, "pii_source": "registry"}],
        }
        tags = exporter._extract_pii_tags(field_info)
        assert "PII" in tags

        field_info = {"tags": [], "matches": [{"dataclass": "email", "is_pii": False}]}
        assert "PII" not in exporter._extract_pii_tags(field_info)
    
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
    
    def test_export_rest_preserves_metadata(self, sample_scan_report):
        exporter = OpenMetadataExporter("http://catalog.invalid/api", token="test")
        table = {"id": "table-id", "columns": [
            {"name": "email", "description": "preserve", "tags": [{"tagFQN": "Existing.Label", "source": "Classification"}], "extension": {"owner": "team"}},
            {"name": "phone", "tags": [], "extension": {}},
        ]}
        response = Mock()
        response.json.return_value = table
        response.raise_for_status.return_value = None
        exporter.metadata.get = Mock(return_value=response)
        exporter.metadata.patch = Mock(return_value=response)
        stats = exporter.export_scan_results("db.schema.users", sample_scan_report)
        assert stats["failed"] == 0 and stats["succeeded"] == stats["attempted"] == 6
        assert table["columns"][0]["description"] == "preserve"
        assert any(t["tagFQN"] == "Existing.Label" for t in table["columns"][0]["tags"])
        assert table["columns"][0]["extension"]["owner"] == "team"
        patch = exporter.metadata.patch.call_args_list[0].kwargs
        assert patch["headers"]["Content-Type"] == "application/json-patch+json"
        assert patch["json"][0]["op"] == "test"

    def test_failed_patch_is_not_counted(self, sample_scan_report):
        exporter = OpenMetadataExporter("http://catalog.invalid/api")
        response = Mock()
        response.json.return_value = {"id": "id", "columns": [{"name": "email", "tags": []}, {"name": "phone", "tags": []}]}
        exporter.metadata.get = Mock(return_value=response)
        exporter.metadata.patch = Mock(side_effect=RuntimeError("HTTP 500"))
        stats = exporter.export_scan_results("users", sample_scan_report, link_glossary_terms=False, add_properties=False)
        assert stats["tags_added"] == stats["succeeded"] == 0
        assert stats["failed"] == 2 and len(stats["errors"]) == 2

    def test_dry_run_and_confidence_do_not_contact_server(self, sample_scan_report):
        exporter = OpenMetadataExporter("http://catalog.invalid/api")
        exporter.metadata.get = Mock(side_effect=AssertionError("No reads in dry run"))
        stats = exporter.export_scan_results("users", sample_scan_report, min_confidence=98, dry_run=True)
        assert stats["fields_processed"] == 1 and stats["attempted"] == 0
        assert stats["planned"]

    def test_unknown_column_reports_failure(self, sample_scan_report):
        exporter = OpenMetadataExporter("http://catalog.invalid/api")
        response = Mock()
        response.json.return_value = {"id": "id", "columns": []}
        exporter.metadata.get = Mock(return_value=response)
        exporter.metadata.patch = Mock()
        stats = exporter.export_scan_results("users", sample_scan_report, link_glossary_terms=False, add_properties=False)
        assert stats["failed"] == 2
        exporter.metadata.patch.assert_not_called()
