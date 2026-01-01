# -*- coding: utf-8 -*-
"""Tests for LLM integration with CrafterCmd."""
import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Mock dependencies
try:
    import chromadb
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


@pytest.mark.skipif(not OPENAI_AVAILABLE, reason="OpenAI package not available")
@pytest.mark.skipif(not CHROMADB_AVAILABLE, reason="ChromaDB not available")
class TestLLMIntegration:
    """Tests for LLM integration with CrafterCmd."""
    
    def create_test_registry(self, tmp_path):
        """Create a test registry JSONL file."""
        registry_file = tmp_path / "test_registry.jsonl"
        test_data = [
            {
                "id": "email",
                "name": "Email Address",
                "doc": "Electronic mail address",
                "langs": ["en"],
                "categories": ["pii"],
                "country": [],
            }
        ]
        
        with open(registry_file, 'w') as f:
            for item in test_data:
                f.write(json.dumps(item) + '\n')
        
        return registry_file
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_crafter_cmd_with_llm_init(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test CrafterCmd initialization with LLM."""
        from metacrafter.core import CrafterCmd
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock OpenAI clients
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536)]
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        mock_provider_client = Mock()
        mock_provider_response = Mock()
        mock_provider_response.choices = [Mock()]
        mock_provider_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.9, "reason": "test"}'
        mock_provider_client.chat.completions.create.return_value = mock_provider_response
        mock_provider_openai.return_value = mock_provider_client
        
        cmd = CrafterCmd(
            remote=None,
            debug=False,
            use_llm=True,
            llm_provider="openai",
            llm_registry_path=str(registry_file),
            llm_index_path=str(index_path),
            llm_api_key="test-key"
        )
        
        assert cmd.llm_classifier is not None
        assert cmd.classification_mode == "hybrid"
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_crafter_cmd_llm_only_mode(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test CrafterCmd with LLM-only mode."""
        from metacrafter.core import CrafterCmd
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock OpenAI clients
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(2)]
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        mock_provider_client = Mock()
        mock_provider_response = Mock()
        mock_provider_response.choices = [Mock()]
        mock_provider_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.9, "reason": "test"}'
        mock_provider_client.chat.completions.create.return_value = mock_provider_response
        mock_provider_openai.return_value = mock_provider_client
        
        cmd = CrafterCmd(
            remote=None,
            debug=False,
            llm_only=True,
            llm_provider="openai",
            llm_registry_path=str(registry_file),
            llm_index_path=str(index_path),
            llm_api_key="test-key"
        )
        
        assert cmd.classification_mode == "llm"
        assert cmd.llm_classifier is not None
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_scan_data_llm_only_mode(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test scan_data with LLM-only mode."""
        from metacrafter.core import CrafterCmd
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock embeddings
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(3)]
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        # Mock LLM response
        mock_provider_client = Mock()
        mock_provider_response = Mock()
        mock_provider_response.choices = [Mock()]
        mock_provider_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.9, "reason": "test"}'
        mock_provider_client.chat.completions.create.return_value = mock_provider_response
        mock_provider_openai.return_value = mock_provider_client
        
        cmd = CrafterCmd(
            remote=None,
            debug=False,
            llm_only=True,
            llm_provider="openai",
            llm_registry_path=str(registry_file),
            llm_index_path=str(index_path),
            llm_api_key="test-key"
        )
        
        items = [
            {"email": "test@example.com", "name": "John"},
            {"email": "user@domain.org", "name": "Jane"},
        ]
        
        report = cmd.scan_data(
            items,
            limit=10,
            classification_mode="llm"
        )
        
        assert report is not None
        assert "results" in report
        assert "data" in report
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_scan_data_hybrid_mode(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test scan_data with hybrid mode."""
        from metacrafter.core import CrafterCmd
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock embeddings
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(3)]
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        # Mock LLM response
        mock_provider_client = Mock()
        mock_provider_response = Mock()
        mock_provider_response.choices = [Mock()]
        mock_provider_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.9, "reason": "test"}'
        mock_provider_client.chat.completions.create.return_value = mock_provider_response
        mock_provider_openai.return_value = mock_provider_client
        
        cmd = CrafterCmd(
            remote=None,
            debug=False,
            use_llm=True,
            llm_provider="openai",
            llm_registry_path=str(registry_file),
            llm_index_path=str(index_path),
            llm_api_key="test-key"
        )
        
        items = [
            {"email": "test@example.com", "unknown_field": "xyz123"},
            {"email": "user@domain.org", "unknown_field": "abc456"},
        ]
        
        report = cmd.scan_data(
            items,
            limit=10,
            classification_mode="hybrid",
            llm_min_confidence=50.0
        )
        
        assert report is not None
        assert "results" in report
        assert "data" in report
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_scan_data_rules_mode_default(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test scan_data with rules-only mode (default)."""
        from metacrafter.core import CrafterCmd
        
        cmd = CrafterCmd(remote=None, debug=False)
        
        items = [
            {"email": "test@example.com", "name": "John"},
        ]
        
        report = cmd.scan_data(
            items,
            limit=10,
            classification_mode="rules"
        )
        
        assert report is not None
        assert "results" in report
        assert "data" in report
        # Should not use LLM
        assert cmd.llm_classifier is None
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_llm_fallback_on_error(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test that LLM errors don't crash the scan in hybrid mode."""
        from metacrafter.core import CrafterCmd
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock embeddings
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(3)]
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        # Mock LLM to raise error
        mock_provider_client = Mock()
        mock_provider_client.chat.completions.create.side_effect = Exception("API Error")
        mock_provider_openai.return_value = mock_provider_client
        
        cmd = CrafterCmd(
            remote=None,
            debug=False,
            use_llm=True,
            llm_provider="openai",
            llm_registry_path=str(registry_file),
            llm_index_path=str(index_path),
            llm_api_key="test-key"
        )
        
        items = [
            {"email": "test@example.com", "unknown_field": "xyz123"},
        ]
        
        # Should not raise exception, should fall back to rule-based results
        report = cmd.scan_data(
            items,
            limit=10,
            classification_mode="hybrid"
        )
        
        assert report is not None
        assert "results" in report
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_llm_only_mode_without_classifier(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test LLM-only mode falls back to rules if classifier not available."""
        from metacrafter.core import CrafterCmd
        
        cmd = CrafterCmd(remote=None, debug=False)
        # No LLM classifier initialized
        
        items = [
            {"email": "test@example.com"},
        ]
        
        # Should fall back to rule-based
        report = cmd.scan_data(
            items,
            limit=10,
            classification_mode="llm"
        )
        
        assert report is not None
        assert "results" in report


class TestLLMResultFormat:
    """Tests for LLM result format conversion."""
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_llm_result_to_rule_result(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test conversion of LLM results to RuleResult format."""
        from metacrafter.core import CrafterCmd
        from metacrafter.classify.processor import RuleResult
        import tempfile
        import json
        
        registry_file = tmp_path / "test_registry.jsonl"
        with open(registry_file, 'w') as f:
            f.write('{"id": "email", "name": "Email", "doc": "Email address", "langs": ["en"]}\n')
        
        index_path = tmp_path / "test_index"
        
        # Mock embeddings
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(3)]
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        # Mock LLM response
        mock_provider_client = Mock()
        mock_provider_response = Mock()
        mock_provider_response.choices = [Mock()]
        mock_provider_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.85, "reason": "test"}'
        mock_provider_client.chat.completions.create.return_value = mock_provider_response
        mock_provider_openai.return_value = mock_provider_client
        
        cmd = CrafterCmd(
            remote=None,
            debug=False,
            llm_only=True,
            llm_provider="openai",
            llm_registry_path=str(registry_file),
            llm_index_path=str(index_path),
            llm_api_key="test-key"
        )
        
        items = [{"email": "test@example.com"}]
        report = cmd.scan_data(items, limit=10, classification_mode="llm")
        
        # Check that results are in correct format
        assert "data" in report
        if report["data"]:
            field_data = report["data"][0]
            assert "field" in field_data
            assert "matches" in field_data
            if field_data["matches"]:
                match = field_data["matches"][0]
                assert "dataclass" in match or "key" in match
                assert "confidence" in match
                assert "ruletype" in match or match.get("ruletype") == "llm"

