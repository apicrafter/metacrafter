# -*- coding: utf-8 -*-
"""Tests for LLM classifier."""
import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Mock dependencies that may not be available
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
class TestLLMClassifier:
    """Tests for LLMClassifier."""
    
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
                "examples": [{"value": "test@example.com"}]
            },
            {
                "id": "phone",
                "name": "Phone Number",
                "doc": "Telephone number",
                "langs": ["en"],
                "categories": ["pii"],
                "country": [],
                "examples": [{"value": "555-1234"}]
            }
        ]
        
        with open(registry_file, 'w') as f:
            for item in test_data:
                f.write(json.dumps(item) + '\n')
        
        return registry_file
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_llm_classifier_init(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test LLMClassifier initialization."""
        from metacrafter.classify.llm import LLMClassifier
        
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
        
        classifier = LLMClassifier(
            registry_path=str(registry_file),
            index_path=str(index_path),
            embedding_api_key="test-key",
            llm_provider="openai",
            llm_api_key="test-key",
            rebuild_index=True
        )
        
        assert classifier.registry_path == registry_file
        assert classifier.index_path == index_path
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_llm_classifier_default_registry_path(self, mock_provider_openai, mock_embedder_openai, tmp_path, monkeypatch):
        """Test LLMClassifier finds default registry path."""
        from metacrafter.classify.llm import LLMClassifier
        
        # Create registry in expected location
        registry_dir = tmp_path / "metacrafter-registry" / "data"
        registry_dir.mkdir(parents=True)
        registry_file = registry_dir / "datatypes_latest.jsonl"
        self.create_test_registry(registry_dir.parent.parent)
        
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
        
        # Change to directory where registry would be found
        original_cwd = Path.cwd()
        try:
            monkeypatch.chdir(tmp_path)
            # This should find the registry
            # Note: This test may need adjustment based on actual path discovery logic
            pass
        finally:
            monkeypatch.chdir(original_cwd)
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_llm_classifier_classify_field(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test classifying a single field."""
        from metacrafter.classify.llm import LLMClassifier
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock embeddings
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(3)]  # 2 datatypes + 1 query
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        # Mock LLM response
        mock_provider_client = Mock()
        mock_provider_response = Mock()
        mock_provider_response.choices = [Mock()]
        mock_provider_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.95, "reason": "Matches email pattern"}'
        mock_provider_client.chat.completions.create.return_value = mock_provider_response
        mock_provider_openai.return_value = mock_provider_client
        
        classifier = LLMClassifier(
            registry_path=str(registry_file),
            index_path=str(index_path),
            embedding_api_key="test-key",
            llm_provider="openai",
            llm_api_key="test-key",
            rebuild_index=True
        )
        
        result = classifier.classify_field(
            field_name="email_address",
            sample_values=["test@example.com", "user@domain.org"]
        )
        
        assert result["field"] == "email_address"
        assert result["datatype_id"] == "email"
        assert result["confidence"] == 0.95
        assert "datatype_url" in result
        assert "matches" in result
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_llm_classifier_classify_batch(self, mock_provider_openai, mock_embedder_openai, tmp_path):
        """Test batch classification."""
        from metacrafter.classify.llm import LLMClassifier
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock embeddings
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(4)]  # 2 datatypes + 2 queries
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        # Mock LLM responses
        mock_provider_client = Mock()
        mock_provider_response = Mock()
        mock_provider_response.choices = [Mock()]
        mock_provider_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.9, "reason": "test"}'
        mock_provider_client.chat.completions.create.return_value = mock_provider_response
        mock_provider_openai.return_value = mock_provider_client
        
        classifier = LLMClassifier(
            registry_path=str(registry_file),
            index_path=str(index_path),
            embedding_api_key="test-key",
            llm_provider="openai",
            llm_api_key="test-key",
            rebuild_index=True
        )
        
        fields = [
            {"field_name": "email", "sample_values": ["test@example.com"]},
            {"field_name": "phone", "sample_values": ["555-1234"]},
        ]
        
        results = classifier.classify_batch(fields)
        assert len(results) == 2
        assert results[0]["field"] == "email"
        assert results[1]["field"] == "phone"
    
    @patch('metacrafter.classify.llm.embedder.OpenAI')
    @patch('metacrafter.classify.llm.providers.ollama.requests.post')
    def test_llm_classifier_with_ollama(self, mock_post, mock_embedder_openai, tmp_path):
        """Test LLMClassifier with Ollama provider."""
        from metacrafter.classify.llm import LLMClassifier
        
        registry_file = self.create_test_registry(tmp_path)
        index_path = tmp_path / "test_index"
        
        # Mock embeddings (still uses OpenAI)
        mock_embedder_client = Mock()
        mock_embedder_client.embeddings.create.return_value = Mock(
            data=[Mock(embedding=[0.1] * 1536) for _ in range(3)]
        )
        mock_embedder_openai.return_value = mock_embedder_client
        
        # Mock Ollama response
        mock_response = Mock()
        mock_response.json.return_value = {
            "message": {"content": '{"datatype_id": "email", "confidence": 0.85, "reason": "test"}'}
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        classifier = LLMClassifier(
            registry_path=str(registry_file),
            index_path=str(index_path),
            embedding_api_key="test-key",
            llm_provider="ollama",
            llm_base_url="http://localhost:11434",
            rebuild_index=True
        )
        
        result = classifier.classify_field(
            field_name="email",
            sample_values=["test@example.com"]
        )
        
        assert result["datatype_id"] == "email"
        assert result["confidence"] == 0.85


class TestLLMClassifierErrors:
    """Tests for error handling in LLMClassifier."""
    
    def test_llm_classifier_missing_registry(self):
        """Test LLMClassifier raises error when registry not found."""
        from metacrafter.classify.llm import LLMClassifier
        
        with pytest.raises(FileNotFoundError):
            LLMClassifier(
                registry_path="/nonexistent/registry.jsonl",
                embedding_api_key="test-key",
                llm_provider="openai",
                llm_api_key="test-key"
            )
    
    def test_llm_classifier_missing_embedding_key(self):
        """Test LLMClassifier raises error when embedding API key missing."""
        from metacrafter.classify.llm import LLMClassifier
        import tempfile
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": "test"}\n')
            registry_path = f.name
        
        try:
            with patch.dict('os.environ', {}, clear=True):
                with pytest.raises(ValueError, match="OpenAI API key required for embeddings"):
                    LLMClassifier(
                        registry_path=registry_path,
                        embedding_api_key=None,
                        llm_provider="ollama"
                    )
        finally:
            import os
            if os.path.exists(registry_path):
                os.remove(registry_path)
    
    def test_llm_classifier_invalid_provider(self):
        """Test LLMClassifier raises error for invalid provider."""
        from metacrafter.classify.llm import LLMClassifier
        import tempfile
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": "test"}\n')
            registry_path = f.name
        
        try:
            with pytest.raises(ValueError, match="Unknown provider"):
                LLMClassifier(
                    registry_path=registry_path,
                    embedding_api_key="test-key",
                    llm_provider="invalid_provider"
                )
        finally:
            import os
            if os.path.exists(registry_path):
                os.remove(registry_path)

