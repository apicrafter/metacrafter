# -*- coding: utf-8 -*-
"""Tests for LLM provider implementations."""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import os

# Test base provider
def test_base_provider_abstract():
    """Test that BaseLLMProvider is abstract."""
    from metacrafter.classify.llm.providers.base import BaseLLMProvider
    
    # Should not be able to instantiate directly
    with pytest.raises(TypeError):
        BaseLLMProvider()


class TestOpenAIProvider:
    """Tests for OpenAI provider."""
    
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_openai_provider_init(self, mock_openai_class):
        """Test OpenAI provider initialization."""
        from metacrafter.classify.llm.providers.openai import OpenAIProvider
        
        provider = OpenAIProvider(api_key="test-key", model="gpt-4o-mini")
        assert provider.model == "gpt-4o-mini"
        assert provider.api_key == "test-key"
        assert provider.supports_json_mode() is True
        assert provider.get_default_model() == "gpt-4o-mini"
    
    @patch.dict(os.environ, {'OPENAI_API_KEY': 'env-key'})
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_openai_provider_env_key(self, mock_openai_class):
        """Test OpenAI provider uses env var for API key."""
        from metacrafter.classify.llm.providers.openai import OpenAIProvider
        
        provider = OpenAIProvider()
        assert provider.api_key == "env-key"
    
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_openai_provider_no_key(self, mock_openai_class):
        """Test OpenAI provider raises error without API key."""
        from metacrafter.classify.llm.providers.openai import OpenAIProvider
        
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="OpenAI API key required"):
                OpenAIProvider()
    
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_openai_provider_classify(self, mock_openai_class):
        """Test OpenAI provider classification."""
        from metacrafter.classify.llm.providers.openai import OpenAIProvider
        
        # Mock OpenAI client
        mock_client = Mock()
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = '{"datatype_id": "email", "confidence": 0.95, "reason": "Matches email pattern"}'
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        provider = OpenAIProvider(api_key="test-key")
        provider.client = mock_client
        
        result = provider.classify("Test prompt")
        assert result["datatype_id"] == "email"
        assert result["confidence"] == 0.95
        assert "reason" in result


class TestOpenRouterProvider:
    """Tests for OpenRouter provider."""
    
    @patch('metacrafter.classify.llm.providers.openrouter.OpenAI')
    def test_openrouter_provider_init(self, mock_openai_class):
        """Test OpenRouter provider initialization."""
        from metacrafter.classify.llm.providers.openrouter import OpenRouterProvider
        
        provider = OpenRouterProvider(api_key="test-key", model="openai/gpt-4o-mini")
        assert provider.model == "openai/gpt-4o-mini"
        assert provider.get_default_model() == "openai/gpt-4o-mini"
        assert provider.supports_json_mode() is True
        
        # Verify OpenAI was called with correct base URL
        mock_openai_class.assert_called_once()
        call_kwargs = mock_openai_class.call_args[1]
        assert call_kwargs["base_url"] == "https://openrouter.ai/api/v1"
        assert "HTTP-Referer" in call_kwargs["default_headers"]
        assert "X-Title" in call_kwargs["default_headers"]


class TestOllamaProvider:
    """Tests for Ollama provider."""
    
    def test_ollama_provider_init(self):
        """Test Ollama provider initialization."""
        from metacrafter.classify.llm.providers.ollama import OllamaProvider
        
        provider = OllamaProvider(base_url="http://localhost:11434", model="llama3")
        assert provider.model == "llama3"
        assert provider.base_url == "http://localhost:11434"
        assert provider.api_url == "http://localhost:11434/api/chat"
        assert provider.get_default_model() == "llama3"
        assert provider.supports_json_mode() is True
    
    @patch('metacrafter.classify.llm.providers.ollama.requests.post')
    def test_ollama_provider_classify(self, mock_post):
        """Test Ollama provider classification."""
        from metacrafter.classify.llm.providers.ollama import OllamaProvider
        
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "message": {"content": '{"datatype_id": "email", "confidence": 0.9, "reason": "test"}'}
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        provider = OllamaProvider()
        result = provider.classify("Test prompt")
        
        assert result["datatype_id"] == "email"
        assert result["confidence"] == 0.9
        mock_post.assert_called_once()
        call_data = mock_post.call_args[1]["json"]
        assert call_data["model"] == "llama3"
        assert call_data["format"] == "json"
    
    @patch('metacrafter.classify.llm.providers.ollama.requests.post')
    def test_ollama_provider_connection_error(self, mock_post):
        """Test Ollama provider handles connection errors."""
        from metacrafter.classify.llm.providers.ollama import OllamaProvider
        import requests
        
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection refused")
        
        provider = OllamaProvider()
        with pytest.raises(requests.exceptions.ConnectionError):
            provider.classify("Test prompt")


class TestLMStudioProvider:
    """Tests for LM Studio provider."""
    
    @patch('metacrafter.classify.llm.providers.lmstudio.OpenAI')
    def test_lmstudio_provider_init(self, mock_openai_class):
        """Test LM Studio provider initialization."""
        from metacrafter.classify.llm.providers.lmstudio import LMStudioProvider
        
        provider = LMStudioProvider(base_url="http://localhost:1234/v1", model="local-model")
        assert provider.model == "local-model"
        assert provider.base_url == "http://localhost:1234/v1"
        assert provider.get_default_model() == "local-model"
        assert provider.supports_json_mode() is True
        
        # Verify OpenAI was called with correct base URL
        mock_openai_class.assert_called_once()
        call_kwargs = mock_openai_class.call_args[1]
        assert call_kwargs["base_url"] == "http://localhost:1234/v1"


class TestPerplexityProvider:
    """Tests for Perplexity provider."""
    
    def test_perplexity_provider_init(self):
        """Test Perplexity provider initialization."""
        from metacrafter.classify.llm.providers.perplexity import PerplexityProvider
        
        provider = PerplexityProvider(api_key="test-key", model="llama-3.1-sonar-small-128k-online")
        assert provider.model == "llama-3.1-sonar-small-128k-online"
        assert provider.api_key == "test-key"
        assert provider.get_default_model() == "llama-3.1-sonar-small-128k-online"
        assert provider.supports_json_mode() is False  # Perplexity doesn't have explicit JSON mode
    
    @patch.dict(os.environ, {'PERPLEXITY_API_KEY': 'env-key'})
    def test_perplexity_provider_env_key(self):
        """Test Perplexity provider uses env var for API key."""
        from metacrafter.classify.llm.providers.perplexity import PerplexityProvider
        
        provider = PerplexityProvider()
        assert provider.api_key == "env-key"
    
    @patch('metacrafter.classify.llm.providers.perplexity.requests.post')
    def test_perplexity_provider_classify(self, mock_post):
        """Test Perplexity provider classification."""
        from metacrafter.classify.llm.providers.perplexity import PerplexityProvider
        
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "choices": [{"message": {"content": '{"datatype_id": "email", "confidence": 0.85, "reason": "test"}'}}]
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        provider = PerplexityProvider(api_key="test-key")
        result = provider.classify("Test prompt")
        
        assert result["datatype_id"] == "email"
        assert result["confidence"] == 0.85
        mock_post.assert_called_once()
        call_headers = mock_post.call_args[1]["headers"]
        assert call_headers["Authorization"] == "Bearer test-key"


class TestProviderFactory:
    """Tests for provider factory."""
    
    def test_get_provider_openai(self):
        """Test getting OpenAI provider."""
        from metacrafter.classify.llm.providers import get_provider
        
        with patch('metacrafter.classify.llm.providers.openai.OpenAI'):
            provider = get_provider("openai", api_key="test-key")
            assert provider is not None
            assert provider.model == "gpt-4o-mini"  # Default model
    
    def test_get_provider_ollama(self):
        """Test getting Ollama provider."""
        from metacrafter.classify.llm.providers import get_provider
        
        provider = get_provider("ollama", base_url="http://localhost:11434")
        assert provider is not None
        assert provider.base_url == "http://localhost:11434"
    
    def test_get_provider_invalid(self):
        """Test getting invalid provider raises error."""
        from metacrafter.classify.llm.providers import get_provider
        
        with pytest.raises(ValueError, match="Unknown provider"):
            get_provider("invalid_provider")
    
    def test_list_providers(self):
        """Test listing available providers."""
        from metacrafter.classify.llm.providers import list_providers
        
        providers = list_providers()
        assert isinstance(providers, list)
        assert "openai" in providers
        assert "ollama" in providers


class TestProviderJSONParsing:
    """Tests for JSON parsing in providers."""
    
    @patch('metacrafter.classify.llm.providers.openai.OpenAI')
    def test_parse_json_from_text(self, mock_openai_class):
        """Test parsing JSON from text response."""
        from metacrafter.classify.llm.providers.openai import OpenAIProvider
        
        # Mock client with text response (no JSON mode)
        mock_client = Mock()
        mock_response = Mock()
        mock_response.choices = [Mock()]
        # Response with extra text around JSON
        mock_response.choices[0].message.content = 'Here is the result: {"datatype_id": "phone", "confidence": 0.8, "reason": "test"}'
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        provider = OpenAIProvider(api_key="test-key")
        provider.client = mock_client
        
        result = provider.classify("Test prompt")
        assert result["datatype_id"] == "phone"
        assert result["confidence"] == 0.8

