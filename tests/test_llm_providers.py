# -*- coding: utf-8 -*-
"""LLM providers: OpenAI-compatible presets, structured output fallback, Anthropic."""
import json
from unittest.mock import Mock, patch

import pytest
import requests

from tests.llm_helpers import OPENAI_COMPATIBLE_POST, mock_chat

SCHEMA = {
    "type": "object",
    "properties": {
        "datatype_id": {"anyOf": [{"type": "string", "enum": ["email"]}, {"type": "null"}]},
        "confidence": {"type": "number"},
        "reason": {"type": "string"},
    },
    "required": ["datatype_id", "confidence", "reason"],
    "additionalProperties": False,
}


def test_base_provider_abstract():
    from metacrafter.classify.llm.providers.base import BaseLLMProvider

    with pytest.raises(TypeError):
        BaseLLMProvider()


@pytest.mark.parametrize(
    "name,env,base",
    [
        ("openai", "OPENAI_API_KEY", "https://api.openai.com/v1"),
        ("openrouter", "OPENROUTER_API_KEY", "https://openrouter.ai/api/v1"),
        ("perplexity", "PERPLEXITY_API_KEY", "https://api.perplexity.ai"),
    ],
)
def test_remote_presets_need_keys(name, env, base, monkeypatch):
    from metacrafter.classify.llm.providers import get_provider

    monkeypatch.delenv(env, raising=False)
    with pytest.raises(ValueError, match=env):
        get_provider(name)
    monkeypatch.setenv(env, "k")
    provider = get_provider(name)
    assert provider.base_url == base
    assert provider.api_key == "k"
    assert provider.is_local is False


@pytest.mark.parametrize("name", ["ollama", "lmstudio"])
def test_local_presets(name):
    from metacrafter.classify.llm.providers import get_provider

    provider = get_provider(name)
    assert provider.is_local is True
    assert provider.api_key is None


def test_base_url_override_controls_locality():
    from metacrafter.classify.llm.providers import get_provider

    assert get_provider("openai-compatible", api_key="k", base_url="http://127.0.0.1:8000/v1").is_local
    assert not get_provider("openai-compatible", api_key="k", base_url="https://llm.example.com/v1").is_local


@patch(OPENAI_COMPATIBLE_POST)
def test_classify_sends_json_schema(mock_post):
    from metacrafter.classify.llm.providers import OpenAIProvider

    mock_chat(mock_post, '{"datatype_id": "email", "confidence": 0.9, "reason": "r"}')
    provider = OpenAIProvider(api_key="k")
    result = provider.classify("prompt", schema=SCHEMA)
    assert result["datatype_id"] == "email"
    payload = mock_post.call_args.kwargs["json"]
    assert payload["response_format"]["type"] == "json_schema"
    assert payload["response_format"]["json_schema"]["schema"] == SCHEMA
    assert mock_post.call_args.kwargs["headers"]["Authorization"] == "Bearer k"


@patch(OPENAI_COMPATIBLE_POST)
def test_schema_rejected_falls_back_to_json_object(mock_post):
    from metacrafter.classify.llm.providers import OllamaProvider

    bad = Mock(status_code=400)
    good = Mock(status_code=200)
    good.raise_for_status.return_value = None
    good.json.return_value = {"choices": [{"message": {"content": '{"datatype_id": null, "confidence": 0, "reason": ""}'}}]}
    mock_post.side_effect = [bad, good]
    result = OllamaProvider().classify("prompt", schema=SCHEMA)
    assert result["datatype_id"] is None
    assert mock_post.call_args.kwargs["json"]["response_format"] == {"type": "json_object"}


@patch(OPENAI_COMPATIBLE_POST)
def test_connection_error(mock_post):
    from metacrafter.classify.llm.providers import OllamaProvider

    mock_post.side_effect = requests.exceptions.ConnectionError("refused")
    provider = OllamaProvider(max_retries=1)
    with pytest.raises(ConnectionError):
        provider.classify("prompt")


def test_parse_json_from_text():
    from metacrafter.classify.llm.providers import OllamaProvider

    provider = OllamaProvider()
    assert provider._parse_response('Answer: {"datatype_id": "email", "confidence": 0.5}')["datatype_id"] == "email"


def test_provider_factory():
    from metacrafter.classify.llm.providers import get_provider, list_providers

    names = list_providers()
    assert {"openai", "openrouter", "ollama", "lmstudio", "perplexity", "openai-compatible", "anthropic"} <= set(names)
    with pytest.raises(ValueError, match="Unknown provider"):
        get_provider("nope")


def _anthropic_response(text, stop_reason="end_turn"):
    block = Mock(type="text", text=text)
    return Mock(content=[block], stop_reason=stop_reason, stop_details=None)


def test_anthropic_provider_structured_output_and_fallbacks():
    pytest.importorskip("anthropic")
    from metacrafter.classify.llm.providers.anthropic import AnthropicProvider

    with patch("anthropic.Anthropic") as client_cls:
        client = client_cls.return_value
        client.beta.messages.create.return_value = _anthropic_response('{"datatype_id": "email", "confidence": 0.8, "reason": "r"}')
        provider = AnthropicProvider(api_key="k")
        result = provider.classify("prompt", schema=SCHEMA)
    assert result["datatype_id"] == "email"
    kwargs = client.beta.messages.create.call_args.kwargs
    assert kwargs["model"] == "claude-opus-5-5"
    assert kwargs["output_config"]["format"] == {"type": "json_schema", "schema": SCHEMA}
    assert kwargs["fallbacks"] == "default"
    assert kwargs["betas"] == ["server-side-fallback-2026-07-01"]
    assert "temperature" not in kwargs
    assert provider.is_local is False


def test_anthropic_refusal_is_an_error():
    pytest.importorskip("anthropic")
    from metacrafter.classify.llm.providers.anthropic import AnthropicProvider

    with patch("anthropic.Anthropic") as client_cls:
        client_cls.return_value.beta.messages.create.return_value = _anthropic_response("", stop_reason="refusal")
        provider = AnthropicProvider(api_key="k", max_retries=1)
        with pytest.raises(RuntimeError, match="declined"):
            provider.classify("prompt", schema=SCHEMA)
