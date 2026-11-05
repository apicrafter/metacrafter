# -*- coding: utf-8 -*-
"""LLM providers: one OpenAI-compatible client with presets, plus Anthropic.

Provider names: ``openai``, ``openrouter``, ``perplexity``, ``ollama``, ``lmstudio``
(presets of :class:`OpenAICompatibleProvider`), ``openai-compatible`` (no preset
defaults beyond OpenAI's; pass ``base_url``) and ``anthropic``.
"""
from typing import Dict, Type

from .base import BaseLLMProvider
from .lmstudio import LMStudioProvider
from .ollama import OllamaProvider
from .openai import OpenAIProvider
from .openai_compatible import PRESETS, OpenAICompatibleProvider
from .openrouter import OpenRouterProvider
from .perplexity import PerplexityProvider


def _anthropic_provider(**kwargs):
    from .anthropic import AnthropicProvider

    return AnthropicProvider(**kwargs)


_PROVIDERS: Dict[str, object] = {
    "openai": OpenAIProvider,
    "openrouter": OpenRouterProvider,
    "ollama": OllamaProvider,
    "lmstudio": LMStudioProvider,
    "perplexity": PerplexityProvider,
    "openai-compatible": OpenAICompatibleProvider,
    "anthropic": _anthropic_provider,
}


def get_provider(provider_name: str, **kwargs) -> BaseLLMProvider:
    """Return a provider instance by name (see module docstring)."""
    provider_name = provider_name.lower()
    if provider_name not in _PROVIDERS:
        raise ValueError(
            f"Unknown provider: {provider_name}. Supported providers: {', '.join(_PROVIDERS)}"
        )
    return _PROVIDERS[provider_name](**kwargs)


def list_providers() -> list:
    """List all provider names."""
    return list(_PROVIDERS)


__all__ = [
    "BaseLLMProvider",
    "OpenAICompatibleProvider",
    "OpenAIProvider",
    "OpenRouterProvider",
    "OllamaProvider",
    "LMStudioProvider",
    "PerplexityProvider",
    "PRESETS",
    "get_provider",
    "list_providers",
]
