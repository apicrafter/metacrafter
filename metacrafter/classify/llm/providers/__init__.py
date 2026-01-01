# -*- coding: utf-8 -*-
"""LLM provider implementations."""
from typing import Dict, Any, Optional, Type

from .base import BaseLLMProvider

# Import providers with graceful handling of missing dependencies
try:
    from .openai import OpenAIProvider
except ImportError:
    OpenAIProvider = None

try:
    from .openrouter import OpenRouterProvider
except ImportError:
    OpenRouterProvider = None

try:
    from .ollama import OllamaProvider
except ImportError:
    OllamaProvider = None

try:
    from .lmstudio import LMStudioProvider
except ImportError:
    LMStudioProvider = None

try:
    from .perplexity import PerplexityProvider
except ImportError:
    PerplexityProvider = None

# Provider registry (only include available providers)
_PROVIDERS: Dict[str, Type[BaseLLMProvider]] = {}
if OpenAIProvider is not None:
    _PROVIDERS["openai"] = OpenAIProvider
if OpenRouterProvider is not None:
    _PROVIDERS["openrouter"] = OpenRouterProvider
if OllamaProvider is not None:
    _PROVIDERS["ollama"] = OllamaProvider
if LMStudioProvider is not None:
    _PROVIDERS["lmstudio"] = LMStudioProvider
if PerplexityProvider is not None:
    _PROVIDERS["perplexity"] = PerplexityProvider


def get_provider(provider_name: str, **kwargs) -> BaseLLMProvider:
    """
    Get a provider instance by name.
    
    Args:
        provider_name: Name of the provider (openai, openrouter, ollama, lmstudio, perplexity)
        **kwargs: Provider-specific configuration parameters
        
    Returns:
        Provider instance
        
    Raises:
        ValueError: If provider name is not recognized
    """
    provider_name = provider_name.lower()
    if provider_name not in _PROVIDERS:
        raise ValueError(
            f"Unknown provider: {provider_name}. "
            f"Supported providers: {', '.join(_PROVIDERS.keys())}"
        )
    
    provider_class = _PROVIDERS[provider_name]
    return provider_class(**kwargs)


def list_providers() -> list:
    """List all available provider names."""
    return list(_PROVIDERS.keys())


__all__ = [
    "BaseLLMProvider",
    "get_provider",
    "list_providers",
]

# Conditionally export providers if available
if OpenAIProvider is not None:
    __all__.append("OpenAIProvider")
if OpenRouterProvider is not None:
    __all__.append("OpenRouterProvider")
if OllamaProvider is not None:
    __all__.append("OllamaProvider")
if LMStudioProvider is not None:
    __all__.append("LMStudioProvider")
if PerplexityProvider is not None:
    __all__.append("PerplexityProvider")

