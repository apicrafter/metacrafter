# -*- coding: utf-8 -*-
"""OpenRouterProvider: the `openrouter` preset of the OpenAI-compatible client."""
from .openai_compatible import OpenAICompatibleProvider


class OpenRouterProvider(OpenAICompatibleProvider):
    """OpenAI-compatible client with `openrouter` defaults (base URL, API key variable, model)."""

    preset = "openrouter"
