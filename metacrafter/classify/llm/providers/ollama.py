# -*- coding: utf-8 -*-
"""OllamaProvider: the `ollama` preset of the OpenAI-compatible client."""
from .openai_compatible import OpenAICompatibleProvider


class OllamaProvider(OpenAICompatibleProvider):
    """OpenAI-compatible client with `ollama` defaults (base URL, API key variable, model)."""

    preset = "ollama"
